import pandas as pd
from lxml import etree
import pymysql
import numpy as np

# --------------------------------------------------------------------
# XML parse

context = etree.iterparse("PNR.xml", tag="{urn:schemas-microsoft-com:office:spreadsheet}Row")

rows = []
for _, row in context:
    values = []
    current_index = 1

    for cell in row.findall("{urn:schemas-microsoft-com:office:spreadsheet}Cell"):
        index_attr = cell.get("{urn:schemas-microsoft-com:office:spreadsheet}Index")
        if index_attr:
            target_index = int(index_attr)
            while current_index < target_index:
                values.append(None)
                current_index += 1

        data = cell.find("{urn:schemas-microsoft-com:office:spreadsheet}Data")
        values.append(data.text if data is not None else None)
        current_index += 1

    rows.append(values)

# --------------------------------------------------------------------
# Data Preparation

# prep for dataframe
header = rows[0]
data = rows[1:]

# drop all 16 value rows, they are not needed
new_data = []
for row in data:
    if len(row) == 10:
        new_data.append(row)

# convert in dataframe
df = pd.DataFrame(new_data, columns=header)

# sort dataframe by earliest travelDate and bookingTime
df_sorted = df.copy()
df_sorted['bookingTime'] = pd.to_datetime(df_sorted['bookingTime'], format='%H:%M:%S', errors='coerce')
df_sorted['bookingTime'] = df_sorted['bookingTime'] - df_sorted['bookingTime'].dt.normalize()
df_sorted['travelDate'] = pd.to_datetime(df['travelDate'])
df_sorted = df_sorted.sort_values(['travelDate','bookingTime'])
df_sorted['npass'] = df_sorted['npass'].astype('int64')
df_sorted = df_sorted[df_sorted['npass'] != 0]  # if npass is 0 then no seat are reserved
df_sorted = df_sorted.dropna() # drop passenger with incomplete name

# create FlightID for reservations based on source, destination, and the travel date
f_keys = (
    df_sorted[['source', 'dest', 'travelDate']].drop_duplicates().reset_index(drop=True)
)

f_keys['Flight_ID'] = ['F' + str(i+1) for i in range(len(f_keys))]

df_sorted = df_sorted.merge(f_keys, on=['source', 'dest', 'travelDate'], how='left')  # assign Flight_ID to each passenger entry

# --------------------------------------------------------------------
# Reserving Seats

# airplane seat number generation
first_lst = [f"F{i}" for i in range(1,51)]
business_lst = [f"B{i}" for i in range(1,101)]
economy_lst = [f"E{i}" for i in range(1,151)]

f_max, b_max, e_max = 50, 100, 150

reservations = []

for f_id, group in df_sorted.groupby('Flight_ID', sort=False):

    pointer_f = pointer_b = pointer_e = 0
    used_f = used_b = used_e = 0

    for idx, vals in group.iterrows():
        remaining = int(vals['npass'])
        travel_date = vals['travelDate']
        requested = vals['class']

        while remaining > 0:

            allocated = False

            # reserve requested seat
            if requested == 'first':
                if used_f < f_max:
                    take = min(remaining, f_max - used_f)
                    seats = first_lst[pointer_f:pointer_f + take]

                    for s in seats:
                        reservations.append((f_id, idx, 'first', travel_date, s))

                    pointer_f += take
                    used_f += take
                    remaining -= take
                    allocated = True

            elif requested == 'business':
                if used_b < b_max:
                    take = min(remaining, b_max - used_b)
                    seats = business_lst[pointer_b:pointer_b + take]

                    for s in seats:
                        reservations.append((f_id, idx, 'business', travel_date, s))

                    pointer_b += take
                    used_b += take
                    remaining -= take
                    allocated = True

            else:  # economy
                if used_e < e_max:
                    take = min(remaining, e_max - used_e)
                    seats = economy_lst[pointer_e:pointer_e + take]

                    for s in seats:
                        reservations.append((f_id, idx, 'economy', travel_date, s))

                    pointer_e += take
                    used_e += take
                    remaining -= take
                    allocated = True

            if allocated:
                continue 

            # check for other avialable seats if specific class is full
            for c in ['first', 'business', 'economy']:
                if remaining == 0:
                    break

                if c == 'first' and used_f < f_max:
                    take = min(remaining, f_max - used_f)
                    seats = first_lst[pointer_f:pointer_f + take]

                    for s in seats:
                        reservations.append((f_id, idx, 'first', travel_date, s))

                    pointer_f += take
                    used_f += take
                    remaining -= take
                    break

                elif c == 'business' and used_b < b_max:
                    take = min(remaining, b_max - used_b)
                    seats = business_lst[pointer_b:pointer_b + take]

                    for s in seats:
                        reservations.append((f_id, idx, 'business', travel_date, s))

                    pointer_b += take
                    used_b += take
                    remaining -= take
                    break

                elif c == 'economy' and used_e < e_max:
                    take = min(remaining, e_max - used_e)
                    seats = economy_lst[pointer_e:pointer_e + take]

                    for s in seats:
                        reservations.append((f_id, idx, 'economy', travel_date, s))

                    pointer_e += take
                    used_e += take
                    remaining -= take
                    break
            else:
                # all classes full
                break

reserve_df = pd.DataFrame(reservations, columns=['Flight_ID', 'PassengerID', 'class', 'travelDate', 'seat_num'])

# --------------------------------------------------------------------
# Preprocessing for MYSQL table population

passenger_df = df_sorted
passenger_df['PassengerID'] = passenger_df.index
passenger_df = passenger_df[['PassengerID', 'firstname', 'lastname', 'address', 'age', 'source', 'dest', 'travelDate', 'class', 'bookingTime', 'npass', 'Flight_ID']]

class_lst = list(df_sorted['class'].unique())
seats = dict(zip(class_lst, [150, 50, 100]))
airports = pd.read_csv('iata.txt', header=None, names=['airport_code'])
flights_df = df_sorted[['Flight_ID', 'source', 'dest', 'travelDate']]
flights_df = flights_df.drop_duplicates(subset=['source', 'dest', 'travelDate']) # drop duplicates

reserve_df['res_ID'] = reserve_df.index
reserve_df = reserve_df[['res_ID', 'Flight_ID', 'PassengerID', 'class', 'travelDate', 'seat_num']]

checkin_df = reserve_df.copy()
times = np.random.randint(0, 24*60*60, size=len(checkin_df))
# create checkInDate within same day
checkin_df['checkInDate'] = checkin_df['travelDate'] - pd.to_timedelta(times, unit='s')
checkin_df['check_in_ID'] = checkin_df.index
checkin_df = checkin_df[['check_in_ID', 'Flight_ID', 'PassengerID', 'travelDate', 'checkInDate', 'class', 'seat_num']]

# --------------------------------------------------------------------
# merge dfs to have one singular csv file for Part B

# merge dfs to have one singular csv file for Part B
air_system = (
    passenger_df.merge(
        reserve_df,
        on=["PassengerID", "Flight_ID", "travelDate"],
        how="left"
    )
    .merge(
        checkin_df,
        on=["PassengerID", "Flight_ID", "travelDate", "seat_num"],
        how="left"
    )
)

air_system = air_system[
    [
        "PassengerID", "firstname", "lastname", "address", "age",
        "source", "dest", "travelDate", "class", "bookingTime", 
        "npass", "Flight_ID", "res_ID", "seat_num", "check_in_ID", "checkInDate"
    ]
]

# save to CSV
air_system.to_csv('air_system.csv', index=False)

# --------------------------------------------------------------------
# Insert Queries

# connect to database
mydbase = pymysql.connect(host="localhost",
                             user="root",
                             passwd="Oxfrds_not_Broks1!",
                             database="airline_system"
)

print("Connected successfully")
mycursor = mydbase.cursor()
mydbase.ping()

# insert passenger
insert_pass = f"""
INSERT INTO passenger
(PassengerID, firstname, lastname, address, age, source, dest, travelDate, class, bookingTime, npass, Flight_ID)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

pass_vals = [tuple(row) for row in passenger_df.to_numpy()]
mycursor.executemany(insert_pass, pass_vals)
mydbase.commit()
print("Populated passenger table.")

# insert class
insert_class = f"""
INSERT INTO class
(Class_type)
VALUES (%s)
"""
mycursor.executemany(insert_class, class_lst)
mydbase.commit()
print("Populated class table.")

# insert seats
insert_seats = f"""
INSERT INTO seats
(Class_type, max_seats)
VALUES (%s, %s)
"""
rows = [(c, s) for c, s in seats.items()]
mycursor.executemany(insert_seats, rows)
mydbase.commit()
print("Populated seats table.")

# insert airports
insert_airport = f"""
INSERT INTO airports
(IATA_code)
VALUES (%s)
"""
airport_vals = [tuple(row) for row in airports.to_numpy()]
mycursor.executemany(insert_airport, airport_vals)
mydbase.commit()
print("Populated airports table.")

# insert flights
insert_flights = f"""
INSERT INTO flights
(Flight_ID, source, destination, travelDate)
VALUES (%s, %s, %s, %s)
"""
flights_vals = [tuple(row) for row in flights_df.to_numpy()]
mycursor.executemany(insert_flights, flights_vals)
mydbase.commit()
print("Populated flights table.")

# insert reservations
insert_res = f"""
INSERT INTO reservations
(res_ID, Flight_ID, PassengerID, class, travelDate, seat_num)
VALUES (%s, %s, %s, %s, %s, %s)
"""

res_vals = [tuple(row) for row in reserve_df.to_numpy()]
mycursor.executemany(insert_res, res_vals)
mydbase.commit()
print("Populated reservations table.")

# insert check-ins
insert_c = f"""
INSERT INTO check_in
(check_in_ID, Flight_ID, PassengerID, travelDate, checkInDate, class, seat_num)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

c_vals = [tuple(row) for row in checkin_df.to_numpy()]
mycursor.executemany(insert_c, c_vals)
mydbase.commit()
print("Populated check_in table.")