import pandas as pd
import numpy as np
import pymysql

air_sys_df = pd.read_csv('air_system.csv')

print("Key Info:")
print("-----")

print("Rows and Columns: ", air_sys_df.shape)
print("-----")
print(air_sys_df.head(5))
print("-----")
print(air_sys_df.dtypes)
print("-----")

# --------------------------------------------------------------------

def get_user_input():
    '''Input relation, its attributes, functional dependecies, and primary keys.'''
    relation = input("Enter relation name: ")
    input_attrib = input("Enter attributes (comma-separated): ")
    attributes = [a.strip() for a in input_attrib.split(",")]

    fds_input = input("Enter functional dependencies (e.g. A->B | C->D): ")

    fds = []
    for f in fds_input.split("|"):
        left, right = f.split("->")

        lhs = left.strip()
        rhs = [a.strip() for a in right.split(",")]

        fds.append((lhs, rhs))

    keys_input = input("Enter primary key(s): ")
    primary_keys = [[pk.strip() for pk in keys_input.split(",")]]

    return relation, attributes, fds, primary_keys

relation, attributes, fds, primary_keys = get_user_input()
# USER INPUT
# airline_system
# PassengerID, firstname, lastname, address, age, source, dest, travelDate, class, bookingTime, npass, Flight_ID, res_ID, seat_num, check_in_ID, checkInDate
# PassengerID->firstname,lastname,address,age,class,bookingTime,npass | Flight_ID->source,dest,travelDate | res_ID->seat_num | check_in_ID->checkInDate
# PassengerID,Flight_ID,res_ID,check_in_ID

# --------------------------------------------------------------------

def attrib_closure(attributes, fds):
    closure = set(attributes)

    change_attrib = True
    while change_attrib:
        change_attrib = False

        for left, right in fds:
            left = set(left)
            right = set(right)
            if left.issubset(closure) and not right.issubset(closure):
                closure |= right # union
                change_attrib = True
    return closure

def get_prime_attributes(primary_keys):
    primes = set()
    for pk in primary_keys:
        primes.update(pk)
    return primes

def is_superkey(X, primary_keys):
    X = {X} if type(X) == type("") else set(X)
    return any(set(pk).issubset(X) for pk in primary_keys)

def find_part_deps(fds, primary_keys):
    primes = get_prime_attributes(primary_keys)
    partial_deps = []

    for lhs, rhs in fds:
        lhs_set = {lhs}

        for pk in primary_keys:
            pk_set = set(pk)

            # subset of primary key
            if lhs_set < pk_set:
                for attr in rhs:
                    if attr not in primes:
                        partial_deps.append(f"{lhs} -> {attr}")
    result = list(set(partial_deps))
    return "No Partial Dependencies" if not result else result

def find_trans_deps(fds, primary_keys):
    primes = get_prime_attributes(primary_keys)
    transitive_deps = []

    for lhs, rhs in fds:
        for attr in rhs:
            if attr not in primes:  # non-prime only
                transitive_deps.append(f"{lhs} -> {attr}")
    result = list(set(transitive_deps))
    return "No Transitive Dependecies" if not result else result

closure = attrib_closure(attributes, fds)
print("Attribute Closure:")
print(closure)
print("-----")

part_dep = find_part_deps(fds, primary_keys)
print("Partial Dependencies:")
print(part_dep)
print("-----")

print("Transitive Dependencies:")
trans_dep = find_trans_deps(fds, primary_keys)
print(trans_dep)
print("-----")

# --------------------------------------------------------------------

def check_1nf(data, primary_keys):
    '''Returns 'True' if relation satisfys first normal
       form.'''
    flat_keys = [pk for sublist in primary_keys for pk in sublist]  # flatten list of lists

    # nulls
    if data[flat_keys].isnull().any().any():
        return False, "Primary key has null values."
    
    # duplicates
    if data.duplicated(subset=flat_keys).any():
        return False, "Found duplicate values in keys."
    
    # check if values are singular
    for col in data.columns:
        if data[col].apply(lambda x: isinstance(x, (list, set, dict))).any():
            return False, "Non-regular values found."
    
    return True

def check_2nf(primary_keys, fds):
    primes = get_prime_attributes(primary_keys)
    violations = []

    for lhs, rhs in fds:
        lhs_set = {lhs}

        for pk in primary_keys:
            pk_set = set(pk)

            if lhs_set < pk_set:
                for attr in rhs:
                    if attr not in primes:
                        violations.append((lhs, attr))

    violations = list(set(violations))
    
    if violations:
        return False, violations
    else:
        return True
    
def check_3nf(primary_keys, fds):
    primes = get_prime_attributes(primary_keys)
    violations = []

    for lhs, rhs in fds:
        for attr in rhs:
            if not is_superkey(lhs, primary_keys) and attr not in primes:
                violations.append(f"{lhs} -> {attr}")

    violations = list(set(violations))

    if violations:
        return False, violations
    else:
        return True

print("Normalization Status:")
print("1NF:")
print(check_1nf(air_sys_df, primary_keys))
print("-----")
print("2NF:")
check_2nf(primary_keys, fds)
print("-----")
print("3NF:")
check_3nf(primary_keys, fds)
print("-----")

# BCNF Decomposition
def decompose_to_tables(primary_keys, fds, relation):
    '''Decompose the relation into normalized tables following BCNF.'''
    _3nf = check_3nf(primary_keys, fds)
    
    if _3nf == True:
        print("Relation in 3nf, no decomposition needed.")
        return None
    
    tables = {}

    for lhs, rhs in fds:
        if "_ID" in lhs:
            table_name = lhs.replace("_ID", "s")
        elif "ID" in lhs:
            table_name = lhs.replace("ID", "s")
        else:
            table_name = lhs

        table_name = table_name.replace("res", "Reservation")

        tables[table_name] = [lhs] + rhs

    print(f"BCNF decomposition of {relation}:")
    print("----------")
    for table_name, attr in tables.items():
        print(f"{table_name} ({','.join(attr)})")

    return tables

# get tables
tables = decompose_to_tables(primary_keys, fds, relation)

def create_sql_tables(tables, df):
    '''Create SQL queries to drop existing tables, create tables, 
    and insert statements.'''
    drop_q = []
    create_q = []
    insert_q = []
    insert_vals = []

    for table_name, attrs in tables.items():
        lines = []
        p_key = attrs[0]

        for col in attrs:
            if col == p_key:
                lines.append(f"\t{col} VARCHAR(50) PRIMARY KEY")
            else:
                lines.append(f"\t{col} VARCHAR(50)")
        drop_q.append(f"DROP TABLE IF EXISTS {table_name};")
        create_q.append(f"CREATE TABLE {table_name} (\n" + ",\n".join(lines) + "\n);")

        placeholders = ", ".join(["%s"] * len(attrs))
        insert_q.append(f"INSERT INTO {table_name}\n({', '.join(attrs)})\nVALUES ({placeholders})")

        table_df = df[attrs].drop_duplicates(subset=[p_key])
        vals = [tuple(row) for row in table_df.to_numpy()]
        insert_vals.append(vals)

    return drop_q, create_q, insert_q, insert_vals

print("SQL Queries Created:")
print("----------")
drop_q, create_q, insert_q, insert_vals = create_sql_tables(tables, air_sys_df)

for d in drop_q:
    print(d)

print("----------")

for c in create_q:
    print(c)
    print("----------")

for i in insert_q:
    print(i)
    print("----------")

#-------------------------------------------------------
# Populate Tables
# connect to database
mydbase = pymysql.connect(host="localhost", 
                          user="root",
                          passwd="Oxfrds_not_Broks1!",
                          database="airline_system"
)

print("Connected successfully")
mycursor = mydbase.cursor()

for query in drop_q:
    mycursor.execute(query)

for query in create_q:
    mycursor.execute(query)
    print("Table created.")

print("----------")

for query, vals in zip(insert_q, insert_vals):
    mycursor.executemany(query, vals)
    print("Populated table.")

mydbase.commit()

def query_driver(mycursor):
    '''Interactive query interface for the create tables above.'''
    print("\nQuery Interface")
    print("=" * 20)
    print("Press 'q' to quit interface.")
    print("Database: airline_system")
    print("Available tables: Passengers, Flights, Reservations, check_ins")
    print("=" * 20)

    while True:
        query = input("\nEnter query: ").strip()

        if query.lower() == "q":
            print("Exiting query interface.")
            break

        if not query:
            continue

        try:
            mycursor.execute(query)
            results = mycursor.fetchall()

            if results:
                col_head = [desc[0] for desc in mycursor.description]
                print("\n" + " | ".join(col_head))
                print("-" * (len(" | ".join(col_head))))

                for row in results:
                    print(" | ".join([str(val) for val in row]))

                print(f"\n{len(results)} row(s) returned.")
            else:
                print("Query executed successfully. No results returned.")

        except Exception as e:
            print(f"Error: {e}")

# run interface
query_driver(mycursor)

# TEST QUERIES
# SELECT * FROM Reservations ORDER BY res_ID DESC LIMIT 5;
# INSERT INTO Reservations (res_ID, seat_num) VALUES ("E1", "EX");
# DELETE FROM Reservations WHERE res_ID = "E1";