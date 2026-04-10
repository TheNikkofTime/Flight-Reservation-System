# Flight Reservation & Database Normalization System

## Overview

This project consists of two main components:

- **Part A:** A flight reservation system that processes passenger data, generates flights, and assigns seats.
- **Part B:** A database normalization system that analyzes the generated data and decomposes it into structured relational tables.

The project is an extensive example of big data programming: from raw passenger data → reservation system → normalized database → query interface.

---

## Project Structure
Part_A.py 
- Reservation system & data processing
  
Part_B.py
- Normalization & query interface

---

## Required Python Packages
Part_A.py
```python
import pandas as pd
from lxml import etree
import pymysql
import numpy as np
```

Part_B.py
```python
import pandas as pd
import numpy as np
import pymysql
```

## Part A: Reservation System

### Description

Part A parses passenger data from an XML file and builds a complete airline reservation system.

### Features

- **XML → DataFrame**
  - Parses passenger data using `lxml`
  - Converts it into a structured Pandas DataFrame

- **Data Cleaning**
  - Removes invalid or incomplete records
  - Sorts by travel date and booking time
  - Filters out zero-passenger entries

- **Flight ID Generation**
  - Unique `Flight_ID` created from:
    - Source airport
    - Destination airport
    - Travel date

- **Seat Allocation**
  - First Class: 50 seats
  - Business Class: 100 seats
  - Economy Class: 150 seats
  - Automatically assigns seats
  - Handles overflow across classes
    - If the requested class type is full, find the next best class and reserve a open seat. 

- **Reservation & Check-In Simulation**
  - Generates:
    - Reservations
    - Seat assignments
    - Random check-in times

- **Database Population**
  - Inserts data into MySQL tables:
    - `passenger`
    - `class`
    - `seats`
    - `airports`
    - `flights`
    - `reservations`
    - `check_in`

- **Output**
  - Exports combined dataset:
    ```
    air_system.csv
    ```

---

## Part B: Normalization & Database Design

### Description

Part B takes user input descriptions of the relation name, attributes, functional dependencies, primary keys, and the dataset from Part A (air_system.csv) and performs normalization analysis.

### Features

- **Dataset Inspection**
  - Displays schema, data types, and sample rows

- **User Input**
  - Relation name
  - Attributes
  - Functional dependencies
  - Primary keys

### Example User Input

```python
# 'airline_system'

# 'PassengerID, firstname, lastname, address, age, source, dest, travelDate, class, bookingTime, npass, Flight_ID, res_ID, seat_num, check_in_ID, checkInDate'

# 'PassengerID->firstname,lastname,address,age,class,bookingTime,npass | Flight_ID->source,dest,travelDate | res_ID->seat_num | check_in_ID->checkInDate'

# 'PassengerID,Flight_ID,res_ID,check_in_ID'
```

- **Attribute Closure**
  - Computes closures for dependency validation

- **Normalization Checks**
  - **1NF:** No nulls, no duplicates, atomic values
  - **2NF:** Detects partial dependencies
  - **3NF:** Detects transitive dependencies

- **BCNF Decomposition**
  - Splits large table into normalized tables:
    - Passengers (PassengerID,firstname,lastname,address,age,class,bookingTime,npass)
    - Flights (Flight_ID,source,dest,travelDate)
    - Reservations (res_ID,seat_num)
    - check_ins (check_in_ID,checkInDate)

- **SQL Generation**
  - Automatically creates:
    - `DROP TABLE` queries
    - `CREATE TABLE` queries
    - `INSERT` queries

- **Database Population**
  - Executes generated SQL queries in MySQL

---

## Interactive Query Interface

Part B includes a command-line SQL interface.

### Capabilities

- Run SQL queries directly
- Supports:
  - `SELECT`
  - `INSERT`
  - `DELETE`
- Displays results in Python terminal
- Displays errors if necessary.

### Example Queries

~~~sql
SELECT * FROM Reservations ORDER BY res_ID DESC LIMIT 5;

INSERT INTO Reservations (res_ID, seat_num) VALUES ("E1", "EX");

DELETE FROM Reservations WHERE res_ID = "E1";
~~~
