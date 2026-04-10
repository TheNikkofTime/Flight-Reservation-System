DROP DATABASE IF EXISTS airline_system; 
CREATE DATABASE airline_system; 
USE airline_system;

-- Passenger table
CREATE TABLE passenger(
	PassengerID INT PRIMARY KEY,
    firstname VARCHAR(50),
    lastname VARCHAR(50),
    address VARCHAR(50),
    age INT,
    source VARCHAR(20),
    dest VARCHAR(20),
    travelDate VARCHAR(20),
    class VARCHAR(20),
    bookingTime VARCHAR(20),
    npass INT,
    Flight_ID VARCHAR(20)
);

-- Class (shows reference for type of class)
CREATE TABLE class(
	Class_type VARCHAR(50) PRIMARY KEY
);

-- Seats (shows maximum capacity for class seats)
CREATE TABLE seats(
	Class_type VARCHAR(50) PRIMARY KEY,
    max_seats INT
);

-- Airports
CREATE TABLE airports(
	IATA_code VARCHAR(10)
);

DROP TABLE IF EXISTS flights;
-- Flights
CREATE TABLE flights(
	Flight_ID VARCHAR(20) PRIMARY KEY,
    source VARCHAR(20),
    destination VARCHAR(20),
    travelDate VARCHAR(20)
);

-- Reservations
CREATE TABLE reservations(
	res_ID INT PRIMARY KEY,
	Flight_ID VARCHAR(20),
    PassengerID VARCHAR(20),
    class VARCHAR(20),
	travelDate VARCHAR(20),
    seat_num VARCHAR(20)
);

-- Checkin
CREATE TABLE check_in(
	check_in_ID INT PRIMARY KEY,
	Flight_ID VARCHAR(20),
    PassengerID VARCHAR(20),
    travelDate VARCHAR(20),
    checkInDate VARCHAR(20),
    class VARCHAR(20),
    seat_num VARCHAR(20)
);

SELECT * FROM passenger LIMIT 5;

-- 4 queries for Project 1 Part A
-- 1. Show flight schedules between 2 airports between two dates
SELECT DISTINCT Flight_ID, source, dest, travelDate FROM passenger 
WHERE source = 'EWR' 
AND dest = 'LAX'
AND travelDate BETWEEN '2100-01-01 00:00:00' AND '2100-01-07 00:00:00';

-- 2. Rank top 3 (source, dest) airports based on booking requests for a week
SELECT source, dest, COUNT(*) AS total_requests FROM passenger
WHERE travelDate BETWEEN '2100-01-01' AND '2100-01-07'
GROUP BY source, dest
ORDER BY total_requests DESC
LIMIT 3;

-- 3. Next available (has seats) flight between given airports 
SELECT 
    f.Flight_ID,
    (300 - COUNT(r.seat_num)) AS seats_available
FROM flights f
LEFT JOIN reservations r 
    ON f.Flight_ID = r.Flight_ID
WHERE 
    f.source = 'EWR'
    AND f.destination = 'LAX'
GROUP BY f.Flight_ID
ORDER BY f.travelDate
LIMIT 100;

-- 4. Average occupancy rate (%full) for all flights between 2 cities
SELECT Flight_ID, (COUNT(seat_num) / 300) AS percent_full FROM reservations GROUP BY Flight_ID;