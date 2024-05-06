import logging

# model.py

#!/usr/bin/env python3

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
    CREATE KEYSPACE IF NOT EXISTS {}
    WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_AIRPORTS_TABLE = """
    CREATE TABLE IF NOT EXISTS airports (
        airport_id TEXT PRIMARY KEY,
        name TEXT,
        city TEXT,
        country TEXT,
        iata_code TEXT
    )
"""

CREATE_PASSENGERS_TABLE = """
    CREATE TABLE IF NOT EXISTS passengers (
        passenger_id TEXT PRIMARY KEY,
        name TEXT,
        age INT,
        gender TEXT,
        country_of_origin TEXT
    )
"""

CREATE_FLIGHTS_TABLE = """
    CREATE TABLE IF NOT EXISTS flights (
        flight_id TEXT PRIMARY KEY,
        departure_airport_id TEXT,
        arrival_airport_id TEXT,
        departure_time TIMESTAMP,
        arrival_time TIMESTAMP,
        airline TEXT,
        flight_number TEXT,
        FOREIGN KEY (departure_airport_id) REFERENCES airports(airport_id),
        FOREIGN KEY (arrival_airport_id) REFERENCES airports(airport_id)
    )
"""

CREATE_PASSENGER_FLIGHTS_TABLE = """
    CREATE TABLE IF NOT EXISTS passenger_flights (
        passenger_id TEXT,
        flight_id TEXT,
        transit_mode TEXT,
        PRIMARY KEY (passenger_id, flight_id),
        FOREIGN KEY (passenger_id) REFERENCES passengers(passenger_id),
        FOREIGN KEY (flight_id) REFERENCES flights(flight_id)
    )
"""

CREATE_ADVERTISEMENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS advertisements (
        advertisement_id TEXT PRIMARY KEY,
        airport_id TEXT,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        budget DECIMAL,
        FOREIGN KEY (airport_id) REFERENCES airports(airport_id)
    )
"""

SELECT_PASSENGERS_BY_AIRPORT_MONTH = """
    SELECT passenger_id, name, age, gender, country_of_origin, flight_id
    FROM passenger_flights
    JOIN passengers ON passenger_flights.passenger_id = passengers.passenger_id
    JOIN flights ON passenger_flights.flight_id = flights.flight_id
    WHERE departure_airport_id = ?
    AND EXTRACT(month FROM departure_time) = ?
"""

SELECT_ADVERTISEMENTS_BY_AIRPORT_MONTH = """
    SELECT advertisement_id, start_date, end_date, budget
    FROM advertisements
    WHERE airport_id = ?
    AND start_date <= ?
    AND end_date >= ?
"""


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_AIRPORTS_TABLE)
    session.execute(CREATE_PASSENGERS_TABLE)
    session.execute(CREATE_FLIGHTS_TABLE)
    session.execute(CREATE_PASSENGER_FLIGHTS_TABLE)
    session.execute(CREATE_ADVERTISEMENTS_TABLE)

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))
    create_schema(session) 

def get_passengers_by_airport_month(session, airport_id, month):
    log.info(f"Retrieving passengers for airport {airport_id} in month {month}")
    rows = session.execute(SELECT_PASSENGERS_BY_AIRPORT_MONTH, (airport_id, month))
    for row in rows:
        print(f"=== Passenger: {row.passenger_id} ===")
        print(f"- Name: {row.name}")
        print(f"- Age: {row.age}")
        print(f"- Gender: {row.gender}")
        print(f"- Country of Origin: {row.country_of_origin}")
        print(f"- Flight ID: {row.flight_id}")

def get_advertisements_by_airport_month(session, airport_id, month):
    log.info(f"Retrieving advertisements for airport {airport_id} in month {month}")
    rows = session.execute(SELECT_ADVERTISEMENTS_BY_AIRPORT_MONTH, (airport_id, month, month))
    for row in rows:
        print(f"=== Advertisement: {row.advertisement_id} ===")
        print(f"- Start Date: {row.start_date}")
        print(f"- End Date: {row.end_date}")
        print(f"- Budget: {row.budget}")

