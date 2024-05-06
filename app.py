#!/usr/bin/env python3
import logging
import os
from cassandra.cluster import Cluster
import model

# Set logger
log = logging.getLogger()
log.setLevel(logging.INFO)
handler = logging.FileHandler('airport_ads.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'airport_ads')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        1: "Ver pasajeros por aeropuerto y mes",
        2: "Ver publicidad por aeropuerto y mes",
        3: "Salir",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    while True:
        print_menu()
        option = int(input('Ingrese su opción: '))

        if option == 1:
            airport_id = input('Ingrese el ID del aeropuerto: ')
            month = int(input('Ingrese el mes (1-12): '))
            model.get_passengers_by_airport_month(session, airport_id, month)
        elif option == 2:
            airport_id = input('Ingrese el ID del aeropuerto: ')
            month = int(input('Ingrese el mes (1-12): '))
            model.get_advertisements_by_airport_month(session, airport_id, month)
        elif option == 3:
            exit(0)
        else:
            print("Opción no válida. Por favor, ingrese un número válido.")


if __name__ == '__main__':
    main()
