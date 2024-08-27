import mysql.connector
from mysql.connector import Error
import sys
from Create_DB.Create_DB import create_server_connection, create_database
from Create_DB.Create_tables import create_tables

DATABASE = ''
HOSTNAME = ''
USERNAME = ''
PASSWORD = ''

def main():
    connection = create_server_connection(HOSTNAME, USERNAME, PASSWORD)
    create_database_query = "CREATE DATABASE {}".format(DATABASE)
    create_database(connection, create_database_query)

    create_tables(HOSTNAME, USERNAME, PASSWORD, DATABASE)

if __name__ == '__main__':
    #if len(sys.argv) < 4:
        #print("Usage: python Main_DB_Creation.py <DB name> <hostname> <username> <password>")
        #exit(1)
    # See expected input file format in README
    DATABASE = sys.argv[1]
    HOSTNAME = sys.argv[2]
    USERNAME = sys.argv[3]
    PASSWORD = sys.argv[4]
    main()