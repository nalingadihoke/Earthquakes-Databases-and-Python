import mysql.connector
from mysql.connector import Error
from Data_ETL.Get_Data import get_2017_data
import sys

DATABASE = ''
HOSTNAME = ''
USERNAME = ''
PASSWORD = ''

def main():
    def create_db_connection(host_name, user_name, user_password, db_name):
        connection = None
        try:
            connection = mysql.connector.connect(
                host=host_name,
                user=user_name,
                passwd=user_password,
                database=db_name
            )
            print("MySQL Database connection successful")
        except Error as err:
            print(f"Error: '{err}'")
        return connection

    connection = create_db_connection(HOSTNAME, USERNAME, PASSWORD, DATABASE)
    mycursor = connection.cursor()

    def commit(sql, val):
        try:
            mycursor.execute(sql, val)
            connection.commit()
        except:
            pass

    def add_magType(quake_magType):
        sql = "INSERT INTO magtype (magtype_id, magtype) VALUES (%s, %s)"
        for index, row in quake_magType.iterrows():
            val = (row['magType_ID'], row['magType'])
            commit(sql, val)

    def add_sources(quake_sources):
        sql = "INSERT INTO sources (sources_id, sources) VALUES (%s, %s)"
        for index, row in quake_sources.iterrows():
            val = (row['sources_ID'], row['sources'])
            commit(sql, val)


    def add_title(quake_title):
        sql = "INSERT INTO title (title_ID, title) VALUES (%s, %s)"
        for index, row in quake_title.iterrows():
            val = (row['title_ID'], row['title'])
            commit(sql, val)

    def add_location(quake_location):
        sql = "INSERT INTO location (id, title, lon, lat, depth, sources, title_ID, sources_ID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        for index, row in quake_location.iterrows():
            val = (row['id'], row['title'], row['lon'], row['lat'], row['depth'], row['sources'], row['title_ID'], row['sources_ID'])
            commit(sql, val)

    def add_time(quake_time):
        sql = "INSERT INTO time (id, datetime, updated_datetime) VALUES (%s, %s, %s)"
        for index, row in quake_time.iterrows():
            val = (row['id'], row['date_time'], row['updated_datetime'])
            commit(sql, val)

    def add_magnitude(quake_magnitude):
        sql = "INSERT INTO magnitude (id, mag, magType, tsunami, magType_ID) VALUES (%s, %s, %s, %s, %s)"
        for index, row in quake_magnitude.iterrows():
            val = (row['id'], row['mag'], row['magType'], row['tsunami'], row['magType_ID'])
            commit(sql, val)

    data = get_2017_data()
    quake_location = data[0]
    quake_title = data[1]
    quake_sources = data[2]
    quake_magnitude = data[3]
    quake_magType = data[4]
    quake_time = data[5]

    add_magType(quake_magType)
    print("magType done")
    add_title(quake_title)
    print("title done")
    add_sources(quake_sources)
    print("sources done")
    add_location(quake_location)
    print("location done")
    add_time(quake_time)
    print("time done")
    add_magnitude(quake_magnitude)
    print("magnitude done")

if __name__ == '__main__':
    #if len(sys.argv) < 4:
        #print("Usage: python Main_ETL.py <DB name> <hostname> <username> <password>")
        #exit(1)
    # See expected input file format in README
    DATABASE = sys.argv[1]
    HOSTNAME = sys.argv[2]
    USERNAME = sys.argv[3]
    PASSWORD = sys.argv[4]
    main()

