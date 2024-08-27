import mysql.connector
from mysql.connector import Error

def create_db_connection(host_name='localhost', user_name='root', user_password='gnrforever', db_name='earthquakes'):
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

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

create_title_table = """
CREATE TABLE title (
  title_id INT,
  title VARCHAR(255),
  PRIMARY KEY (title_id)
  );
 """

create_sources_table = """
CREATE TABLE sources (
  sources_id INT,
  sources VARCHAR(255),
  PRIMARY KEY (sources_id)
  );
 """

create_magtype_table = """
CREATE TABLE magtype (
  magtype_id INT,
  magtype VARCHAR(255),
  PRIMARY KEY (magtype_id)
  );
 """

create_location_table = """
CREATE TABLE location (
  id VARCHAR(255) ,
  title VARCHAR(255),
  lon DECIMAL(6,2),
  lat DECIMAL(6,2),
  depth DECIMAL(6,2),
  sources VARCHAR(255),
  title_ID INT,
  sources_ID INT,
  PRIMARY KEY (id),
  FOREIGN KEY (sources_ID) REFERENCES sources(sources_id),
  FOREIGN KEY (title_ID) REFERENCES title(title_id)
  );
 """

create_magnitude_table = """
CREATE TABLE magnitude (
  id VARCHAR(255) ,
  mag DECIMAL(6,2),
  magType VARCHAR(255),
  tsunami INT,
  magType_ID INT,
  PRIMARY KEY (id),
  FOREIGN KEY (magType_ID) REFERENCES magtype(magtype_id)
  );
 """

create_time_table = """
CREATE TABLE time (
  id VARCHAR(255) ,
  datetime DATETIME,
  updated_datetime DATETIME,
  PRIMARY KEY (id)
  );
 """

def create_tables(HOSTNAME, USERNAME, PASSWORD, DATABASE):
    connection = create_db_connection(HOSTNAME, USERNAME, PASSWORD, DATABASE)
    execute_query(connection, create_title_table)
    execute_query(connection, create_sources_table)
    execute_query(connection, create_magtype_table)
    execute_query(connection, create_location_table)
    execute_query(connection, create_magnitude_table)
    execute_query(connection, create_time_table)