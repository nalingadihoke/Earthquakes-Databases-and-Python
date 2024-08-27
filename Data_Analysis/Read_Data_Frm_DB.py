import mysql.connector as connection
import pandas as pd

pd.set_option('display.max_columns', 500)

def Get_Tables(HOSTNAME, USERNAME, PASSWORD, DATABASE):
    mydb = connection.connect(host=HOSTNAME,user=USERNAME, passwd=PASSWORD, database = DATABASE)
    tables = ['location', 'magnitude', 'time']
    dfs = []
    for i in tables:
        query = "Select * from {}".format(i)
        result_dataFrame = pd.read_sql(query,mydb)
        dfs.append(result_dataFrame)
    mydb.close() #close the connection
    return dfs