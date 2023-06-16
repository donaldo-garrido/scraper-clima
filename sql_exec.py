import mysql.connector
from mysql.connector import Error
import pandas as pd


# Función para conectar al servidor
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
        host = host_name,
        user = user_name,
        passwd = user_password
    )
        print('MySQL Server connection successful')

    except Error as err:
        print(f"Error: '{err}' ")

    return connection



#Función para crear la base de datos
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print('Database create succesfully')
    except Error as err:
        print(f"Error: '{err}'")




# Conectarse a la base de datos
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
        host = host_name,
        user = user_name,
        passwd = user_password,
        database = db_name
    )
        print('MySQL DB connection successful')

    except Error as err:
        print(f"Error: '{err}' ")

    return connection




# Ejecutar querys de SQL

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        cursor.close()
        print('--------------------\nQuery was succesful')
    except Error as err:
        print(f"Error: '{err}' ")