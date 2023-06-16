from sql_exec import create_server_connection, create_db_connection, execute_query

def create(db_name, pw):

    # Iniciar conexión al server
    connection = create_server_connection('localhost', 'donaldo', pw)


    # Query para crear la base de datos
    create_database_query = "CREATE DATABASE {}".format(db_name)
    execute_query(connection, create_database_query)



    # Querys para creación de tablas
    create_tables_query = ["""
    CREATE TABLE Ciudad (
        ciudad_id     VARCHAR(128) UNIQUE,
        nombre       VARCHAR(128),
        PRIMARY KEY(ciudad_id)
    ) ENGINE=InnoDB CHARACTER SET=utf8;
    """, 
    """
    CREATE TABLE Respuesta (
        respuesta_id     VARCHAR(128) UNIQUE,
        respuesta_HTTP       INTEGER,
        PRIMARY KEY(respuesta_id)
    ) ENGINE=InnoDB CHARACTER SET=utf8;
    """,
    """
    CREATE TABLE Datos (
        ciudad_id       VARCHAR(128),
        respuesta_id    VARCHAR(128),
        distancia		VARCHAR(128),
        fecha_hora		VARCHAR(128),
        temperatura_actual	FLOAT,
        humedad_relativa    FLOAT,

        CONSTRAINT FOREIGN KEY (ciudad_id) REFERENCES Ciudad (ciudad_id),
        CONSTRAINT FOREIGN KEY (respuesta_id) REFERENCES Respuesta (respuesta_id),

        PRIMARY KEY (ciudad_id, respuesta_id)
    ) ENGINE=InnoDB CHARACTER SET=utf8;
    """]

    for query in create_tables_query:
        connection_db = create_db_connection('localhost', 'donaldo', pw, db_name)
        execute_query(connection_db, query)


    # Query para insertar ciudades
    insert_cities_query = [
    "INSERT INTO Ciudad (ciudad_id, nombre) VALUES ('CDMX', 'ciudad-de-mexico');",
    "INSERT INTO Ciudad (ciudad_id, nombre) VALUES ('MTY', 'monterrey');",
    "INSERT INTO Ciudad (ciudad_id, nombre) VALUES ('MER', 'merida');",
    "INSERT INTO Ciudad (ciudad_id, nombre) VALUES ('WKD', 'wakanda');"
    ]

    for query in insert_cities_query:
        connection_db = create_db_connection('localhost', 'donaldo', pw, db_name)
        execute_query(connection_db, query)