import mysql.connector as connection
import pandas as pd
from mysql.connector import Error
import pyspark
from pyspark.sql import SparkSession
import os

def parquet_update_partition(db_name, pw):
    appName = "PySpark MySQL - parquet"
    master = "local"

    spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

    ciudades = ['CDMX', 'MTY', 'MER']

    data = []

    for ciudad in ciudades:
        try:
            mydb = connection.connect(host="localhost", database = db_name ,user="donaldo", passwd=pw ,use_pure=True)
            query = """SELECT Datos.ciudad_id, Datos.respuesta_id, Datos.temperatura_actual, Datos.humedad_relativa
                    FROM Datos
                    WHERE Datos.ciudad_id = {}
                    ORDER BY Datos.respuesta_id;""".format('"'+ciudad+'"')
            pandas_df = pd.read_sql(query, mydb)
            mydb.close() #close the connection

        except Error as err:
            print(f"Error: '{err}' ")


        # Convert Pandas dataframe to spark DataFrame
        df = spark.createDataFrame(pandas_df)
        variables = ["temperatura_actual", "humedad_relativa"]

        aux = 2

        data_ciudad = [[df.collect()[-1][0]]]

        for variable in variables:

            max_var = df.agg({variable: "max"}).collect()[0]
            var_max = max_var["max({})".format(variable)]

            min_var = df.agg({variable: "min"}).collect()[0]
            var_min = min_var["min({})".format(variable)]

            avg_var = df.agg({variable: "avg"}).collect()[0]
            var_avg = avg_var["avg({})".format(variable)]

            var_current = df.collect()[-1][aux]
            aux+=1

            data_ciudad.append([var_max, var_min, var_avg, var_current])

        run = df.collect()[-1][1].split('-')[1]
        data_ciudad.append([run])
        data_ciudad = tuple([item for sublist in data_ciudad for item in sublist])

        print(data_ciudad)

        data.append(data_ciudad)


    print(data)


    columns=["ciudad","temperatura_max","temperatura_min","temperatura_prom","temperatura_actual",
            "humedad_max","humedad_min","humedad_prom","humedad_actual", "corrida"]
    df_parquet=spark.createDataFrame(data, columns)

    if not os.listdir('./parquet'):
        df_parquet.write.mode("overwrite").parquet('./parquet/clima-{}.parquet'.format(db_name))


    df_parquet.write.mode('append').parquet("./parquet/clima-{}.parquet".format(db_name))


    #parDF=spark.read.parquet("./parquet/clima.parquet/corrida=140620232058")

    df_parquet.write.partitionBy("corrida").mode("append").parquet("./parquet/clima-{}.parquet".format(db_name))


    # Para ver alguna partición:
    # Colocar el identificador de la partición
    # o quitar el dir de corrida=... para ver todo el parquet

    parDF=spark.read.parquet("./parquet/clima-{}.parquet/corrida={}".format(db_name,run))

    parDF.show()