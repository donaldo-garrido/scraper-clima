from sql_exec import create_server_connection, create_db_connection, execute_query

import json


def insert_data(db_name, pw):

    slugs = ['CDMX', 'MTY', 'MER', 'WKD']

    # Iniciar conexión al server
    connection = create_server_connection('localhost', 'donaldo', pw)



    for slug in slugs:

        with open('json_data_{}.json'.format(slug)) as json_file:
            data = json.load(json_file)
            
            # Insertamos los valores para la tabla de las respuestas
            respuesta_id = '"'+data['identifier']+'"'
            respuesta_http = '"'+str(data['status'])+'"'

            respuesta_query = """INSERT INTO Respuesta 
            (respuesta_id, respuesta_HTTP) VALUES ({}, {});""".format(respuesta_id, respuesta_http)

            connection_db = create_db_connection('localhost', 'donaldo', pw, db_name)
            execute_query(connection_db, respuesta_query)
            print(respuesta_query)

            if data['status'] == 200:

                # Insertamos los valores para la tabla de las respuestas
                respuesta_id = '"'+data['identifier']+'"'

                datos = data['data']
                distancia = '"'+datos['Distancia']+'"'
                fecha_hora = '"'+datos['Fecha y Hora']+'"'
                temperatura_actual = '"'+datos['Temperatura actual']+'"'
                humedad_relativa = '"'+datos['Humedad relativa']+'"'
                ciudad_id = respuesta_id.split('-')[0]+'"'
            
                datos_query = """INSERT INTO Datos (respuesta_id, ciudad_id, distancia, fecha_hora, 
                temperatura_actual, humedad_relativa) VALUES ({}, {}, {}, 
                {}, {}, {});""".format(respuesta_id, ciudad_id, distancia, fecha_hora, temperatura_actual, humedad_relativa)

                print(datos_query)

                connection_db = create_db_connection('localhost', 'donaldo', pw, db_name)
                execute_query(connection_db, datos_query)
