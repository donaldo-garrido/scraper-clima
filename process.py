from create_db_tables import create
from scraper import scraper, parse
from insert_data_json_sql import insert_data
from parquet_elaboration import parquet_update_partition

from datetime import datetime
import json

# Base de datos
db_name = 'clima_db'

# Ingresar contraseña
pw = '360Donaldo'


create(db_name, pw)

slugs = [('ciudad-de-mexico','CDMX'), ('monterrey','MTY'), ('merida','MER'), ('wakanda', 'WKD')]

to_JSON = {}

for slug in slugs:

    data_cleaned = ''
    url='https://www.meteored.mx/'+slug[0]+'/historico'

    content, status, petition = scraper(url)

    now = datetime.now()

    identifier  = slug[1]+'-'+now.strftime("%d%m%Y%H%M")


    #print(status)
    #print(petition)
    #print(identifier)

    if status == 200:
        data_cleaned = parse(content)
        #print(data_cleaned)

    to_JSON['identifier'] = identifier
    to_JSON['request'] = str(petition)
    to_JSON['status'] = status
    to_JSON['data'] = data_cleaned
    print(to_JSON)

    with open('json_data_{}.json'.format(slug[1]), 'w') as outfile:
        json.dump(to_JSON, outfile)

    #json_object = json.dumps(str(to_JSON)) 
    #print(json_object)


insert_data(db_name, pw)

parquet_update_partition(db_name, pw)