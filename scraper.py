# Importación de librerías
import requests
from bs4 import BeautifulSoup
import re

def scraper(url):
    page = requests.get(url)

    return page.text, page.status_code, page.json


def parse(content):
    soup=BeautifulSoup(content, 'html.parser')

    dist = str(soup.find("span", {"id": "dist_cant"}))
    act_date = str(soup.find("span", {"id": "fecha_act_dato"}))
    act_temp = str(soup.find("span", {"id": "ult_dato_temp"}))
    rel_hum = str(soup.find("span", {"id": "ult_dato_hum"}))

    keys = ['Distancia', 'Fecha y Hora', 'Temperatura actual', 'Humedad relativa']
    items = [dist, act_date, act_temp, rel_hum]

    data_cleaned = {}

    for i in range(len(items)):
        data = re.split('w*(?=<)', re.split('(?<=">)w*', items[i])[1])[0]
        data_cleaned[keys[i]] = data

    return data_cleaned