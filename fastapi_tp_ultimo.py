from datetime import datetime, timedelta
import sqlite3
import numpy as np
import pandas as pd
import os
from fastapi import FastAPI, File, UploadFile
#from pathlib import Path
#import matplotlib.pyplot as plt
#from tabulate import tabulate
#import psycopg2
#import random
import string


#/recommendations/<ADV>/<Modelo>
#Esta entrada devuelve un JSON en dónde se indican las recomendaciones del
#día para el adv y el modelo en cuestión.

resultados_consolidados=pd.read_csv(r"/Users/ychaher/Desktop/Maestria/airflow/airflow/resultados_consolidados.csv")
app = FastAPI()
@app.get("/recomendacion/")
async def recomendacion(advertiser_id: str, date: str, modelo: str):
    conn = sqlite3.connect(':memory:')
    resultados_consolidados.to_sql('tabla_products1', conn, index=False)
    cur1= conn.cursor()
    cur1.execute(f"SELECT GROUP_CONCAT(product_id) as products FROM tabla_products1 WHERE advertiser_id = '{advertiser_id}' AND date = '{date}' AND  modelo = '{modelo}'")
    datos1 = cur1.fetchone()
    return {"advertiser_id": advertiser_id,  "date": date, "products": datos1[0]}



#Cantidad de advertisers
#/stats/
#Esta entrada devuelve un JSON con un resumen de estadísticas sobre las
#recomendaciones a determinar por ustedes. Algunas opciones posibles:
   #● Cantidad de advertisers
@app.get("/stats/")
async def get_stats():
    conn = sqlite3.connect(':memory:')#crea una conexión a la base
    resultados_consolidados.to_sql('tabla_products2', conn, index=False) #aca "tabla_product2" es el nombre de la nueva tabla
    cur2 = conn.cursor()
    cur2.execute('SELECT DISTINCT advertiser_id FROM tabla_products2')
    resultados = cur2.fetchall()
    return {"advertiser_ids": [resultado[0] for resultado in resultados]}


#Coincidencias 
#Esta entrada devuelve un JSON con un resumen de estadísticas sobre las
#recomendaciones a determinar por ustedes. Algunas opciones posibles:
   # Productos que coinciden entre ambos modelos por advertiser por dia
resultados_consolidados_coincidencias=pd.read_csv(r"/Users/ychaher/Desktop/Maestria/airflow/airflow/resultados_consolidados_coincidencias.csv")

@app.get("/coincidencias/")
async def coincidencias(advertiser_id: str, date: str):
    conn = sqlite3.connect(':memory:')
    resultados_consolidados_coincidencias.to_sql('tabla_products3', conn, index=False) #aca "product" es el nombre de la nueva tabla
    cur3 = conn.cursor()
    cur3.execute(f"SELECT date, advertiser_id, advertiser_product, GROUP_CONCAT(product_id) as products FROM tabla_products3 WHERE date = '{date}' AND advertiser_id = '{advertiser_id}' AND modelo = 'modelo_1' AND advertiser_product IN (SELECT advertiser_product FROM tabla_products3 WHERE date = '{date}' AND advertiser_id = '{advertiser_id}' AND modelo = 'modelo_2') GROUP BY date, advertiser_id")
    datos3 = cur3.fetchone()
    return {"advertiser_id": advertiser_id, "date": date, "products": datos3[3]}


#History
#/history/<ADV>/
#Esta entrada devuelve un JSON con todas las recomendaciones para el advertirse pasado por parámetro en los últimos 7 días.

from typing import Optional
@app.get("/history/")
async def get_products(advertiser_id: str, modelo: str, date: Optional[str] = None):
    if date is None:
        date = datetime.today().strftime('%Y-%m-%d')
    else:
        date = datetime.strptime(date, '%Y-%m-%d')

    conn = sqlite3.connect(':memory:')
    resultados_consolidados.to_sql('tabla_products4', conn, index=False)
    c = conn.cursor()
    fecha_inicial = (date - timedelta(days=7)).strftime('%Y-%m-%d')
    query = f"SELECT date, product_id FROM tabla_products4 WHERE advertiser_id='{advertiser_id}' AND modelo='{modelo}' AND date BETWEEN '{fecha_inicial}' AND '{date.strftime('%Y-%m-%d')}'"
    c.execute(query)
    datos = c.fetchall()
    datos = pd.read_sql(query, conn)
    conn.close()

    results = {}
    for index, row in datos.iterrows():
        fecha = row['date']
        product_id = row['product_id']
        if fecha not in results:
            results[fecha] = [product_id]
        else:
            results[fecha].append(product_id)

    return {"advertiser_id": advertiser_id, "modelo": modelo, "results": results}

#Diferencias
@app.get("/variaciones/{modelo}")
def variaciones(modelo: str, date: str):
    conn = sqlite3.connect(':memory:')
    resultados_consolidados.to_sql('tabla_products5', conn, index=False)
    cur = conn.cursor()
    cur.execute(f"SELECT DISTINCT advertiser_id FROM tabla_products5 WHERE modelo = '{modelo}'")
    advertisers = cur.fetchall()
    result = {}
    date1 = date
    date_object = datetime.strptime(date, '%Y-%m-%d')
    next_day_object = date_object - timedelta(days=1)
    date2 = next_day_object.strftime('%Y-%m-%d')
    cur.execute(f"SELECT t1.advertiser_id, COUNT(DISTINCT t1.product_id) FROM tabla_products5 t1 WHERE t1.date = '{date1}' AND t1.product_id NOT IN (SELECT t2.product_id FROM tabla_products5 t2 WHERE t2.date = '{date2}') GROUP BY t1.advertiser_id")
    rtdo = cur.fetchall()
    result = {item[0]: {"modelo": modelo, "advertiser": item[0], "variacion_en_productos": item[1] } for item in rtdo}
    return result
