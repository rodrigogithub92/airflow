from datetime import datetime
import sqlite3
import numpy as np
import pandas as pd
import datetime
import os
from fastapi import FastAPI, File, UploadFile
#from pathlib import Path
#import matplotlib.pyplot as plt
#from tabulate import tabulate
#import psycopg2
#import random
import string


#Cargar datos
#Cargar datos
resultados_consolidados=pd.read_csv("resultados_consolidados.csv")

#recomendaciones por advertiser y modelo
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


@app.get("/stats/")
async def get_stats():
    conn = sqlite3.connect(':memory:')#crea una conexión a la base
    resultados_consolidados.to_sql('tabla_products2', conn, index=False) #aca "tabla_product2" es el nombre de la nueva tabla
    cur2 = conn.cursor()
    cur2.execute('SELECT DISTINCT advertiser_id FROM tabla_products2')
    resultados = cur2.fetchall()
    return {"advertiser_ids": [resultado[0] for resultado in resultados]}


#Coincidencias (falta filtrar fecha)

#Cargar datos
resultados_consolidados_coincidencias=pd.read_csv("resultados_consolidados_coincidencias.csv")

@app.get("/coincidencias/")
async def coincidencias(advertiser_id: str, date: str):
    conn = sqlite3.connect(':memory:')
    resultados_consolidados_coincidencias.to_sql('tabla_products3', conn, index=False) #aca "product" es el nombre de la nueva tabla
    cur3 = conn.cursor()
    cur3.execute(f"SELECT date, advertiser_id, advertiser_product, GROUP_CONCAT(product_id) as products FROM tabla_products3 WHERE modelo = 'modelo_1' AND advertiser_product IN (SELECT advertiser_product FROM tabla_products3 WHERE modelo = 'modelo_2')")
    datos3 = cur3.fetchone()
    return {"advertiser_id": advertiser_id, "date": date, "products": datos3[3]}


#Coincidencias2 (falta filtrar fecha)

#Cargar datos
resultados_consolidados_coincidencias=pd.read_csv("resultados_consolidados_coincidencias.csv")

@app.get("/coincidencias2/")
async def coincidencias2(advertiser_id: str, date: str):
    conn = sqlite3.connect(':memory:')
    resultados_consolidados_coincidencias.to_sql('tabla_products3', conn, index=False) #aca "product" es el nombre de la nueva tabla
    cur3 = conn.cursor()
    cur3.execute(f"SELECT DISTINCT date, advertiser_id, product_id, advertiser_product FROM tabla_products3 WHERE modelo='modelo_1' OR modelo='modelo_2' GROUP BY date, advertiser_id HAVING COUNT(DISTINCT modelo) = 2")
    datos3 = cur3.fetchone()
    return {"advertiser_id": advertiser_id, "date": date, "products": datos3[2]}

#Histoy



######## fecha opcional
from typing import Optional
@app.get("/history2/")
async def get_products(advertiser_id: str, modelo: str, date: Optional[str] = None):
    conn = sqlite3.connect(':memory:')
    c = conn.cursor()
    resultados_consolidados.to_sql('tabla_products4', conn, index=False)

    # Si no se especifica una fecha, se buscan todas las fechas
    if date is None:
        query = f"SELECT DISTINCT date FROM tabla_products4 WHERE advertiser_id='{advertiser_id}' AND modelo='{modelo}'"
        c.execute(query)
        fechas = [row[0] for row in c.fetchall()]
    else:
        fechas = [date]

    productos = {}
    for fecha in fechas:
        query = f"SELECT product_id FROM tabla_products4 WHERE advertiser_id='{advertiser_id}' AND modelo='{modelo}' AND date='{fecha}'"
        c.execute(query)
        productos_fecha = [row[0] for row in c.fetchall()]
        productos[fecha] = productos_fecha

    return {"advertiser_id": advertiser_id, "modelo": modelo, "productos_por_fecha": productos}


###########3días anteriores

from datetime import datetime, timedelta
from typing import Optional

@app.get("/history3/")
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
