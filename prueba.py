from fastapi import FastAPI
import sqlite3
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
resultados_consolidados_coincidencias=pd.read_csv("resultados_consolidados_coincidencias.csv")
app = FastAPI()

@app.get("/coincidencias/")
async def get_coincidencias(fecha: str, advertiser_id: str):
    with sqlite3.connect(':memory:') as conn:
        c = conn.cursor()

        # ejecutar la consulta SQL
        query = """
        SELECT
          date,
          advertiser_id,
          GROUP_CONCAT(product_id ORDER BY product_id) AS product_id_list
        FROM (
          SELECT
            date,
            advertiser_id,
            product_id,
            GROUP_CONCAT(modelo ORDER BY modelo) AS modelo_list
          FROM
            tu_tabla
          WHERE
            date = ? AND advertiser_id = ?
          GROUP BY
            date,
            advertiser_id,
            product_id
        ) t
        WHERE
          modelo_list = 'modelo_1,modelo_2'
        GROUP BY
          date,
          advertiser_id;
        """
        c.execute(query, (fecha, advertiser_id))
        resultados = c.fetchall()

        # convertir resultados en una lista de diccionarios
        output = []
        for r in resultados:
            output.append({
                "fecha": r[0],
                "advertiser_id": r[1],
                "product_id_list": r[2].split(",")
            })

        return output
