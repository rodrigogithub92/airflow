
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

import numpy as np
import pandas as pd
import boto3
import io


def filtrar_datos3():

    def levantar_archivos(ads_views_file_name,advertiser_id_file_name,product_views_file_name):
        
        s3 = boto3.client("s3")

        bucket_name = "udesa-tp-grupo-9"

        s3_file_name_ads_views = ads_views_file_name

        s3_file_name_advertiser_id = advertiser_id_file_name

        s3_file_name_product_views = product_views_file_name

        obj_ads_views = s3.Object(bucket_name, s3_file_name_ads_views)

        obj_product_views = s3.Object(bucket_name, s3_file_name_product_views)

        obj_advertiser_id = s3.Object(bucket_name, s3_file_name_advertiser_id)

        body_ads_views = obj_ads_views.get()['Body'].read().decode('utf-8')

        body_product_views = obj_product_views.get()['Body'].read().decode('utf-8')

        body_advertiser_id = obj_advertiser_id.get()['Body'].read().decode('utf-8')

        ads_views1 = pd.read_csv(body_ads_views)
        
        advertiser_id1 = pd.read_csv(body_advertiser_id)
            
        product_views1 = pd.read_csv(body_product_views)
                        
        return ads_views1, advertiser_id1, product_views1
    
    ads_views1, advertiser_id1, product_views1 = levantar_archivos('ads_views.csv','advertiser_id.csv', 'product_views.csv')
    
    def transformar_formato_fecha():
        
        ads_views1['date'] = pd.to_datetime(ads_views1['date'])
            
        product_views1['date'] = pd.to_datetime(product_views1['date'])
            
            #edited_date_df = {'ads_views2':ads_views1, 'product_views2':product_views1}
            
        return ads_views1, product_views1
        
    ads_views1, product_views1 = transformar_formato_fecha()
    
    def filtramos_por_dia_anterior():
    
        from datetime import datetime, timedelta

        yesterday = datetime.now() - timedelta(days=1)

        yesterday_str = yesterday.strftime('%Y-%m-%d')

        ads_views_yesterday = ads_views1[ads_views1['date'] == yesterday_str]

        product_views_yesterday = product_views1[product_views1['date'] == yesterday_str]
            
        return ads_views_yesterday, product_views_yesterday
        
    ads_views_yesterday, product_views_yesterday = filtramos_por_dia_anterior()
       
    #Nos quedamos con los advertiser_id que están en "advertiser_id.csv"

    def filtramos_por_advertiser(ads_views_yesterday, product_views_yesterday):
    
        ads_views_yesterday_filtered_by_adversiterid = ads_views_yesterday[ads_views_yesterday['advertiser_id'].isin(advertiser_id1['advertiser_id'])]

        product_views_yesterday_filtered_by_adversiterid = product_views_yesterday[product_views_yesterday['advertiser_id'].isin(advertiser_id1['advertiser_id'])]
        
        return ads_views_yesterday_filtered_by_adversiterid, product_views_yesterday_filtered_by_adversiterid
        
    ads_views_yesterday_filtered_by_adversiterid, product_views_yesterday_filtered_by_adversiterid = filtramos_por_advertiser(ads_views_yesterday, product_views_yesterday)
    
    #ads_views_yesterday_filtered_by_adversiterid.to_csv('/workspace/ads_views_filtered.csv', index=False)

    #product_views_yesterday_filtered_by_adversiterid.to_csv('/workspace/product_views_filtered.csv', index=False)

    #Pasamos los archivos a S3
    
    csv_buffer_ads_views = io.StringIO()

    csv_buffer_product_views = io.StringIO()

    ads_views_yesterday_filtered_by_adversiterid.to_csv(csv_buffer_ads_views, index=False)

    product_views_yesterday_filtered_by_adversiterid.to_csv(csv_buffer_product_views, index=False)

    ads_views_file_name_s3 = 'ads_views_filtered_' + datetime.today().strftime('%Y-%m-%d') + '.csv'

    product_views_file_name_s3 = 'product_views_filtered_' + datetime.today().strftime('%Y-%m-%d') + '.csv'

    s3.Object(bucket_name,ads_views_file_name_s3).put(Body=csv_buffer_ads_views.getvalue())

    s3.Object(bucket_name,product_views_file_name_s3).put(Body=csv_buffer_product_views.getvalue())

    return ads_views_yesterday_filtered_by_adversiterid, product_views_yesterday_filtered_by_adversiterid,advertiser_id1


def Modelo_TopCTR_TopProduct():

    s3 = boto3.client("s3")

    bucket_name = "udesa-tp-grupo-9"

    s3_file_name_ads_views_filtered = 'ads_views_filtered_' + datetime.today().strftime('%Y-%m-%d') + '.csv'

    s3_file_name_product_views_filtered = 'product_views_filtered_' + datetime.today().strftime('%Y-%m-%d') + '.csv'

    obj_ads_views_filtered = s3.Object(bucket_name, s3_file_name_ads_views_filtered)

    obj_product_views_filtered = s3.Object(bucket_name, s3_file_name_product_views_filtered)

    body_ads_views_filtered = obj_ads_views_filtered.get()['Body'].read().decode('utf-8')

    body_product_views_filtered = obj_product_views_filtered.get()['Body'].read().decode('utf-8')

    ads_views_yesterday_filtered_by_adversiterid = pd.read_csv(body_ads_views_filtered)
            
    product_views_yesterday_filtered_by_adversiterid = pd.read_csv(body_product_views_filtered)

    def Modelo_TopCTR():

        grouped_df = ads_views_yesterday_filtered_by_adversiterid.groupby(["advertiser_id","product_id","type"]).size().reset_index(name='count')

            #Armamos tabla pivot

        pivoted_df = grouped_df.pivot_table(index=['advertiser_id', 'product_id'], columns='type', values='count', fill_value=0).reset_index()

            #Renombramos las columnas

        pivoted_df = pivoted_df.rename(columns={'impression': 'counts_impressions', 'click': 'counts_clicks'})

            #Agregamos la columna 'CTR' con el ratio entre 'counts_clicks' y 'count_impresions'

        pivoted_df['CTR'] = pivoted_df['counts_clicks']/pivoted_df['counts_impressions']

        Tabla_para_CTR_results = pivoted_df.groupby(['advertiser_id']).apply(lambda x: x.nlargest(20, 'CTR')).reset_index(drop=True)

            #Limpiamos el dataframe quedandonos únicamente con las columnas advertiser_id y product_id

        CTR_Results = Tabla_para_CTR_results.drop(columns=['counts_clicks','counts_impressions','CTR'])

        return CTR_Results

    def Modelo_TopProduct():
        
        grouped = product_views_yesterday_filtered_by_adversiterid .groupby(['advertiser_id', 'product_id'])

        counts = grouped.size()

        top_20_products_views_by_advertiser = counts.groupby('advertiser_id').nlargest(20).reset_index(level=0, drop=True)

        TopProduct_Results = top_20_products_views_by_advertiser.to_frame(name='counts')
            
        return TopProduct_Results
        
    CTR_Results = Modelo_TopCTR()
    
    TopProduct_Results = Modelo_TopProduct()

    #CTR_Results.to_csv('/workspace/ctr_results.csv')

    #TopProduct_Results.to_csv('/workspace/topproduct_results.csv')
    
    csv_buffer_ctr_results = io.StringIO()

    csv_buffer_top_product = io.StringIO()

    CTR_Results.to_csv(csv_buffer_ctr_results, index=False)

    TopProduct_Results.to_csv(csv_buffer_top_product, index=False)

    CTR_Results_file_name_s3 = 'CTR_Results_' + datetime.today().strftime('%Y-%m-%d') + '.csv'

    TopProduct_Results_file_name_s3 = 'TopProduct_Results_' + datetime.today().strftime('%Y-%m-%d') + '.csv'

    s3.Object(bucket_name,CTR_Results_file_name_s3).put(Body=csv_buffer_ctr_results.getvalue())

    s3.Object(bucket_name,TopProduct_Results_file_name_s3).put(Body=csv_buffer_top_product.getvalue())
    
    return CTR_Results, TopProduct_Results


with DAG(
	dag_id='Pipeline',
	schedule_interval=None,
	start_date=datetime.datetime(2022, 4, 27),
	catchup=False,
) as dag:
     
    Filtrar_Datos = PythonOperator(
 		task_id='FiltrarDatos',
 		python_callable=filtrar_datos3
	)

    Modelos_TopCTR_TopProduct = PythonOperator(
 		task_id='Generacion_Recomendaciones',
 		python_callable=Modelo_TopCTR_TopProduct
	)
    

    Filtrar_Datos >> Modelos_TopCTR_TopProduct