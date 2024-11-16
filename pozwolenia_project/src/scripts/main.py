# mydag3

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import zipfile
import os
import sys
import pandas as pd
from airflow.operators.python_operator import PythonVirtualenvOperator
from sqlalchemy import create_engine, inspect
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import psycopg2
from google.cloud import bigquery

from airflow.operators.python_operator import PythonOperator
from pandas_gbq import to_gbq
from google.api_core.exceptions import Conflict
import pyarrow as pa
from dotenv import load_dotenv, dotenv_values


load_dotenv()

base_path = os.getcwd()
modules_path = os.path.join(base_path, '..', 'modules')

if modules_path not in sys.path:
    sys.path.append(modules_path)
    
import functions as func

engine = create_engine('postgresql://my_user:my_password@/my_db?host=/cloudsql/data-engineering-391216.test')

url = "https://wyszukiwarka.gunb.gov.pl/pobranie.html"
base_url = "https://wyszukiwarka.gunb.gov.pl/"
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}


with DAG('my_dag',
         default_args=default_args,
         description='A simple tutorial DAG',
         schedule_interval='@once',
         ) as dag:

    task1 = PythonOperator(
        task_id='download_data2',
        python_callable=func.download_data,
        # requirements=requirements,
        python_version="3.10.9"
        # dag=dag
        )

    task2 = PythonOperator(
        task_id='insert_data_to_db',
        python_callable=func.insert_data_to_db,
        # requirements=requirements,
        python_version="3.10.9"
        # dag=dag
        )

    task3 = PythonOperator(
        task_id='fetch_transform_aggregate',
        python_callable=func.fetch_transform_aggregate,
        op_kwargs={'column_name': 'jednostki_numer', 'n_months': 3, 'today': '2023-06-02'},
        # requirements=requirements_list,
        python_version="3.10.9"
        # dag=dag
        )


    # Definiowanie zależności między zadaniami
    task1 >> task2 >> task3