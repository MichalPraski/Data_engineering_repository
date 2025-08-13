from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import zipfile
import os
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

user = os.getenv("user")
password = os.getenv("password")
host = os.getenv("host")
port = os.getenv("port")
database = os.getenv("database")

engine = create_engine(f'postgresql://{user}:{password}@/my_db?host=/cloudsql/data-engineering-391216.test')


url = "https://wyszukiwarka.gunb.gov.pl/pobranie.html"
base_url = "https://wyszukiwarka.gunb.gov.pl/"
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

def download_data():
    """
    Function for downloading zip file from webpage and 
    unpacking downloaded file to csv format.
    """
    # downloading data from webpage
    a = soup.select_one('div.col-md-12 ol li:nth-child(17) a')
    file_url = os.path.join(base_url, a['href'].lstrip('./'))
    response = requests.get(file_url, stream=True)
    os.makedirs(my_directory, exist_ok=True)
    filename = os.path.join(my_directory, file_url.split('/')[-1])

    with open(filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    # unpacking zip file
    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall(my_directory)

    # deleting zip files from my directory
    for file in os.listdir(my_directory):
        if file.endswith(".zip"):
            os.remove(os.path.join(my_directory, file))


def insert_data_to_db():
    """
    Function for inserting dataframe into database using PostgreSQL and SQLAlchemy
    """
    engine = create_engine('postgresql://{user}:{password}@host.docker.internal:{port}/{database}')
    inspector = inspect(engine)
    filename = './download/wynik_zgloszenia.csv'
    table_name = 'pozwolenia'
    chunksize = 2000
    
    # Create variable if is_table_exist
    if inspector.has_table(table_name):
        
        # If dataframe exists, load newest one
        existing_latest_date = pd.read_sql(f"SELECT MAX(data_wplywu_wniosku_do_urzedu) FROM {table_name}", engine).values[0][0]
        
        # existing_latest_date powinien być obiektem datetime, jeśli kolumna 'data' jest prawidłowo sformatowana
        for chunk in pd.read_csv(filename, sep='#', chunksize=chunksize):

            chunk = chunk.loc[chunk['data_wplywu_wniosku_do_urzedu'] > existing_latest_date]
            if not chunk.empty:
                rows_added = len(chunk)
                print(f'Appending {rows_added} new rows to the table...')
                chunk.to_sql(table_name, engine, if_exists='append', index=False)

    else:
        
        # If dataframe does not exists, load entire csv file
        for chunk in pd.read_csv(filename, sep='#', chunksize = chunksize):
            rows_added = len(chunk)
            print(f'Inserting {rows_added} rows to the new table...')
            chunk.to_sql(table_name, engine, if_exists='append', index=False)


def fetch_data_from_db(n_months:int, today:str):
    """
    Function that retrieves data from the database for a selected time range
    params:
    n_months: time range for analysis in months
    today: last date of data for analysis
    """
    def run_query(query, params):
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        df = pd.read_sql_query(query, con=connection, params=params)
        connection.close()
        return df
    query = '''
            SELECT * 
            FROM pozwolenia 
            WHERE 
                data_wplywu_wniosku_do_urzedu::date <= %s::date and
                data_wplywu_wniosku_do_urzedu::date >= %s::date - INTERVAL '%s months'
            order by data_wplywu_wniosku_do_urzedu;
            '''
    params = (today, today, n_months)
    df = run_query(query = query, params=params)
    return df


def transform_data(df:pd.DataFrame):
    """
    Function to transform data based on dataframe input
    params:
    df: pandas data frame input to transform
    """
    ZAMIERZENIA_MAPPING = {
    'budowa nowego/nowych obiektów budowlanych' : 'budowa',
    'nadbudowa istniejącego/istniejących obiektów budowlanych' : 'nadbudowa',
    'odbudowa istniejącego/istniejących obiektów budowlanych' : 'odbudowa',
    'rozbudowa istniejącego/istniejących obiektów budowlanych' : 'rozbudowa'
    }
    df['data_wplywu_wniosku'] = pd.to_datetime(df['data_wplywu_wniosku_do_urzedu']).dt.date
    df = df.loc[df['stan'] == 'Brak sprzeciwu']
    df = df.drop(columns=['numer_ewidencyjny_urzad', 'nazwa_organu', 'obiekt_kod_pocztowy', 'ulica', 'terc', 'cecha.1', 'cecha', 'ulica_dalej', 'nr_domu', 'numer_dzialki', 'numer_arkusza_dzialki',                                   'nazwisko_projektanta', 'imie_projektanta', 'projektant_numer_uprawnien', 'projektant_pozostali', 'data_wplywu_wniosku_do_urzedu', 'numer_ewidencyjny_system',                                               'miasto', 'nazwa_zam_budowlanego', 'kubatura', 'stan', 'obreb_numer']
    )

    df = df.sort_values(by=['data_wplywu_wniosku'], ascending=True)
    df = df.reset_index(drop=True)
    df['id'] = df.index
    df_cols = list(df.columns)
    df_cols = [df_cols[-1]] + [df_cols[-2]] + df_cols[:-2]
    df = df[df_cols]
    df['rodzaj_zam_budowlanego'] = df['rodzaj_zam_budowlanego'].map(ZAMIERZENIA_MAPPING)
    
    # matching column "jednostki_numer"
    df['jednostki_numer'] = df['jednostki_numer'].str.replace("_", "")
    df = df[df['jednostki_numer'].apply(len) <= 7]
    def change_code(code):
        if int(code[-1]) in [8,9]:
            return code[:-1]+"1"
        if int(code[-1]) in [4,5]:
            return code[:-1]+"3"
        return code
    df['jednostki_numer'] = df['jednostki_numer'].apply(change_code)
    
    # Creating column "jednostki_woj" and "jednostki_pow"
    df['jednostki_woj'] = df['jednostki_numer'].str[:2]
    df['jednostki_pow'] = df['jednostki_numer'].str[2:4]
    df['jednostki_woj_pow'] = df['jednostki_woj'].astype(str) + df['jednostki_pow'].astype(str)

    return df


def build_aggregates(data_frame:pd.DataFrame, n_months:int, today:str):
    """
    params:
    data_frame: Function that creates aggregates based on dataframe,
    column_name: column to create aggregate on,
    n_months: determines on how many months back the aggregate is to be created
    """
    from datetime import datetime
    import operator
    from operator import attrgetter
    df = data_frame
    today = pd.to_datetime(today)
    df['data_wplywu_wniosku'] = pd.to_datetime(df['data_wplywu_wniosku'])
    df['months'] = (today.to_period('M') - df['data_wplywu_wniosku'].dt.to_period('M')).apply(attrgetter('n'))

    # yielding x last months from data
    cutoff_date = today - pd.DateOffset(months=n_months)
    cutoff_date = cutoff_date.to_pydatetime().date()

    today_date = today.date()
    df = df.loc[(df['data_wplywu_wniosku'] >= pd.Timestamp(cutoff_date)) & (df['data_wplywu_wniosku'] <= pd.Timestamp(today_date))]

    df_group = pd.pivot_table(df, index='jednostki_numer', columns=('kategoria', 'rodzaj_zam_budowlanego'), values='id', aggfunc='count').fillna(0)
    df_group_woj = pd.pivot_table(df, index='jednostki_woj', columns=('kategoria', 'rodzaj_zam_budowlanego'), values='id', aggfunc='count').fillna(0)
    df_group_woj_pow = pd.pivot_table(df, index='jednostki_woj_pow', columns=('kategoria', 'rodzaj_zam_budowlanego'), values='id', aggfunc='count').fillna(0)

    dataframes = [df_group, df_group_woj, df_group_woj_pow]
    for dataframe in dataframes:
        dataframe.columns = ['cat_' + '_'.join(element) + f"_last_{n_months}_months" if type(element) is tuple else 'cat_' + element + f"_last_{n_months}_months" for element in dataframe.columns]
    df_all_group = pd.concat(dataframes)

    return df_all_group


def fetch_transform_aggregate(n_months:int, today:str):
    """
    A function that retrieves data from a database, then transforms that data and creates an aggregate
    params:
    column_name - name of column to create aggregate
    n_months -
    next, function that inserts final dataframe to google cloud platform big query
    param: df -> final dataframe
    """
    dataframes = []
    for month in range(1, n_months+1):
        df = fetch_data_from_db(n_months=month, today=today)
        df = transform_data(df=df)
        df = build_aggregates(data_frame=df, n_months=month, today=today)
        dataframes.append(df)
    
    # Concatenation of all dataframes
    df = pd.concat(dataframes, axis=1)


    df.reset_index(inplace=True)
    df = df.rename(columns=({'index':'kod', 'JPT_NAZWA_':'voivodeship', 'JPT_SJR_KO':'area_type', 'JPT_KOD_JE':'area_type_number', 'RODZAJ':'area_type2'}))

    client = bigquery.Client()
    
    df.columns = df.columns.str.replace('.', '_')

    project_id = 'data-engineering-391216'
    dataset_id = 'pozwolenia_dataset'

    # Creating new dataset
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"

    # trying to create dataset
    try:
        dataset = client.create_dataset(dataset)
    except Conflict:
        print(f'Dataset {dataset_id} already exists.')

    # saving final table to big query
    df.to_gbq('pozwolenia_dataset.pozwolenia_final_result', project_id=project_id, if_exists='replace')
    print(f'Table loaded successfully.')