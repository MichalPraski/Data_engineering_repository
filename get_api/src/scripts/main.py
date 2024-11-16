import pandas as pd
import numpy as np
import os
import sys
import requests
import pathlib
from pathlib import Path
sys.path.append(Path()/'src'/'scripts')
import Data_engineering_repository.get_api.src.modules.functions as func
pd.set_option('display.max_columns', None)
import time
from azure.storage.blob import ContainerClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
global con_string, container_name
conn_string = os.getenv('conn_string')
container_name = os.getenv('container_name')

def extract():

    print(conn_string)
    print(container_name)

    start_time = time.time()
    data_path = Path()/'src'/'data'
    
    # station df
    print("Creating station_df")
    station_df, stations_id_list = func.get_measuring_station_data()

    func.dataframe_to_file(
        data_path=data_path,
        dataframe_name='station_df', 
        input_dataframe=station_df
    )
    func.upload_files(
        file=station_df,
        filename='station_df',
        connection_string=conn_string,
        container_name=container_name
    )

    # measuring_emplacement_data
    print("Creating measuring_emplacement_data")
    measuring_emplacement_data, emplacement_id_list = func.get_measuring_emplacement_data(
        stations_id_list=stations_id_list
    )
    func.dataframe_to_file(
        data_path=data_path,
        dataframe_name='measuring_emplacement_data',  
        input_dataframe=measuring_emplacement_data
    )
    # # func.upload_files(
    # #     file=measuring_emplacement_data,
    # #     filename='measuring_emplacement_data',
    # #     connection_string=conn_string,
    # #     container_name=container_name
    # # )

    # measuring_data
    print("Creating measuring_data")
    measuring_data = func.get_measuring_data(
        emplacement_id_list=emplacement_id_list
    )
    func.dataframe_to_file(
        data_path=data_path, 
        dataframe_name='measuring_data', 
        input_dataframe=measuring_data
    )
    # # func.upload_files(
    # #     file=measuring_data,
    # #     filename='measuring_data',
    # #     connection_string=conn_string,
    # #     container_name=container_name
    # # )

    # air_quality_index
    print('Creating air quality index data')
    air_index_data = func.get_air_index_data(
        stations_id_list = stations_id_list
    )
    func.dataframe_to_file(
        data_path=data_path, 
        dataframe_name='air_index_data', 
        input_dataframe=air_index_data
    )
    # # func.upload_files(
    # #     file=air_index_data,
    # #     filename='air_index_data',
    # #     connection_string=conn_string,
    # #     container_name=container_name
    # # )


    print('All dataframes created succesfully!')
    end_time = time.time()
    execution_time =  end_time - start_time
    print(f'Execution time: {execution_time}')
    return station_df, measuring_emplacement_data, measuring_data, air_index_data

def main():

    station_df, measuring_emplacement_data, measuring_data, air_index_data = extract()

if __name__=='__main__':
    main()