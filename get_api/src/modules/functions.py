import pandas as pd
import numpy as np
import os
import sys
import requests
import pathlib
from pathlib import Path
import time
from azure.storage.blob import ContainerClient
from datetime import datetime


def get_measuring_station_data():
    """
    Function for getting measuring station data from get api:
    https://api.gios.gov.pl/pjp-api/rest/station/findAll
    params:
    return: dataframe of measuring station
    """
    # getting response json in form of pandas dataframe
    response = requests.get(r'https://api.gios.gov.pl/pjp-api/rest/station/findAll')
    if response.status_code == 200:
        print("Request was successful!")
        df = pd.DataFrame(response.json())
    else:
        print("Request failed!")

    # unpack city column from dict form to several columns: [col_names]
    col_names = ['city_id', 'city_name', 'comune_name', 'district_name', 'province_name']
    dict_keys_list = ['id', 'name', 'communeName', 'districtName', 'provinceName']

    for index, col_set in enumerate(zip(col_names, dict_keys_list)):
        if index < 2:
            df[f'{col_set[0]}'] = df['city'].apply(lambda x: x[f'{col_set[1]}'])
        else:
            df[f'{col_set[0]}'] = df['city'].apply(lambda x: x['commune'][f'{col_set[1]}'])

    # droping city column after unpacking columns
    df = df.drop(columns='city', axis=1)

    # getting list of unique station id elements
    stations_id_list = df['id'].unique().tolist()
    stations_id_list.sort()

    return df, stations_id_list


def get_measuring_emplacement_data(stations_id_list):
    """
    Function for getting measuring emplacement data from get api
    Warning! function can only be implemented upon reaching measuring station data
    params:
        -  measuring_station_df: measuring station data
    return: measuring emplacement_data
    """
    stations_id_list = stations_id_list
    measuring_emplacement_dfs = []
    _col_names = ['param_name', 'param_formula', 'param_code', 'id_param']
    _dict_keys = ['paramName', 'paramFormula', 'paramCode', 'idParam']

    for station in stations_id_list:
        response = requests.get(fr'https://api.gios.gov.pl/pjp-api/rest/station/sensors/{station}')
        if response.status_code == 200:
            # print("Request was successful!")
            df = pd.DataFrame(response.json())
        else:
            print("Request failed!")
            break

        for col_name, dict_key in zip(_col_names, _dict_keys):
            df[f'{col_name}'] = df['param'].apply(lambda x: x[f'{dict_key}'])

        df = df.drop(columns='param', axis=1)
        measuring_emplacement_dfs.append(df)

    measuring_emplacement_df = pd.concat(measuring_emplacement_dfs)

    emplacement_id_list = list(measuring_emplacement_df['id'])
    emplacement_id_list.sort()
    
    return measuring_emplacement_df, emplacement_id_list


def get_measuring_data(emplacement_id_list):
    """
    Function for getting measuring data from get api
    Warning! function can only be implemented upon reaching measuring empacement data
    params:
        -  measuring_emplacement_df: measuring emplacement data
    return: measuring emplacement_data
    """
    emplacement_id_list = emplacement_id_list
    measuring_data_dfs = []
    _dict_keys = ['date', 'value']

    for code in emplacement_id_list:
        try:
            response = requests.get(fr'https://api.gios.gov.pl/pjp-api/rest/data/getData/{code}')
        except ValueError:
            continue
        if response.status_code == 200:
            df = pd.DataFrame(response.json())
        else:
            print("Request failed!")
            continue

        for dict_key in _dict_keys:
            df[f'{dict_key}'] = df['values'].apply(lambda x: x[f'{dict_key}'])

        df = df.drop(columns='values', axis=1)
        measuring_data_dfs.append(df)
    measuring_data_dfs
    df = pd.concat(measuring_data_dfs)
    
    return df


def get_air_index_data(stations_id_list):
    """
    Function or getting air quality index data
    params:
        - station_id_list -> list with unique station id elements
    return air quality index dataframe
    """
    # declaration of lists
    stations_id_list = stations_id_list
    measuring_data_dfs = []

    # loop for creating data for each station id
    for code in stations_id_list:
        response = requests.get(fr'https://api.gios.gov.pl/pjp-api/rest/aqindex/getIndex/{code}')
        if response.status_code == 200:
            df = pd.DataFrame(response.json())
        else:
            print("Request failed!")

        # transformation of data
        df = df.reset_index(drop=True).head(1)

        # appending data to measuring_data_dfs
        measuring_data_dfs.append(df)

    # concating all data to one dataframe
    df_all = pd.concat(measuring_data_dfs)

    return df_all


def dataframe_to_file(data_path, input_dataframe, dataframe_name):
    """
    Function that dumps pandas or pyspark dataframe into file and saves
    created file into data directory.
    If data directory does not exists, this category will be created automaticly
    with following function
    """
    input_dataframe.to_csv(fr'/Users/michau/Desktop/wszystko/programowanie/DE/get_api/src/data/{dataframe_name}.csv')

def upload_files(file, filename, connection_string, container_name):
    """
    Function to upload data into azure blob
    """
    path = filename
    container_client  = ContainerClient.from_connection_string(connection_string, container_name)
    print("Uploading files to blob storage.................")
    blob_client = container_client.get_blob_client(path)
    blob_client.upload_blob(file, overwrite=True)
    print(f'{path} uploaded to blob storage')