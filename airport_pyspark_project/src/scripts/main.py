import pandas as pd
import numpy as np
import sys
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from pyspark.sql import SQLContext
from pyspark import SparkContext
import re
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from pyspark.sql.functions import col, format_string, date_format, format_number
from pyspark.sql.functions import count, sum, avg, expr
from pyspark.sql import functions as F
warnings.filterwarnings('ignore')

from google.cloud import storage


spark = SparkSession.builder \
        .appName('Airports_aggregates_per_day') \
        .config('spark.jars', 'gs://dataproc-staging-europe-west1-305196457839-tm6piscn/notebooks/jupyter/FLIGHTS/app/bigquery_connection/spark-3.1-bigquery-0.28.0-preview.jar') \
        .getOrCreate()

# Ustawienie projekt BigQuery i ścieżkę do tabeli
def get_df(table_name:str, spark):
    project_id = 'mentoring-372319'
    dataset_id = 'flights_database'
    table_id = table_name
    full_table_path = f"{project_id}:{dataset_id}.{table_id}"
    # Odczytaj dane z BigQuery
    df = spark.read.format('com.google.cloud.spark.bigquery.BigQueryRelationProvider').option('table', full_table_path).load()
    return df

def aggregate(spark_session, bookings, tickets, ticket_flights, flights):
    # create main dataframe
    dfa = bookings.join(tickets, on='book_ref', how='left')
    dfa = dfa.join(ticket_flights, on='ticket_no', how='left')
    dfa = dfa.join(flights, on='flight_id', how='left')
    dfa = dfa.withColumn('scheduled_departure_date', date_format(dfa['scheduled_departure'], 'yyyy-MM-dd'))
    dfa = dfa.withColumn('book_date_date', date_format(dfa['book_date'], 'yyyy-MM-dd')).dropna()
    # create delay column
    dfa = dfa.withColumn("delay", expr("actual_departure - scheduled_departure"))
    
    # 1. Suma dokonanych rezerwacji na lotnisko w ciągu dnia
    df1_agg_a = (dfa.groupBy('departure_airport', 'book_date_date')
                    .agg(count('book_ref').alias('book_ref_count'))
                    .orderBy('book_ref_count', ascending=False))

    # 2. Średnie opóźnienie odlotów z lotniska w ciągu dnia
    df_agg2 = (dfa.groupBy('departure_airport', 'book_date_date')
                    .agg(avg('delay').alias('mean_delay'))
                    .orderBy('mean_delay', ascending=False))

    # 3. średnie obłożenie lotów  z lotniska w ciągu dnia
    df_agg_3a = (dfa.groupBy('departure_airport', 'scheduled_departure_date')
                            .agg(count('flight_id').alias('flight_cnt'))
                            .orderBy('flight_cnt', ascending=False))

    df_agg_3 = (df_agg_3a.groupBy('departure_airport')
                            .agg(avg('flight_cnt').alias('flight_avg_cnt_daily'))
                            .orderBy('flight_avg_cnt_daily', ascending=False))

    # 4. Średnie obłożenie lotów w zależności od klasy przedziałowej na lotnisko w ciągu dnia
    df4_agg_a = (dfa.groupBy('departure_airport', 'fare_conditions', 'scheduled_departure_date')
                            .agg(count('flight_id').alias('flight_cnt'))
                            .orderBy('flight_cnt', ascending=False))

    df4_agg_b = (df4_agg_a.groupBy('departure_airport', 'fare_conditions')
                            .agg(avg('flight_cnt').alias('flight_avg_cnt'))
                            .orderBy('flight_avg_cnt', ascending=False))

    # 5. Średnia liczba obsłużonych pasażerów na lotnisku w ciągu dnia
    df5_agg_a = (dfa.groupBy('departure_airport', 'scheduled_departure_date')
                            .agg(count('passenger_id').alias('passenger_id_count'))
                            .orderBy('passenger_id_count', ascending=False))

    df5_agg_b = (df5_agg_a.groupBy('departure_airport')
                            .agg(avg('passenger_id_count').alias('avg_passenger_id_count'))
                            .orderBy('avg_passenger_id_count', ascending=False))

    # 6. Średnia liczba lotów z lotniska w ciągu dnia
    df6_agg_a = (dfa.groupBy('departure_airport', 'scheduled_departure_date')
                            .agg(count('flight_id').alias('flight_id_count'))
                            .orderBy('flight_id_count', ascending=False))

    df6_agg_b = (df6_agg_a.groupBy('departure_airport')
                            .agg(count('flight_id_count').alias('avg_flight_id_count'))
                            .orderBy('avg_flight_id_count', ascending=False))

    # saving dataframe to bigquery
    # converting delay column from interval to string
    dfb = dfa.withColumn("delay", F.col("delay").cast("string"))
    
    return dfb

def run_job(spark_session):
    df_aggregates = get_df(table_name='aggregates', spark = spark_session)
    df_aircrafts_data = get_df(table_name='aircrafts_data', spark = spark_session)
    df_airports_coordinates = get_df(table_name='airports_coordinates', spark = spark_session)
    df_airports_data = get_df(table_name='airports_data', spark = spark_session)
    df_boarding_passes = get_df(table_name='boarding_passes', spark = spark_session)
    df_bookings = get_df(table_name='bookings', spark = spark_session)
    df_flights = get_df(table_name='flights', spark = spark_session)
    df_seats = get_df(table_name='seats', spark = spark_session)
    df_ticket_flights = get_df(table_name='ticket_flights', spark = spark_session)
    df_ticket_flights_v1 = get_df(table_name='ticket_flights_v1', spark = spark_session)
    df_tickets = get_df(table_name='tickets', spark = spark_session)
    
    # creating aliases
    bookings = df_bookings.alias('b')
    tickets = df_tickets.alias('t')
    ticket_flights = df_ticket_flights.alias('tf')
    flights = df_flights.alias('f')
    aircrafts_data = df_aircrafts_data.alias('ad')
    seats = df_seats.alias('s')
    
    # wywołanie funkcji aggregate
    tabelka_finalna = aggregate(spark_session, bookings, tickets, ticket_flights, flights)
    
    # zapisanie finalnej tabelki do google cloud
    tabelka_finalna.write.format("com.google.cloud.spark.bigquery") \
                    .option("writeMethod", "direct") \
                    .option("temporaryGcsBucket", 'dataproc-staging-europe-central2-305196457839-jazljgsi') \
                    .option("allowUnknownTypes", "true") \
                    .save('flights_database.tabelka_finalna')
    

def main():

    run_job(spark_session = spark)


if __name__ == '__main__':
    main()