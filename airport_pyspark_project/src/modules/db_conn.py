import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

class db_conn:
    def __init__(self, path):
        self.path = path
        self.db_name = ""
        self.host = ""
        self.port = ""
        self.user = ""
        self.password = ""
        self._load_config()

    def _load_config(self):
        """
        Load DB connection info from config file
        """
        global spark
        variables = []
        with open(self.path, 'r') as file:
            for line in file:
                if "=" in line:
                    variable = line.split("=")[1].strip()
                    variables.append(variable)

        self.db_name, self.host, self.port, self.user, self.password = variables

        # Inicjalizacja Sparksession
        # spark = SparkSession.builder \
        #                     .appName("My_app") \
        #                     .getOrCreate()
        spark = SparkSession.builder.config("spark.jars", "postgresql-42.0.0.jar") \
					.config("spark.driver.extraClassPath", "/Users/michau/Desktop/wszystko/programowanie/DE/pyspark/postgresql-42.5.4.jar") \
					.master("local") \
					.appName("My_app") \
					.getOrCreate()

    def run_query(self, query:str):
        """
        Function to run queries
        """
        engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}")

        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)

        spark=SparkSession.builder.appName('NAZWA_APLIKACJI').getOrCreate()
        df = spark.createDataFrame(df)

        return df

        # spark
        # df = spark.read \
        #     .jdbc(url="jdbc:postgresql://localhost:5432/postgres",
        #           table=query,
        #           properties={f"user": "{self.user}", "password":"{self.password}", "driver":"org.postgresql.Driver"})

    def save_to_db(self, df, table_name, if_exists='replace', index=False):
        """
        Save DataFrame to the PostgreSQL database
        """
        engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}")
        
        df.to_sql(table_name, engine, if_exists=if_exists, index=index)


    def get_spark_df(self, table_name:str):
        """
        Function to get spark dataframe from postgresql database
        """
        df = spark.read \
                  .format("jdbc") \
                  .option("url", "jdbc:postgresql://localhost:5432/postgres") \
                  .option("dbtable", f"bookings.{table_name}") \
                  .option("user", "postgres") \
                  .option("password", "postgres") \
                  .load()
                  # .option("driver", "org.postgresql.Driver") \
        df.printSchema()

        # creating views to be able to create sql queries on pyspark dataframes
        # df.createOrReplaceTempView(table_name)
        # return table_name
        return df