from dotenv import load_dotenv
import os

# Load environment variables
ENV_PATH = "./.env"
load_dotenv(ENV_PATH)

# airflow lib
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# data processing lib
import pandas as pd
from pyhive import hive
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, when
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


db_host = os.getenv("POSTGRES_HOST", "backend-postgres")
db_port = os.getenv("POSTGRES_PORT", "5432")
db_user = os.getenv("POSTGRES_USER", "postgres")
db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
db_database = os.getenv("POSTGRES_DATABASE", "test")

# Create a connection to the PostgreSQL database
pg_engine = create_engine(
    f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}"
)

spark = SparkSession.builder \
            .appName("test") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
            .config("spark.sql.warehouse.dir", "/opt/hive/warehouse") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .enableHiveSupport() \
            .getOrCreate()

@dag(
    dag_id="etl_restaurant_detail",
    start_date=datetime(2023, 12, 1),
    schedule_interval="@daily",
    catchup=False,
)
def restaurant_detail_dag():

    @task
    def store_restaurant_detail():

        try:
            with pg_engine.connect() as conn:
                # create and insert into restaurant table
                df_restaurant = pd.read_csv("./sources/restaurant_detail.csv")
                print("Creating or Inserting restaurant_detail table...")
                df_restaurant.to_sql(
                    "restaurant_detail", conn, if_exists="replace", index=False
                )
                print("Done!")
        except SQLAlchemyError as e:
            print(f"Error while inserting or creating database: {e}")

    @task
    def transform_load_restaurant_detail():

        with pg_engine.connect() as pg_conn:
            ## use pandas read postgres instead spark because cannot connect to jdbc drivers
            df_order_detail = pd.read_sql_query("select * from public.order_detail", pg_conn)
            spark_df_order = spark.createDataFrame(df_order_detail) 
            spark_df_order = spark_df_order.withColumn("dt", date_format(col("order_created_timestamp"),"yyyymmdd"))
            spark_df_order.write.mode("overwrite").format("parquet").partitionBy("dt").saveAsTable("order_detail")
    
    @task 
    def create_new_restaurant_detail():

        spark_df_restaurant_new = spark.sql("SELECT * FROM restaurant_detail")
        spark_df_restaurant_new = spark_df_restaurant_new.withColumn(
            "cooking_bin", 
            when(col("esimated_cooking_time").between(1,40),1)
            .when(col("esimated_cooking_time").between(41,80),2)
            .when(col("esimated_cooking_time").between(81,120),3)
            .when(col("esimated_cooking_time")>120,4)
            .otherwise(0)
        )
        spark_df_restaurant_new.write.mode("overwrite").saveAsTable("__restaurant_detail_new__")
                

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    

    store_restaurant_detail = store_restaurant_detail()
    transform_load_restaurant_detail = transform_load_restaurant_detail()
    create_new_restaurant_detail = create_new_restaurant_detail()

    start >> store_restaurant_detail
    store_restaurant_detail >> transform_load_restaurant_detail
    transform_load_restaurant_detail >> create_new_restaurant_detail
    create_new_restaurant_detail >> end

restaurant_detail_dag = restaurant_detail_dag()