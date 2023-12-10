from dotenv import load_dotenv
import os

# Load environment variables
ENV_PATH = "./.env"
load_dotenv(ENV_PATH)

# airflow lib
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
            .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse") \
            .config("spark.driver.memory", "16g") \
            .config("spark.executor.memory", "16g") \
            .enableHiveSupport() \
            .getOrCreate()


@dag(
    dag_id="etl_order_restaurant_detail",
    start_date=datetime(2023, 12, 1),
    schedule_interval="@daily",
    catchup=False,
)
def order_detail_dag():
    # @task
    # def store_order_detail():

    #     try:
    #         # create and insert into order_detail table
    #         with pg_engine.connect() as conn:
    #             df_order_detail = pd.read_csv("./sources/order_detail.csv")
    #             df_order_detail["order_created_timestamp"] = pd.to_datetime(
    #                 df_order_detail["order_created_timestamp"], format="%Y-%m-%d %H:%M:%S"
    #             )
    #             print("Creating or Inserting order_detail table...")
    #             df_order_detail.to_sql(
    #                 "order_detail", conn, if_exists="replace", index=False
    #             )
    #             print("Done!")
    #     except SQLAlchemyError as e:
    #         print(f"Error while inserting or creating database: {e}")
    
    # @task
    # def store_restaurant_detail():

    #     try:
    #         with pg_engine.connect() as conn:
    #             # create and insert into restaurant table
    #             df_restaurant = pd.read_csv("./sources/restaurant_detail.csv")
    #             print("Creating or Inserting restaurant_detail table...")
    #             df_restaurant.to_sql(
    #                 "restaurant_detail", conn, if_exists="replace", index=False
    #             )
    #             print("Done!")
    #     except SQLAlchemyError as e:
    #         print(f"Error while inserting or creating database: {e}")

    @task 
    def transform_load_restaurant_detail():

        etl_restaurant_statement = """
            with 
            lastest_dt_restaurant as (
                SELECT restaurant_id, max(TO_CHAR(order_created_timestamp,'YYYYMMDD')) as dt 
                FROM public.order_detail
                GROUP BY restaurant_id
            )

            select rd.*, ldr.dt
            from public.restaurant_detail rd
            left join lastest_dt_restaurant ldr on rd.id = ldr.restaurant_id
        """

        with pg_engine.connect() as pg_conn:
            ## use pandas read postgres instead spark because cannot connect to jdbc drivers
            df_restaurant = pd.read_sql_query(etl_restaurant_statement, pg_conn)
            spark_df_restaurant = spark.createDataFrame(df_restaurant)
            spark_df_restaurant.write.mode("overwrite").format("parquet").partitionBy("dt").saveAsTable("restaurant_detail")
    
    # @task
    # def transform_load_order_detail():

    #     with pg_engine.connect() as pg_conn:
    #         ## use pandas read postgres instead spark because cannot connect to jdbc drivers
    #         df_order_detail = pd.read_sql_query("select * from public.order_detail", pg_conn)
    #         spark_df_order = spark.createDataFrame(df_order_detail) 
    #         spark_df_order = spark_df_order.withColumn("dt", date_format(col("order_created_timestamp"),"yyyymmdd"))
    #         spark_df_order.write.mode("overwrite").format("parquet").partitionBy("dt").saveAsTable("order_detail")
                

    # start = EmptyOperator(task_id="start")
    # end = EmptyOperator(task_id="end")

    # store_order_detail = store_order_detail()
    # transform_load_order_detail = transform_load_order_detail()
    # store_restaurant_detail = store_restaurant_detail()
    transform_load_restaurant_detail = transform_load_restaurant_detail()

    # trigger_create_new_order_detail = TriggerDagRunOperator(
    #     task_id="trigger_create_new_order_detail",
    #     trigger_dag_id="create_new_order_detail"
    # )

    trigger_create_new_restaurant_detail = TriggerDagRunOperator(
        task_id="trigger_create_new_restaurant_detail",
        trigger_dag_id="create_new_restaurant_detail"
    )



    # start >> [store_order_detail, store_restaurant_detail]
    # store_order_detail >> [transform_load_order_detail,transform_load_restaurant_detail]
    # store_restaurant_detail >> [transform_load_order_detail,transform_load_restaurant_detail]
    # transform_load_order_detail >> trigger_create_new_order_detail
    transform_load_restaurant_detail >> trigger_create_new_restaurant_detail
    # [trigger_create_new_order_detail,trigger_create_new_restaurant_detail] >> end



order_detail_dag = order_detail_dag()


