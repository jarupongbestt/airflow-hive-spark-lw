from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
            .appName("test") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://metastore:9083") \
            .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .enableHiveSupport() \
            .getOrCreate()

@dag(
    dag_id="create_new_order_detail",
    start_date=datetime(2023, 12, 1)
)
def create_new_order_detail_dag():

    @task
    def transform_load_new_order_detail():
        spark_df_order_new = spark.sql("SELECT * FROM order_detail")
        spark_df_order_new = spark_df_order_new.withColumn("discount_no_null", col("discount")).fillna(0)
        spark_df_order_new.write.mode("overwrite").saveAsTable("__order_detail_new__")

    sensor_transform_load_order_detail = ExternalTaskSensor(
        task_id="sensor_transform_load_order_detail",
        external_dag_id="etl_order_restaurant_detail",
        external_task_id="transform_load_order_detail"
    )

    end = EmptyOperator(task_id="end")
    transform_load_new_order_detail = transform_load_new_order_detail()

    sensor_transform_load_order_detail >> transform_load_new_order_detail >> end

create_new_order_detail_dag = create_new_order_detail_dag()