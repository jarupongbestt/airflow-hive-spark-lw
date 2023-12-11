# airflow-hive-spark

To run this project, you need to have docker installed on your machine.
if you don't have docker installed, you can install it from here: https://docs.docker.com/get-docker/

after installing docker, you need to run the following command to spawn postgres, hive, spark and airflow containers:
```
docker-compose build -d --build
```

For my pipeline, I used airflow to schedule etl processes. You can access airflow UI from http://localhost:8080 after running docker-compose command.

**Note:** I cannot install the jdbc driver for pyspark to read/write postgresql so I use pandas instead but I convert to Spark to precess. For my pipline failed when I tested it on local machine. This is my error logs 
! [alt text](airflow-block-failed-logs.png "Airflow Failed Logs")

So you can try to run each etl processes with hive-spark.ipynb and store_csv.ipynb in playground/notebooks directory. I have no idea why it failed on airflow but it works on my local machine(ipynb).