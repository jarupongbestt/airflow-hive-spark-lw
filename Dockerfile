FROM apache/airflow:2.7.3-python3.11

WORKDIR /app

USER root
# Install system dependencies for psycopg2
RUN apt-get update \
    && apt-get install -y libpq-dev gcc libpq5 \
    && apt-get install -y --no-install-recommends openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /     

RUN pip install -U pip \
    && pip install -r /requirements.txt

USER root 

COPY ./sources /app/sources


