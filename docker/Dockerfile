# Use the official Airflow image as the base
FROM apache/airflow:2.3.0

# Switch to root user to install system dependencies
USER root

# Install dependencies: wget, unzip, and others
RUN apt-get update && apt-get install -y wget unzip && \
    wget https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-amd64.zip && \
    unzip duckdb_cli-linux-amd64.zip && \
    mv duckdb /usr/local/bin/duckdb && \
    rm duckdb_cli-linux-amd64.zip

# Switch back to airflow user after installation
USER airflow

RUN pip install duckdb requests

# Copy DAGs into the Airflow container
COPY ./dags /opt/airflow/dags
