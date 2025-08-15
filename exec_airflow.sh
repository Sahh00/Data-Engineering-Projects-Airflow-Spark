#!/bin/bash

# Ativa o ambiente virtual
source venv/bin/activate

# Define a variável de ambiente AIRFLOW_HOME
export SPARK_HOME=/home/dev-exata/Documents/projects/alura_airflow/spark-3.1.3-bin-hadoop3.2
export AIRFLOW_HOME=/home/dev-exata/Documents/projects/alura_airflow/ProjectAirflow

# Roda o comando airflow standalone
airflow standalone