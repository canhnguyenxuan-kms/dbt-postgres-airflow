import os
import sys
import json # to load dbt's manifest.json
import pendulum # better timezone handling for DAG scheduling
from airflow import DAG #DAG object to define the workflow
from airflow.operators.bash import BashOperator # to run dbt commands/bash commands as tasks
from airflow.operators.python import PythonOperator  # to run Python functions as tasks

sys.path.append(os.path.join(os.environ['HOME'], 'myrepos/dbt-postgres-airflow/airflow/utilities'))
from helper_function import main

with DAG(
    dag_id="data_weather_fetch_daily",  # this ID shows up in Airflow UI
    start_date=pendulum.today(),  # start date for the DAG to become active
    schedule="42 18 * * *",  # run the DAG daily at 5:40PM
    default_args={
        'retries': 1,  # number of retries on failure   
        'retry_delay': pendulum.duration(minutes=5),  # delay between retries       
    },
) as dag:

    # Define a task to run the main function from helper_function.py
    fetch_weather_task = PythonOperator(
        task_id="ingest_weather_data",  # task ID
        python_callable=main
    )