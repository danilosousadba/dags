from airflow import DAG
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

import requests
import json
import pandas as pd
from pandas import json_normalize

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

def processing_user(ti):
    users = ti.xcom_pull(task_ids=['get_users'])
    print(users)
    print(type(users))
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    user = users[0]['results'][0]
    processed_user = json_normalize({
         'firstname': user['name']['first'],
         'lastname': user['name']['last'],
         'country': user['location']['country']
     })
    processed_user.to_parquet(path='/opt/airflow/dados/raw/users/users.parquet', compression='snappy', index=False)
    print(processed_user.count())

with DAG('random_user', schedule_interval='@daily', default_args=default_args, catchup=False,start_date=datetime(2021, 4, 12)) as dag:
    task_mkdir = BashOperator(
        task_id='mkdir',
        bash_command='mkdir -p /opt/airflow/dados/raw/users /opt/airflow/dados/trusted/users /opt/airflow/dados/refined/users'
    )
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_random_api',
        endpoint='/api'
    )
    get_users = SimpleHttpOperator(
        task_id='get_users',
        http_conn_id='user_random_api',
        endpoint='/api/?results=1',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    processing_user = PythonOperator(
        task_id='processing_user',
        python_callable=processing_user,
    )

    task_mkdir >> is_api_available >> get_users >> processing_user
