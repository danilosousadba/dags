from airflow import DAG
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from scripts import local_to_s3,remove_local_file
import json
import pandas as pd

path = Variable.get("path_randomuser")
compression = Variable.get("compression_randomuser")
filename = Variable.get("filename_randomuser")
bucket_name = Variable.get("bucket_name_randomuser")


default_args = {
    'owner' : 'DaniloSousa',
    'depends_on_past' : False,
    'retries' : 1
}

def processing_user(ti,path,ds, filename, compression):
    users = ti.xcom_pull(task_ids=['get_users'])
    pd.set_option('display.max_columns', None)
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    df = pd.json_normalize(users[0]['results'])
    df.info()
    #schema = build_table_schema(df)
    df2 = pd.DataFrame(df,dtype=str)
    df2.info()
    print("df2.head: ) ",df2.head())
    df2.to_parquet(path=path+filename,compression=compression,index=False)


with DAG('randomuser', schedule_interval='@daily', default_args=default_args, catchup=False, start_date=datetime.now()) as dag:
    api_available = HttpSensor(
        task_id = 'api_availble',
        http_conn_id = 'randomuser_api',
        endpoint = '/api'
    )
    get_users = SimpleHttpOperator(
        task_id = 'get_users',
        http_conn_id = 'randomuser_api',
        endpoint= '/api/?results=10',
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response=True
    )
    processing_user = PythonOperator(
        task_id = 'processing_user',
        python_callable=processing_user,
        op_kwargs={'path': path, 'filename': filename, 'compression': compression}
    )
    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=local_to_s3,
        op_kwargs={
            'bucket_name': bucket_name,
            'dir_target': 'randomuser/',
            'filepath': './data/randomuser/*.parquet'
        },
    )
    remove_df_local = PythonOperator(
         task_id='remove_df_local',
         python_callable=remove_local_file,
         op_kwargs={
            'filepath': './data/randomuser/*.parquet'
         }
     )
api_available >> get_users >> processing_user >> upload_to_s3 >> remove_df_local
