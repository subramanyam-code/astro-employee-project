from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime

import pandas as pd
import boto3
import io
import os


# -------------------------------------------------------
# ENVIRONMENT VARIABLES
# -------------------------------------------------------

S3_BUCKET = os.getenv("S3_BUCKET")

AWS_REGION = os.getenv("AWS_REGION")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")

AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE")


# -------------------------------------------------------
# S3 FILE PATHS
# -------------------------------------------------------

S3_KEY = "employees.json"

PARQUET_KEY = "output/employees.parquet"


# -------------------------------------------------------
# TASK 1
# READ JSON FROM S3
# -------------------------------------------------------

def read_json_from_s3(**context):

    s3_hook = S3Hook(
        aws_conn_id="aws_default"
    )

    obj = s3_hook.get_key(
        key=S3_KEY,
        bucket_name=S3_BUCKET
    )

    json_data = obj.get()["Body"].read().decode("utf-8")

    df = pd.read_json(
        io.StringIO(json_data)
    )

    print("JSON DATA")

    print(df)

    context['ti'].xcom_push(
        key='employee_data',
        value=df.to_json()
    )


# -------------------------------------------------------
# TASK 2
# CONVERT JSON TO PARQUET
# -------------------------------------------------------

def convert_to_parquet(**context):

    s3_hook = S3Hook(
        aws_conn_id="aws_default"
    )

    json_string = context['ti'].xcom_pull(
        key='employee_data',
        task_ids='read_json_task'
    )

    df = pd.read_json(
        io.StringIO(json_string)
    )

    parquet_buffer = io.BytesIO()

    df.to_parquet(
        parquet_buffer,
        engine='pyarrow',
        index=False
    )

    parquet_buffer.seek(0)

    s3_hook.load_bytes(
        bytes_data=parquet_buffer.getvalue(),
        key=PARQUET_KEY,
        bucket_name=S3_BUCKET,
        replace=True
    )

    print("PARQUET FILE UPLOADED TO S3")


# -------------------------------------------------------
# TASK 3
# GET TOP 3 EMPLOYEES
# -------------------------------------------------------

def top_3_salary(**context):

    json_string = context['ti'].xcom_pull(
        key='employee_data',
        task_ids='read_json_task'
    )

    df = pd.read_json(
        io.StringIO(json_string)
    )

    top3 = df.sort_values(
        by='salary',
        ascending=False
    ).head(3)

    print("TOP 3 EMPLOYEES")

    print(top3)

    context['ti'].xcom_push(
        key='top3_data',
        value=top3.to_json()
    )


# -------------------------------------------------------
# TASK 4
# LOAD INTO DYNAMODB
# -------------------------------------------------------

def load_into_dynamodb(**context):

    json_string = context['ti'].xcom_pull(
        key='top3_data',
        task_ids='top_3_task'
    )

    df = pd.read_json(
        io.StringIO(json_string)
    )

    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    table = dynamodb.Table(
        DYNAMODB_TABLE
    )

    for _, row in df.iterrows():

        table.put_item(
            Item={
                'emp_id': int(row['emp_id']),
                'name': str(row['name']),
                'department': str(row['department']),
                'salary': int(row['salary'])
            }
        )

    print("DATA INSERTED INTO DYNAMODB")


# -------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------

with DAG(

    dag_id='employee_s3_dynamodb_pipeline',

    start_date=datetime(2025, 1, 1),

    schedule='@daily',

    catchup=False,

    tags=['s3', 'dynamodb', 'parquet', 'astro']

) as dag:


    # TASK 1
    read_json_task = PythonOperator(

        task_id='read_json_task',

        python_callable=read_json_from_s3

    )


    # TASK 2
    parquet_task = PythonOperator(

        task_id='parquet_task',

        python_callable=convert_to_parquet

    )


    # TASK 3
    top_3_task = PythonOperator(

        task_id='top_3_task',

        python_callable=top_3_salary

    )


    # TASK 4
    dynamodb_task = PythonOperator(

        task_id='dynamodb_task',

        python_callable=load_into_dynamodb

    )


    # DAG FLOW

    read_json_task >> parquet_task

    read_json_task >> top_3_task

    top_3_task >> dynamodb_task