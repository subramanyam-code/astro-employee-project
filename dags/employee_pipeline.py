"""
Employee Pipeline DAG for Astronomer Airflow Project
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'employee_pipeline',
    default_args=default_args,
    description='Employee data processing pipeline',
    schedule_interval='@daily',
    catchup=False,
)

# Define your DAG tasks here
def process_employee_data():
    print("Processing employee data...")


start_task = BashOperator(
    task_id='start',
    bash_command='echo "Starting employee pipeline"',
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_employees',
    python_callable=process_employee_data,
    dag=dag,
)

end_task = BashOperator(
    task_id='end',
    bash_command='echo "Employee pipeline completed"',
    dag=dag,
)

start_task >> process_task >> end_task
