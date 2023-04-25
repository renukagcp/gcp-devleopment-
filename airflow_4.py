import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta



default_args = {
    'start_date' : airflow.utils.dates.days_ago(0),
    'depends_on_past': False,
    'email': ['mnirvinkumar@gmail.com,vbhanu9999@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
    }
    
    
dag = DAG (
        'airflow',
        default_args = default_args,
        description = 'hello')
      
task1 = BashOperator(
    task_id = 'load_to_stage',
    bash_command = 'gsutil mv gs://gcp_airflow/orders.xml gs://gcp_bhanu089 ',
    dag = dag)

task1

