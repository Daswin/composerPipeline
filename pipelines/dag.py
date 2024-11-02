from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.utils.dates import days_ago
from datetime import timedelta,datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 1),
    'depends_on_past': False,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    'employee_data',
    default_args = default_args,
    description = 'generated emp data, transform it then load into bigquery',
    schedule_interval = '@daily',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout = timedelta(minutes=5)
)

with dag:
    run_script_task = BashOperator(
        task_id = 'extract_data',
        bash_command = 'python /home/airflow/gcs/data/extract_data.py',
    )
                    
    start_pipeline = CloudDataFusionStartPipelineOperator(
        task_id="start_datafusion_pipeline",
        location="us-central1",
        pipeline_name="pipleline1-gcs-to-bigQ",
        instance_name="dataFusionInstance1",    
    )

    run_script_task >> start_pipeline