import pendulum
# from airflow import DAG
# from run_dbt_models import run_models
from fetch_data import fetch_data
from airflow.decorators import dag, task 
from airflow.operators.bash import BashOperator
# Build an Airflow DAG
@dag(
    dag_id="dbt_master_pipeline", # The name that shows up in the UI
    start_date=pendulum.today(), # Start date of the DAG
    catchup=False,
    schedule="0 * * * *"
)
def master_pipeline():
    @task
    def fetch_weather_data():
        return fetch_data()
    
    build_dbt = BashOperator(
        task_id="stg_forecast",
        cwd="weather_project"
        bash_command="dbt deps && dbt build"
    )

    fetch_weather_data() >> build_dbt

master_pipeline()