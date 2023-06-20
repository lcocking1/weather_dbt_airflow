from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow import DAG
import pendulum

# @dag(
#     dag_id="dag_execute_dbt",
#     description="execute dbt pipeline",
#     start_date=pendulum.today(),
#     schedule="30 * * * *"
# )
# def etl():
#     @task
#     def pull_git():
#         BashOperator(
#             task_id="sync with git repo",
#             cwd="/home/lcocking/airflow/weather_project",
#             bash_command="git pull etl-repo master"
#         )
#     @task
#     def execute_dbt():
#         BashOperator(
#             task_id="run_dbt_pipeline",
#             cwd="/home/lcocking/airflow/weather_project",
#             bash_command="dbt deps && dbt run"
#         )
    
#     pull_git()
#     execute_dbt()

# etl()

# execute_dbt()

with DAG(
    dag_id="dag_execute_dbt",
    description="execute dbt pipeline",
    start_date=pendulum.today(),
    schedule="30 * * * *"
) as dag:
    sync_repo = BashOperator(
        task_id="sync_with_git_repo",
        cwd="/weather_project",
        bash_command="git pull etl-repo master"
    )
    run_pipeline = BashOperator(
        task_id="run_dbt_pipeline",
        cwd="/weather_project",
        bash_command="dbt deps && dbt run"
    )

sync_repo >> run_pipeline