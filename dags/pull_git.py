import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Build an Airflow DAG
with DAG(
    dag_id="pull_git_repo", # The name that shows up in the UI
    start_date=pendulum.today(), # Start date of the DAG
    catchup=False,
    schedule="0 * * * *"
    ) as dag:
    pull_git = BashOperator(
        task_id="pull_git",
        bash_command="git pull origin master"
    )
    compile_dbt = BashOperator(
        task_id="compile_dbt",
        bash_command="dbt deps && dbt compile"
    )

    pull_git >> compile_dbt

if __name__ == "__main__":
    # dag.cli()
    dag.test()
