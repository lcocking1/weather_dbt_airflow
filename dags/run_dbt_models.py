from airflow.operators.bash import BashOperator
import os
import json

def run_models():
    path = os.environ["PWD"] # retrieve the location of your current folder == ~/airflow/dags/
    dbt_path = os.path.join(path, "weather_project") # path to your dbt project
    manifest_path = os.path.join(dbt_path, "target/manifest.json") # path to manifest.json

    with open(manifest_path) as f: # Open manifest.json
        manifest = json.load(f) # Load its contents into a Python Dictionary
        nodes = manifest["nodes"] # Extract just the nodes
    # Create a dict of Operators
    dbt_tasks = dict()
    for node_id, node_info in nodes.items():
        dbt_tasks[node_id] = BashOperator(
            task_id=".".join(
                [
                    node_info["resource_type"],
                    node_info["package_name"],
                    node_info["name"],
                ]
            ),
            bash_command=f"cd {dbt_path} && dbt run --models {node_info['name']}", # run the model!
        )
    # Define relationships between Operators
    for node_id, node_info in nodes.items():
        upstream_nodes = node_info["depends_on"]["nodes"]
        if upstream_nodes:
            for upstream_node in upstream_nodes:
                if "source" not in upstream_node:
                    dbt_tasks[upstream_node] >> dbt_tasks[node_id]