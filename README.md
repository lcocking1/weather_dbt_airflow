Sample project to pull weather data from api.weather.gov and load/transform the data with dbt-core and apache-airflow.

dbt_master_pipeline is the running configuration. Running this on a small VM to avoid costs. 

Required packages are in requirements.txt (will not work with pip install -r requirements.txt because of airflow configuration). 

airflow dag graph:

![image](https://github.com/lcocking1/weather_dbt_airflow/assets/106569625/2ca351c3-4926-4bff-8ada-d61245b7686f)

dbt docs graph:

![image](https://github.com/lcocking1/weather_dbt_airflow/assets/106569625/90829417-a46e-4d4b-9f6c-5f8d474150ca)

postgresql logging data:

![image](https://github.com/lcocking1/weather_dbt_airflow/assets/106569625/27887692-34e9-4c56-9397-1e496a48735f)

Schedule history of successful runs:

![image](https://github.com/lcocking1/weather_dbt_airflow/assets/106569625/5c517741-0f25-45e3-b017-fb2c81c9825b)
