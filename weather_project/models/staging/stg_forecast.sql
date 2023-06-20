{{
    config(
        materialized='view'
    )
}}

with source as (
    select 
        *
    from 
        {{ source('weather_ingestion', 'grid_forecast') }}
)
select *
from source