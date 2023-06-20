{{ 
    config(
        materialized='incremental',
        indexes=[
            {'columns': ['forecast_history_key'], 'type': 'hash'}
        ]
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['grid_forecast_key', 'updated']) }} as forecast_history_key,
    grid_forecast_key,
    gridpoints_key,
    number as forecast_number,
    start_time,
    end_time,
    is_daytime,
    temperature,
    probability_of_precipitation,
    dewpoint,
    relative_humidity,
    wind_speed,
    wind_direction,
    short_forecast,
    updated as source_update_date,
    generated_at as source_generation_date,
    current_timestamp as inserted_date,
    'ETL' as inserted_user,
    null as last_updated_date,
    null as last_updated_user
from
    {{ ref('stg_forecast') }}

{% if is_incremental() %}

where updated > (select max(inserted_date) from {{ this }} )

{% endif %}
