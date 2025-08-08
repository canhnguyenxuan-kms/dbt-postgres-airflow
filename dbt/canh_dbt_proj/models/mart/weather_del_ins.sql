{{
    config(
        materialized='incremental',
        unique_key='id',
        pre_hook='{{ delete_insert() }}'
    )   
}}

with sql_query as (
    select *
    from {{ ref('stg_weather_report') }}
),
de_dup as (
    select
        *,
        row_number() over (partition by id order by inserted_date desc) as rn
    from sql_query
)
select
    id,
    city,
    temperature,
    weather_description,
    wind_speed,
    weather_time_local,
    inserted_date
from de_dup
where rn = 1