with source as (
    select *
    from {{ source('weather', 'weather_report') }}
),
renamed as (
    select
        id,
        city,
        temperature,
        weather_description,
        wind_speed,
        time as weather_time_local,
        inserted_date
    from source
)   
select *
from renamed