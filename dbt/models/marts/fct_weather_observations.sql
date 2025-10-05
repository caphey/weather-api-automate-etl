{{ config(
    materialized='table',
    schema='marts'
) }}

SELECT 
    -- Clés
    id as observation_id,
    {{ dbt_utils.generate_surrogate_key(['city_clean', 'country_clean']) }} as location_key,
    
    -- Mesures météo
    temperature,
    temperature_category,
    weather_category,
    humidity,
    wind_speed,
    pressure,
    
    -- Dimensions temporelles
    extraction_date,
    DATE_PART('hour', extracted_at) as extraction_hour,
    DATE_PART('dow', extracted_at) as day_of_week, -- 0=Dimanche, 6=Samedi
    
    -- Metadata
    extracted_at,
    data_interval_start

FROM {{ ref('stg_weather') }}
