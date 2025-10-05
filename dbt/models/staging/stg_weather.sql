{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT 
    id,
    TRIM(UPPER(city)) as city_clean,
    TRIM(UPPER(country)) as country_clean,
    temperature,
    CASE 
        WHEN weather_description ILIKE '%sunny%' THEN 'Clear'
        WHEN weather_description ILIKE '%rain%' THEN 'Rain'
        WHEN weather_description ILIKE '%cloud%' THEN 'Cloudy'
        ELSE TRIM(weather_description)
    END as weather_category,
    humidity,
    wind_speed,
    wind_direction,
    pressure,
    visibility,
    uv_index,
    observation_time,
    extracted_at,
    data_interval_start,
 
    CASE 
        WHEN temperature < 0 THEN 'Freezing'
        WHEN temperature BETWEEN 0 AND 10 THEN 'Cold'
        WHEN temperature BETWEEN 11 AND 20 THEN 'Mild' 
        WHEN temperature BETWEEN 21 AND 30 THEN 'Warm'
        ELSE 'Hot'
    END as temperature_category,
    
    DATE(extracted_at) as extraction_date

FROM {{ source('raw_data', 'weather') }}

-- Filtrage des données aberrantes
WHERE temperature IS NOT NULL 
  AND temperature BETWEEN -50 AND 60  -- Températures réalistes
  AND city IS NOT NULL
