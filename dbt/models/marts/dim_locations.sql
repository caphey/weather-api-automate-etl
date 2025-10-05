{{ config(
    materialized='table',
    schema='marts'
) }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['city_clean', 'country_clean']) }} as location_key,
    city_clean as city,
    country_clean as country,
    MIN(extracted_at) as first_observation_date,
    MAX(extracted_at) as last_observation_date,
    COUNT(*) as total_observations

FROM {{ ref('stg_weather') }}
GROUP BY city_clean, country_clean
