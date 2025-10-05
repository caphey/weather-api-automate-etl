from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json
import os

# Configuration par défaut du DAG
default_args = {
    'owner': 'data-analyst',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def create_raw_table():
    """Crée la table raw.weather si nécessaire"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw.weather (
        id SERIAL PRIMARY KEY,
        city VARCHAR(100) NOT NULL,
        country VARCHAR(100),
        temperature INTEGER,
        weather_description TEXT,
        humidity INTEGER,
        wind_speed INTEGER,
        wind_direction VARCHAR(10),
        pressure INTEGER,
        visibility INTEGER,
        uv_index INTEGER,
        observation_time TIMESTAMP,
        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        data_interval_start TIMESTAMP,
        raw_json JSONB
    );
    """
    
    postgres_hook.run(create_table_sql)
    print("Table raw.weather créée avec succès")

def extract_and_load_weather(**context):
    """Extrait les données Weatherstack et les charge dans PostgreSQL"""
    
    api_key = os.getenv('WEATHERSTACK_API_KEY')
    base_url = os.getenv('WEATHERSTACK_BASE_URL')
    
    if not api_key:
        raise ValueError("WEATHERSTACK_API_KEY non trouvée")
    
    # Hook PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Liste de villes à extraire
    cities = ['Paris', 'London', 'New York', 'Tokyo', 'Sydney', 'Berlin', 'Madrid']
    
    for city in cities:
        try:
            params = {
                'access_key': api_key,
                'query': city
            }
            response = requests.get(base_url, params=params)
            response.raise_for_status()

            data = response.json()
            
            # Vérification erreur API
            if 'error' in data:
                print(f"Erreur API pour {city}: {data['error']}")
                continue
            
            # Extraction des données importantes
            current = data.get('current', {})
            location = data.get('location', {})
            
            # Préparation de l'insert SQL
            insert_sql = """
            INSERT INTO raw.weather (
                city, country, temperature, weather_description, 
                humidity, wind_speed, wind_direction, pressure, 
                visibility, uv_index, observation_time, 
                data_interval_start, raw_json
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s
            )
            """
            
            insert_data = (
                location.get('name', city),
                location.get('country'),
                current.get('temperature'),
                current.get('weather_descriptions', [None])[0],
                current.get('humidity'),
                current.get('wind_speed'),
                current.get('wind_dir'),
                current.get('pressure'),
                current.get('visibility'),
                current.get('uv_index'),
                current.get('observation_time'),
                context['data_interval_start'],
                json.dumps(data)  # JSON complet
            )
            
            postgres_hook.run(insert_sql, parameters=insert_data)
            print(f"Données insérées pour {city}")
            
        except Exception as e:
            print(f"Erreur lors du traitement de {city}: {e}")


# Définition du DAG complet
with DAG(
    'weatherstack_full_pipeline',
    default_args=default_args,
    description='Pipeline ETL complet : Extract → Transform → Load → Marts',
    schedule_interval="@daily",
    catchup=False,
    tags=['weatherstack', 'etl', 'dbt', 'production'],
) as dag:
    
    # ÉTAPE 1: Préparation
    create_table_task = PythonOperator(
        task_id='create_raw_table',
        python_callable=create_raw_table,
    )
    
    # ÉTAPE 2: Extraction et chargement
    extract_load_task = PythonOperator(
        task_id='extract_and_load_weather',
        python_callable=extract_and_load_weather,
    )
    
    # ÉTAPE 3: Transformations dbt - Staging
    dbt_staging_task = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /usr/app/dbt && dbt run --select staging',
    )
    
    # ÉTAPE 4: Tests de qualité des données
    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='cd /usr/app/dbt && dbt test --select staging',
    )
    
    # ÉTAPE 5: Transformations dbt - Marts
    dbt_marts_task = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /usr/app/dbt && dbt run --select marts',
    )
    
    # ÉTAPE 6: Tests finaux
    dbt_test_marts_task = BashOperator(
        task_id='dbt_test_marts',
        bash_command='cd /usr/app/dbt && dbt test --select marts',
    )
    
    # ÉTAPE 7: Génération documentation (optionnel)
    dbt_docs_task = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /usr/app/dbt && dbt docs generate',
    )
    
    # Définition des dépendances du pipeline [249][252]
    create_table_task >> extract_load_task >> dbt_staging_task >> dbt_test_task >> dbt_marts_task >> dbt_test_marts_task >> dbt_docs_task