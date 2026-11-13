from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys, os

sys.path.insert(0, '/opt/airflow/src')

from extract_data import extract_weather_data
from load_data import load_weather_data, truncate_table_data
from transform_data import data_transformations


from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)


API_KEY = os.getenv('API_KEY')
url = f'http://api.weatherapi.com/v1/current.json?key={API_KEY}=Sao+Paulo&aqi=yes'


@dag(
    dag_id='youtube_weather_pipeline',
    default_args = {
        'owner':'airflow',
        'dependes_on_past':False,
        'retries': 2,
        'retry_delay':timedelta(seconds=5)
    },
    description='Pipeline ETL - Clima SP',
    schedule='0 */1 * * *',
    start_date=datetime(2026,11,13),
    catchup=False,
    tags={'weather','youtube','dpt'}
)

def weather_pipeline():

    @task
    def extract():
        extract_weather_data(url)

    @task
    def transform():
        df= data_transformations()

        if not df.empty:
            logging.warn("Nenhum dado retornado")

        df.to_parquet('/opt/airflow/data/temp_data.parquet', index=False)

        return logging.info('Arquivo criado com sucesso')

    @task
    def load():
        import pandas as pd
        df = pd.read_parquet('/opt/airflow/data/temp_data.parquet')

        truncate_table_data()
        load_weather_data('sp_weather', df)

    extract() >> transform() >> load()

weather_pipeline()