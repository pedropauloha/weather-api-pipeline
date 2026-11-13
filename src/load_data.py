from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

# from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

POSTGRES_CONN_ID = 'pg_pedroprojects'
TB_WEATHER = 'sp_weather'

user = os.getenv('CONN_USER')
password = os.getenv('CONN_PASSWORD')
database = os.getenv('CONN_DATABASE')
host = os.getenv('CONN_HOSTNAME')
port = os.getenv('CONN_PORT')

def truncate_table_data() -> None:
            
    hook = PostgresHook(
        postgres_conn_id = POSTGRES_CONN_ID
    )
    print(f'Cleaning weather table...')
    hook.run(f'TRUNCATE TABLE {TB_WEATHER};')


def get_engine():
    hook = PostgresHook(
        postgres_conn_id = POSTGRES_CONN_ID
    )
    engine = hook.get_sqlalchemy_engine()
    return engine
    # return create_engine(
    #     f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:{port}/{database}"
    # )
engine = get_engine()

def load_weather_data(table_name:str, df):
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False
    )
    
    logging.info(f"✅ Dados carregados com sucesso!\n") 
    
    df_check = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)
    logging.info(f"Total de registros na tabela: {len(df_check)}\n")