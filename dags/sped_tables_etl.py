from airflow import DAG
from airflow.decorators import task
from datetime import datetime

from sped_tables_etl_process.extract import SpedTablesCrawler
from pathlib import Path
import psycopg2
import uuid


with DAG(
    'sped_tables_etl', 
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
    }, 
    schedule_interval=None, 
    catchup=False
) as dag:
    
    @task()
    def extract_tables():
        crawler = SpedTablesCrawler()
        crawler.extract()

    @task()
    def transform_and_load_tables():
        lake = Path('/tmp/datalake')

        try:
            # create connection
            with psycopg2.connect(
                host="localhost",
                database="your_database",
                user="your_username",
                password="your_password"
            ) as conn:
                
                # create cursor
                with conn.cursor() as cursor:
                    for file_root in lake.iterdir():
                        # save reference into 
                        package, table = file_root.name.removesuffix('.txt').split('___')
                        
                        # this part can be replace for api 
                        cursor.execute("SELECT id FROM sped.tax_package WHERE pacote = %s AND table = %s", (package, table))
                        id_package = cursor.fetchone()[0]
                        if (not id_package):
                            id_package = str(uuid.uuid4())
                            cursor.execute(
                                "INSERT INTO sped.tax_package (id, pacote, tabela) VALUES (%s, %s, %s)", 
                                (str(id_package), package, table)
                            )
                        else:
                            cursor.execute(
                                "INSERT INTO sped.tax_package (id, pacote, tabela) VALUES (%s, %s, %s)", 
                                (str(id_package), package, table)
                            )

                        # save reference into sped_fiscal_dados_tabelas
                        with file_root.open('r') as f:
                            for line in f.readlines():
                                code, description, start_date, end_date = line.split('|')
                                
                                cursor.execute("SELECT * FROM sped.tax_tables WHERE code = %s", (code))
                                if (not cursor.fetchone()):
                                    cursor.execute(
                                        "INSERT INTO sped.tax_tables (id, code, description, start_date, end_date, tax_package) VALUES (%s, %s, %s, %s, %s)", 
                                        (str(uuid.uuid4()), code, description, start_date, end_date, id_package)
                                    )
                        
                        file_root.unlink()
        
        except psycopg2.DatabaseError as error:
            print(f"Error: {error}")
        
    extract_tables() >> transform_and_load_tables()
