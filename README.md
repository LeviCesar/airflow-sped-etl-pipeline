# Airflow SPED ELT pipeline
![Static Badge](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge)
![Static Badge](https://img.shields.io/badge/Requests-2.31-blue?style=for-the-badge)
![Static Badge](https://img.shields.io/badge/beautifulsoup4-4.12.2-blue?style=for-the-badge)

## About

This project implements a ETL pipeline using **Apache Airflow**. The objective is extract data from SPED tables and trasform infos to load on database.

## How to execute project with docker

1. Clone the repository
    ```bash
    git clone https://github.com/LeviCesar/airflow-sped-etl-pipeline.git
    cd airflow-sped-etl-pipeline
    ```

2. Run docker comands
    ```bash
    docker compose up airflow-init -d
    docker compose up -d
    ```

3. Open Airflow on browser: 
    - http://localhost:8080
    - user: airflow
    - passwd: airflow
