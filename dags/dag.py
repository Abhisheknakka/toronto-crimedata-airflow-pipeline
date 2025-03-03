from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
from io import StringIO
from datetime import timedelta
from datetime import datetime

# Define the base URL for the API
base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

# Define your PostgreSQL connection id for Airflow
PG_CONN_ID = 'toronto_data'

# Define the name of your DAG
dag_name = "toronto_open_data_pipeline"

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 2),  # Set the start date
}

# Define the DAG
dag = DAG(
    dag_name,
    default_args=default_args,
    schedule_interval=None,  # Adjust to your preferred schedule
    catchup=False
)

# Task 1: Extract Data from the API
def extract_data():
    url = base_url + "/api/3/action/package_show"
    params = {"id": "neighbourhood-crime-rates"}
    package = requests.get(url, params=params).json()
    
    # Extract data from datastore_active resources
    data = []
    for idx, resource in enumerate(package["result"]["resources"]):
        if resource["datastore_active"]:
            resource_url = base_url + "/datastore/dump/" + resource["id"]
            resource_dump_data = requests.get(resource_url).text
            data.append(resource_dump_data)
            
    return data

# Task 2: Clean the Data
def clean_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    cleaned_data = []
    
    for raw_data in data:
        if isinstance(raw_data, str):  # CSV data
            df = pd.read_csv(StringIO(raw_data))
        else:  # If it's a JSON response
            df = pd.json_normalize(raw_data)
        
        # Check if AREA_NAME column exists
        print(f"Columns in cleaned dataframe: {df.columns}")
        
        # Clean the data: select required columns and drop NA or duplicates
        df_cleaned = df[[
            '_id', 'AREA_NAME', 'HOOD_ID', 
            'ASSAULT_2024', 'AUTOTHEFT_2024', 'BIKETHEFT_2024', 'BREAKENTER_2024', 
            'HOMICIDE_2024', 'ROBBERY_2024', 'SHOOTING_2024', 'THEFTFROMMV_2024', 
            'THEFTOVER_2024', 'POPULATION_2024'
        ]]
        
        # Drop NaN and duplicates
        df_cleaned = df_cleaned.dropna()
        df_cleaned = df_cleaned.drop_duplicates()
        
        cleaned_data.append(df_cleaned)
    
    return cleaned_data


# Task 3: Create Table in PostgreSQL
def create_table():
    # Define the SQL query to create the table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS toronto_crime_data (
    "_id" INT PRIMARY KEY,
    "AREA_NAME" VARCHAR(255),
    "HOOD_ID" INT,
    "ASSAULT_2024" INT,
    "AUTOTHEFT_2024" INT,
    "BIKETHEFT_2024" INT,
    "BREAKENTER_2024" INT,
    "HOMICIDE_2024" INT,
    "ROBBERY_2024" INT,
    "SHOOTING_2024" INT,
    "THEFTFROMMV_2024" INT,
    "THEFTOVER_2024" INT,
    "POPULATION_2024" INT
);

    """
    
    # Get the PostgreSQL connection
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    hook.run(create_table_query)

# Task 4: Store Data in PostgreSQL
def store_data(**kwargs):
    cleaned_data = kwargs['ti'].xcom_pull(task_ids='clean_data')
    
    # Get the PostgreSQL connection
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    
    # Define the table name
    table_name = "toronto_crime_data"
    
    # Loop over the cleaned data and insert it into PostgreSQL
    for df in cleaned_data:
        for index, row in df.iterrows():
            insert_query = """
                INSERT INTO toronto_crime_data ("_id", "AREA_NAME", "HOOD_ID", "ASSAULT_2024", "AUTOTHEFT_2024", 
                    "BIKETHEFT_2024", "BREAKENTER_2024", "HOMICIDE_2024", "ROBBERY_2024", "SHOOTING_2024", 
                    "THEFTFROMMV_2024", "THEFTOVER_2024", "POPULATION_2024") 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("_id") DO NOTHING;
            """.format(table=table_name)  # Customize the query based on your columns
            
            # Insert the data row into the PostgreSQL table
            hook.run(insert_query, parameters=tuple(row))

# Define the DAG tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

store_task = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> clean_task >> create_table_task >> store_task
