from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

# Replace these constants with your own values before deploying
PROJECT_ID = 'your-gcp-project-id'
REGION = 'your-gcp-region'  # e.g. 'asia-south1'
CLUSTER_NAME = 'your-dataproc-cluster-name'
INPUT_BUCKET = 'your-gcs-input-bucket'
ARCHIVE_BUCKET = 'your-gcs-archive-bucket'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Don't wait for previous runs to finish
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # No retries on failure
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'hive_load_airflow_dag',
    default_args=default_args,
    description='Load logistics data into Hive on GCP Dataproc',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=days_ago(1),
    tags=['example'],
)

# Sensor waits for files with prefix 'input_data/logistics_' in GCS input bucket
sense_logistics_file = GCSObjectsWithPrefixExistenceSensor(
    task_id='sense_logistics_file',
    bucket=INPUT_BUCKET,
    prefix='input_data/logistics_',
    mode='poke',           # Active wait during poke interval
    timeout=3600,          # Timeout after 1 hour if no files found
    poke_interval=30,      # Check every 30 seconds
    dag=dag,
)

# Create Hive database if it doesn't exist
create_hive_database = DataprocSubmitJobOperator(
    task_id="create_hive_database",
    job={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "hive_job": {
            "query_list": {"queries": ["CREATE DATABASE IF NOT EXISTS logistics_db;"]}
        }
    },
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Create external Hive table pointing to raw data in GCS
create_hive_table = DataprocSubmitJobOperator(
    task_id="create_hive_table",
    job={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "hive_job": {
            "query_list": {
                "queries": [f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS logistics_db.logistics_data (
                        delivery_id INT,
                        `date` STRING,
                        origin STRING,
                        destination STRING,
                        vehicle_type STRING,
                        delivery_status STRING,
                        delivery_time STRING
                    )
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION 'gs://{INPUT_BUCKET}/input_data/'
                    tblproperties('skip.header.line.count'='1');
                """]
            }
        }
    },
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Create partitioned Hive table
create_partitioned_table = DataprocSubmitJobOperator(
    task_id="create_partitioned_table",
    job={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "hive_job": {
            "query_list": {
                "queries": ["""
                    CREATE TABLE IF NOT EXISTS logistics_db.logistics_data_partitioned (
                        delivery_id INT,
                        origin STRING,
                        destination STRING,
                        vehicle_type STRING,
                        delivery_status STRING,
                        delivery_time STRING
                    )
                    PARTITIONED BY (`date` STRING)
                    STORED AS TEXTFILE;
                """]
            }
        }
    },
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Set Hive properties and load data into partitioned table
set_hive_properties_and_load_partitioned = DataprocSubmitJobOperator(
    task_id="set_hive_properties_and_load_partitioned",
    job={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "hive_job": {
            "query_list": {
                "queries": ["""
                    SET hive.exec.dynamic.partition = true;
                    SET hive.exec.dynamic.partition.mode = nonstrict;

                    INSERT INTO logistics_db.logistics_data_partitioned PARTITION(`date`)
                    SELECT delivery_id, origin, destination, vehicle_type, delivery_status, delivery_time, `date`
                    FROM logistics_db.logistics_data;
                """]
            }
        }
    },
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Move processed files from input bucket to archive bucket
archive_processed_file = BashOperator(
    task_id='archive_processed_file',
    bash_command=f"gsutil -m mv gs://{INPUT_BUCKET}/input_data/logistics_*.csv gs://{ARCHIVE_BUCKET}/",
    dag=dag,
)

# Define task dependencies/order
sense_logistics_file >> create_hive_database >> create_hive_table >> create_partitioned_table >> set_hive_properties_and_load_partitioned >> archive_processed_file
