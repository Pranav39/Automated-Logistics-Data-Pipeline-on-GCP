# Automated-Logistics-Data-Pipeline-on-GCP

## Overview

This Airflow DAG automates the process of loading daily logistics data files from Google Cloud Storage (GCS) into Hive tables on a Google Cloud Dataproc cluster.

The DAG:

- Waits for new logistics data files in a GCS bucket using a sensor.
- Creates Hive database and tables if they don’t already exist.
- Loads data into an external Hive table.
- Creates a partitioned Hive table and populates it dynamically.
- Archives processed files to another GCS bucket.

---

## Tech Stack

- **Google Cloud Storage (GCS):** For storing raw and archived logistics data files.
- **Apache Airflow on Cloud Composer:** Orchestrates the workflow and scheduling.
- **Google Cloud Dataproc:** Managed Hadoop and Hive cluster for processing data.
- **Apache Hive:** Data warehousing on Hadoop used for SQL-like querying of data.
- **Python:** Used to author the Airflow DAG and orchestrate tasks.
- **HiveQL / SQL:** Query language used within Dataproc to create tables and load data.

---

## Project Use Case

This pipeline is designed to handle **daily logistics data** uploads. It runs once daily, waits for new files for up to 1 hour, and processes them if found. This approach balances timely processing with efficient resource usage.

---

## Components

- **GCSObjectsWithPrefixExistenceSensor**: Waits for files with a specified prefix in the GCS bucket.
- **DataprocSubmitJobOperator**: Submits Hive queries to a Dataproc cluster to create databases, tables, and load data.
- **BashOperator**: Archives processed files by moving them to an archive bucket in GCS.

---

## Architecure

+---------------------+       +----------------------+       +----------------------+
| Google Cloud Storage|       |    Cloud Composer    |       |   Dataproc Cluster   |
|  (Input Bucket)     | <---> |   (Airflow DAG)      | <---> | (Hive Jobs Executed) |
| logistics_raw_data  |       | - Sensor checks file |       |                      |
|                     |       | - Submit Hive jobs   |       |                      |
+---------------------+       | - Archive files      |       +----------------------+
                              +----------------------+
                                         |
                                         v
                            +----------------------------+
                            | Google Cloud Storage       |
                            | (Archive Bucket)           |
                            | logistics_archive_data     |
                            +----------------------------+

---

## DAG Structure

1. **sense_logistics_file**: Sensor checks for new files in the logistics raw data GCP bucket.
2. **create_hive_database**: Creates Hive database if it does not exist.
3. **create_hive_table**: Creates an external Hive table on raw data files.
4. **create_partitioned_table**: Creates a partitioned Hive table for optimized querying.
5. **set_hive_properties_and_load_partitioned**: Enables dynamic partitioning and loads data into a partitioned table.
6. **archive_processed_file**: Moves processed CSV files to the archive bucket.

---

## Workflow Diagram
+-----------------------------+
| Upload daily logistics files |
|     to GCS input bucket      |
+--------------+--------------+
               |
               v
+--------------+--------------+
|   GCSObjectsWithPrefix       |
|  ExistenceSensor (waits)     |
+--------------+--------------+
               |
               v
+--------------+--------------+
|  Dataproc Hive Job: Create   |
|    logistics_db database     |
+--------------+--------------+
               |
               v
+--------------+--------------+
|  Dataproc Hive Job: Create   |
| external logistics_data table|
+--------------+--------------+
               |
               v
+--------------+--------------+
| Dataproc Hive Job: Create    |
| logistics_data_partitioned   |
|    partitioned table         |
+--------------+--------------+
               |
               v
+--------------+--------------+
| Dataproc Hive Job: Load data |
|    into partitioned table    |
+--------------+--------------+
               |
               v
+--------------+--------------+
| BashOperator: Move processed |
|    files to archive bucket   |
+-----------------------------+

---

## Configuration

Edit the following variables in the DAG file to match your environment:

- `PROJECT_ID`: Your GCP project ID.
- `REGION`: Region of your Dataproc cluster.
- `CLUSTER_NAME`: Name of your Dataproc cluster.
- `bucket` names in the sensor and archive tasks.

---

## Requirements

- Apache Airflow with Google Cloud provider package installed.
- Google Cloud credentials configured for Airflow to access GCS and Dataproc.
- A running Google Cloud Dataproc cluster with Hive installed.
- Source data files uploaded daily to the configured GCS bucket.

---

## How to Use

1. Deploy the DAG to your Airflow environment.
2. Ensure data files are uploaded daily to the input GCS bucket.
3. Airflow scheduler will trigger the DAG daily.
4. The DAG will wait up to 1 hour for the files, then process and archive them.
5. Monitor DAG runs via the Airflow UI.

---

## Troubleshooting

- **Sensor task fails (timeout):** No new files arrived within the timeout period.
- **Dataproc job failures:** Check Dataproc cluster status and Hive query logs.
- **GCS bucket not found:** Verify bucket names and permissions.
  
---



