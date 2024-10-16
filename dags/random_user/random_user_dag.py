from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from random_user import random_user_etl
import logging

# Set up logging for Airflow
logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Random_user_api_dag',
    default_args=default_args,
    description='DAG to fetch random users, process and save to SQL',
    schedule_interval=timedelta(minutes=5),
    catchup=False
)

def fetch_users(**kwargs):
    try:
        users, fetch_time = random_user_etl.fetch_random_users()
        logger.info(f"Fetched {len(users)} users from the API.")
        return users, fetch_time  # Return both users and fetch time
    except Exception as e:
        logger.error(f"Error fetching users: {e}")
        raise

def process_users(**kwargs):
    ti = kwargs['ti']
    users, fetch_time = ti.xcom_pull(task_ids='fetch_users')  # Pull users and fetch time from XCom
    logger.info(f"Processing {len(users)} users.")

    try:
        json_data, fetch_time, num_partitions, row_count = random_user_etl.process_data(users, fetch_time)  # Process and get JSON, partitions, and row count
        logger.info("Data processing complete.")
        return json_data, fetch_time, num_partitions, row_count  # Return additional information
    except Exception as e:
        logger.error(f"Error processing users: {e}")
        raise

def save_users_to_db(**kwargs):
    ti = kwargs['ti']
    json_str, fetch_time, num_partitions, row_count = ti.xcom_pull(task_ids='process_users')  # Pull JSON, fetch_time, partitions, row_count from XCom
    try:
        random_user_etl.save_to_db(json_str, fetch_time, num_partitions, row_count)  # Save to database
        logger.info("Users successfully saved to database.")
    except Exception as e:
        logger.error(f"Error saving users to database: {e}")
        raise

# Define tasks
fetch_task = PythonOperator(
    task_id='fetch_users',
    python_callable=fetch_users,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_users',
    python_callable=process_users,
    dag=dag
)

save_task = PythonOperator(
    task_id='save_users_to_db',
    python_callable=save_users_to_db,
    dag=dag
)

# Set task dependencies
fetch_task >> process_task >> save_task
