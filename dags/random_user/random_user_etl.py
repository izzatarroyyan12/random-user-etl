import requests
import random
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import psycopg2
import logging
from datetime import datetime

# Set up Airflow logger
logger = logging.getLogger("airflow.task")

# Step 1: Fetch Data from API
def fetch_random_users():
    num_users = random.randint(1000, 2000)
    url = f"https://randomuser.me/api/?results={num_users}"
    try:
        logger.info(f"Fetching {num_users} random users from API.")
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            fetch_time = datetime.now()  # Capture fetch time
            logger.info(f"Successfully fetched {len(data['results'])} users.")
            return data['results'], fetch_time  # Return both users and fetch time
        else:
            logger.error(f"API call failed with status code {response.status_code}")
            raise Exception("API call failed")
    except Exception as e:
        logger.error(f"Error during API fetch: {e}")
        raise

# Step 2: Process and Flatten Data
def process_data(users, fetch_time):
    try:
        logger.info("Processing and flattening data.")
        spark = SparkSession.builder.appName("RandomUserETL").getOrCreate()

        # Load data into a Spark DataFrame
        df = spark.read.json(spark.sparkContext.parallelize(users))

        # Flatten the DataFrame
        df_flattened = df.select(
            col("login.uuid").alias("uuid"),
            col("gender"),
            col("name.first").alias("first_name"),
            col("name.last").alias("last_name"),
            col("email"),
            col("dob.age").alias("age"),
            col("dob.date").alias("dob_date"),
            col("location.city").alias("city"),
            col("location.state").alias("state"),
            col("location.country").alias("country"),
            col("login.username").alias("username"),
            col("login.password").alias("password")
        )

        # Add fetch_time as a new column to the user data
        df_with_time = df_flattened.withColumn("fetch_time", lit(fetch_time))

        # Get number of partitions and row count
        num_partitions = df_with_time.rdd.getNumPartitions()
        row_count = df_with_time.count()

        # Convert to Pandas DataFrame and then to JSON string for saving to PostgreSQL
        pandas_df = df_with_time.toPandas()
        json_data = pandas_df.to_json(orient="records")

        # Stop Spark session after processing
        spark.stop()
        
        logger.info(f"Processing complete with {num_partitions} partitions and {row_count} rows.")
        return json_data, fetch_time, num_partitions, row_count  # Return necessary info for both tables
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

# Step 3: Save DataFrame to PostgreSQL
def save_to_db(json_str, fetch_time, num_partitions, row_count):
    try:
        logger.info("Saving data to PostgreSQL.")

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="user_db",
            user="postgres",
            password="postgres",
            host="localhost"
        )
        cursor = conn.cursor()

        # Save user data to the random_users table
        df = pd.read_json(json_str)  # Convert JSON string back to Pandas DataFrame
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO random_users (uuid, gender, first_name, last_name, email, age, dob_date, city, state, country, username, password, fetch_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                row['uuid'], row['gender'], row['first_name'], row['last_name'], row['email'],
                row['age'], row['dob_date'], row['city'], row['state'],
                row['country'], row['username'], row['password'], fetch_time  # Fetch time is included
            ))

        # Save partition info to the partitions_info table
        cursor.execute("""
            INSERT INTO partitions_info (fetch_time, num_partitions, row_count)
            VALUES (%s, %s, %s);
        """, (fetch_time, num_partitions, row_count))

        # Commit changes and close the connection
        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Data successfully saved to PostgreSQL.")
    except Exception as e:
        logger.error(f"Error saving data to PostgreSQL: {e}")
        raise
