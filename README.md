# Random User API ETL Pipeline

This project is developed as part of an assignment for the Big Data and Analytics course. It demonstrates an ETL (Extract, Transform, Load) pipeline that fetches data from the [Random User API](https://randomuser.me/), processes it using PySpark, and stores it in a PostgreSQL database. The pipeline is orchestrated using Apache Airflow.

## Table of Contents
- [Overview](#overview)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)

## Overview

The goal of this project is to automate the process of fetching and transforming data from the Random User API. The pipeline:
1. **Fetches** data from the API (between 1000 and 2000 users).
2. **Processes** and flattens the user data into a structured format using PySpark.
3. **Saves** the processed data into a PostgreSQL database along with metadata such as the number of partitions and the row count.
4. Runs every 5 minutes using Apache Airflow to simulate batch processing.

## Technologies Used

- **Apache Airflow**: Orchestrates the ETL workflow.
- **PySpark**: Processes the data fetched from the API.
- **PostgreSQL**: Stores the processed user data.
- **Python**: The entire pipeline is written in Python.

## Project Structure

```plaintext
├── dags/random_user
│   ├── random_user_dag.py       # Airflow DAG for orchestrating the ETL pipeline
│   ├── random_user_etl.py       # ETL script that fetches, processes, and saves data
|   ├── sql/
├── sql/
│   ├── create_tables.sql        # SQL script to create necessary tables in PostgreSQL
├── README.md                    # Project documentation
