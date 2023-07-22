# This Airflow setup file is just the configuration file, all the transformations will be happening in the ETL.py script
# Created a directory 'airflow' --> and set it as environmental variable --> AIRFLOW_HOME
# pip install apache-airflow
# initialize database -->  airflow db init

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ETL import *
from airflow import utils

def etl():
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    transformed_df = transform_avg_ratings(movie_df=movies_df, users_df=users_df)
    load_df_to_db(transformed_df)


# Created an admin user
''' 
 Commnd:
    airflow users create \
        --username admin \
        --firstname Peter \
        --lastname Parker \
        --role Admin \
        --email spiderman@superhero.org
'''

# define the arguments to the DAG
default_args = {
    'owner' : 'sai',
    'start_date' : utils.dates.days_ago(1),
    'depends_on_past' : True,
    'email' : ['saipranavgodishala49@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=30),
}

# Instantiate DAG
dag = DAG(dag_id = "ETL_pipeline",
          default_args=default_args,
          schedule_interval="0 0 * * *")

# Schedule Interval --> Chron Time Expression -->  "Min   Hr   day_of_month   week   day_of_week"  
# 0 0 * * * -->  Run the DAG for every  -->   0th min  0th hr evry day 


# Define the ETL task
etl_task = PythonOperator(task_id = "etl_task", python_callable=etl, dag=dag)

etl()

# After this save the files, and move them into dags directory in the airflow directory
# Start the webUI -->  airflow webserver
# Set the environment variable AIRFLOW_HOME
# Start the scheduler -->  airflow scheduler -->  through this we can add a DAG to the airflow scheduler