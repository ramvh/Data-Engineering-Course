#Instructions
#In this exercise, we’ll refactor a DAG with a single overloaded task into a DAG with several tasks with well-defined boundaries
#1 - Read through the DAG and identify points in the DAG that could be split apart
#2 - Split the DAG into multiple PythonOperators
#3 - Run the DAG

import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


#
# TODO: Finish refactoring this function into the appropriate set of tasks,
#       instead of keeping this one large task.
#
# def load_and_analyze(*args, **kwargs):

def log_oldest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")
        
        
def log_youngest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")

def log_station_count():
    redshift_hook = PostgresHook('redshift')
    records = redshift_hook.get_records("""SELECT city FROM city_station_counts ORDER BY COUNT(city) DESC LIMIT 1""")
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"{records[0][0]} has the most number of stations")

dag = DAG(
    "lesson3.exercise2",
    start_date=datetime.datetime.utcnow()
)

create_station_count_task = PostgresOperator(
    task_id = 'create_station_count_task',
    dag = dag,    
    # Count the number of stations by city
    sql ="""
        BEGIN;
        DROP TABLE IF EXISTS city_station_counts;
        CREATE TABLE city_station_counts AS(
            SELECT city, COUNT(city)
            FROM stations
            GROUP BY city
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

create_oldest_task = PostgresOperator(
    task_id="create_oldest",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)


create_youngest_task = PostgresOperator(    
    task_id = 'create_youngest_task',
    dag = dag,
    postgres_conn_id = 'redshift',
    # Find all trips where the rider was under 18
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """
)

log_oldest_task = PythonOperator(
    task_id="log_oldest",
    dag=dag,
    python_callable=log_oldest
)
    
log_youngest_task = PythonOperator(
    task_id = 'log_youngest',
    dag=dag,
    python_callable = log_youngest
)
    
log_station_count_task = PythonOperator(
    task_id = 'log_station_count',
    dag=dag,
    python_callable = log_station_count
)

# load_and_analyze >> create_oldest_task
create_oldest_task >> log_oldest_task
create_youngest_task >> log_youngest_task
create_station_count_task >> log_station_count_task
