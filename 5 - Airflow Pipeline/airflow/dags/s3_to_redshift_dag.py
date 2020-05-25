from datetime import datetime, timedelta
import os
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),          
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry' : False
}

dag = DAG(
        'udac_example_dag', 
        default_args=default_args, 
        schedule_interval='0 * * * *',
        description = 'Load and transform data in Redshift with Airflow'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = 'public.staging_events',
    s3_bucket = 'udacity-dend',
    s3_key = "log_data",
    redshift_conn_id = 'redshift',
    aws_credentials_id="aws_credentials",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='public.songs',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    json='s3://udacity-dend/song_data',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    aws_credentials = 'aws_credentials',
    table = 'public.songplays',
    truncate_table = True,
    query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    aws_credentials = 'aws_credentials',
    table = 'public.users',
    truncate_table = True,
    query = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    aws_credentials = 'aws_credentials',
    table = 'public.songs',
    truncate_table = True,
    query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,    
    aws_credentials = 'aws_credentials',
    table = 'public.artists',
    truncate_table = True,
    query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    aws_credentials = 'aws_credentials',
    table = 'public.time',
    truncate_table = True,
    query = SqlQueries.time_table_insert
)

dq_checks = [{'query': 'SELECT COUNT(*) FROM songs WHERE songid is NULL', 'exp_result' : 0}, 
{'query': 'SELECT COUNT(*) FROM users WHERE userid is NULL', 'exp_result' : 0}]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    dq_checks = dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# ----------------------------------------------------------------------
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] \
>> load_songplays_table >> [load_song_dimension_table
                            , load_artist_dimension_table
                            , load_time_dimension_table
                            , load_user_dimension_table] \
>> run_quality_checks >> end_operator