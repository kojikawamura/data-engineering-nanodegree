import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import (
    CreateTableOperator,
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator)
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries
from subdag import load_dimension_subdag


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket = 'udacity-dend'
s3_song_key = 'song_data'
s3_log_key = 'log_data'

default_args = {
    'owner': 'udacity',
#     'start_date': datetime(2019, 1, 12),
    'start_date': datetime(2019, 11, 1),
    'retries': 1,
}

dag_name = 'udac_example_dag'
dag = DAG(
    dag_name,
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    max_active_runs=1
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift = CreateTableOperator(
    task_id='create_tables_in_redshift',
    dag=dag,
    redshift_conn_id='redshift',
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credential_id='aws_credentials',
    redshift_conn_id='redshift',
    table_name='staging_events',
    s3_bucket=s3_bucket,
    s3_key=s3_log_key,
    log_json_file='log_json_path.json',
)

stage_songs_to_redshift= StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credential_id='aws_credentials',
    redshift_conn_id='redshift',
    table_name='staging_songs',
    s3_bucket=s3_bucket,
    s3_key=s3_song_key,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert, 
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.user_table_insert, 
    table_name='users',
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.song_table_insert, 
    table_name='songs',
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.artist_table_insert, 
    table_name='artists',
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query=SqlQueries.time_table_insert, 
    table_name='time',
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables_in_redshift
create_tables_in_redshift >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator