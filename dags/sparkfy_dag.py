from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTableOperator)
from helpers import SqlQueries
from airflow.operators.subdag_operator import SubDagOperator
from dimensions_subdag import load_dimension_subdag

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
# need to define aws credentials 
# need to stablish a connection with redshift
# and s3 bucket



default_args = {
    'owner': 'victor',
    'start_date': datetime(2022, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}
dag_name = 'sparkfy_dag'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_on_redshift = CreateTableOperator(
    task_id='create_tables_in_redshift',
    redshift_conn_id='redshift',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table_name="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    file_format="JSON",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table_name="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    file_format="JSON",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        table_name="users",
        sql_query=SqlQueries.user_table_insert,
        delete_load=False,
    ),
    task_id="Load_user_dim_table",
    dag=dag,
)

load_song_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        table_name="songs",
        sql_query=SqlQueries.song_table_insert,
        delete_load=False,
    ),
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        table_name="artists",
        sql_query=SqlQueries.artist_table_insert,
        delete_load=False,
    ),
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = SubDagOperator(
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        table_name="time",
        sql_query=SqlQueries.time_table_insert,
        delete_load=False,
    ),
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Dependencies
start_operator >> create_tables_on_redshift >> [stage_songs_to_redshift, stage_events_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                           load_time_dimension_table] >> run_quality_checks >> end_operator
