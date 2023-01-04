from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    ui_color = '#89DA10'
    sql_file = '../create_tables.sql'

    @apply_defaults
    def __init__(self, redshift_conn_id="", *args, **kwargs):
        
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Postgres SQL Hook created')

        self.log.info('Creating tables in Redshift.')
        with open(CreateTableOperator.sql_file, 'r') as file:
            queries = file.read()
        
        redshift.run(queries)
        self.log.info("Tables created ")