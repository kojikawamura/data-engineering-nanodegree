from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    ui_color = '#ADD8E6'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # Create credentials and postgre hook
        self.log.info('Creating Postgre hook')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Create tables in Redshift
        self.log.info('Creating tables in Redshift')
        with open('/home/workspace/airflow/create_tables.sql', 'r') as f:
            sql_queries = f.read()
            redshift_hook.run(sql_queries)

        self.log.info("Tables are successfully created ")
