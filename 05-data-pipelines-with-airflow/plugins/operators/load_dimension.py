from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_query='',
                 delete=True,
                 table_name='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.delete = delete
        self.table_name = table_name

    def execute(self, context):
        self.log.info('Create a Postgre hook')
        redshift_hook = PostgresHook(
            postgres_conn_id=self.redshift_conn_id)
        
        if self.delete:
            self.log.info(f'Deleting data on {self.table_name}')
            redshift_hook.run(f'DELETE FROM {self.table_name}')
            
        self.log.info(f'Loading data into {self.table_name}')
        redshift_hook.run(self.sql_query)
        self.log.info(f'Dimension Table {self.table_name} loaded.')