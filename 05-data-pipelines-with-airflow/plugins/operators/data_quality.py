from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Creating a PostgreHook')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info(f'Start to check data quality on {table}')
            records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {table}')
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"Table {table} doesn't have any records")
                raise ValueError(f'Data quality check failed')
        self.log.info(f'Data quality on {table} is good')