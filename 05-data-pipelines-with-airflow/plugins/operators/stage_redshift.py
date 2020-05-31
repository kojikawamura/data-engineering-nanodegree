from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """
    
    @apply_defaults
    def __init__(self,
                 aws_credential_id='',
                 redshift_conn_id='',
                 table_name='',
                 s3_bucket='',
                 s3_key='',
                 log_json_file='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credential_id = aws_credential_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file

    def execute(self, context):
        # Create credentials and postgre hook
        self.log.info('Creating a Postgre hook')
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(
            postgres_conn_id=self.redshift_conn_id)
        
        # Copy data from S3 to Redshift
        self.log.info('Copying data from S3 to tables in Redshift')
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        copy_sql = self.copy_sql.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            'auto' if not self.log_json_file \
                else f's3://{self.s3_bucket}/{self.log_json_file}'
        )
        redshift_hook.run(copy_sql)
        
        self.log.info(f'{self.table_name} is successfully staged!')