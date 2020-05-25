from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 aws_credentials = '',
                 redshift_conn_id = '',
                 table = '',
                 truncate_table = True,
                 query = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query            

    def execute(self, context):
        redshift = PostgresHook(postres_conn_id = self.redshift_conn_id)

        if self.truncate_table == True:
            self.log.info(f'TRUNCATING TABLE: {self.table}')
            redshift.run(f'DELETE FROM {self.table}')

        self.log.info(f'RUNNING QUERY: {self.query}')
        redshift.run(f'INSERT INTO {self.table} {self.query}')

