from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 dq_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks
        
    def execute(self, context):
        self.log.info('STARTING DATA QUALITY CHECKS...')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        

        for check in self.dq_checks:
            sql = check['query']
            exp_result = check['exp_result']
            
            actual_result = redshift.get_records(sql)
            if exp_result != actual_result[0][0]:
                raise ValueError(f'DATA QUALITY FAILED ON TABLE: {self.table}, EXPECTED RESULT: {exp_result}, ACTUAL RESULT: {actual_result}')
            else:
                self.log.info(f'DATA QUALITY PASSED FOR TABLE: {self.table}, QUERY: {sql}, ACTUAL RESULT: {actual_result}')