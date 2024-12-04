from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = '',
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests
        
    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id = self.redshift_conn_id)   
        for test in self.tests:
            table = test.get("table")
            result = test.get("return")
            
            records = postgres.get_records(table)[0]
            if records[0] == result:
                self.log.info("Data quality check passed")
            else:
                self.log.info("Data quality check failed")
        self.log.info("Data quality check finished")