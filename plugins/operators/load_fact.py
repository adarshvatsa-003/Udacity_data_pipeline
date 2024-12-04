from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(f'Truncate table {self.table}')
            postgres.run(f'TRUNCATE {self.table}')
            
        self.log.info(f'Loading fact table {self.table}')
        postgres.run(f'INSERT INTO {self.table} {self.sql}')
        self.log.info('Finished')