from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    sql_insert = """
        INSERT INTO {} 
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 truncate_load=True,
                 sql_statement="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_load = truncate_load
        self.sql_statement = sql_statement

    def execute(self, context):
        """
            Transforms data into dimensional tables and loads into Redshift
                - redshift_conn_id: redshift cluster connection
                - table: table name on redshift
                - truncate_load: Indicator whether to delete existing data in table first before load
                - sql_statement: sql statement to execute to transform and load the table
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_load:
            self.log.info("Clearing data from destination Redshift table")
            try:
                redshift.run("DELETE FROM {}".format(self.table))
            except:
                self.log.error("Error: Could not delete from Redshift table")
                raise
             
         
        formatted_sql = LoadDimensionOperator.sql_insert.format(
            self.table,
            self.sql_statement
        )
        self.log.info("Inserting data into destination Redshift table")
        self.log.info(f"Executing {formatted_sql}")
        try:
            redshift.run(formatted_sql)
            self.log.info("Insert successful")
        except Exception as e:
            self.log.error("Error: Could not insert data into Redshift table")
            self.log.error(e)
            raise
