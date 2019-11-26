from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 quality_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.quality_checks = quality_checks

    def execute(self, context):
        """
            Perform data quality checks on Redshift dimensional and fact tables
                - redshift_conn_id: redshift cluster connection
                - tables: table names on redshift
                - quality_checks: list of data quality checks to be done
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        list_of_errors = []
        
        for table in self.tables:
            self.log.info("Performing data quality checks on {}".format(table))
            for check in self.quality_checks:
                check_name = check.get("name_of_check")
                formatted_sql_check = check.get("sql_statement").format(table)
                min_expected_result = check.get("min_expected_result")
                
                check_result = int(redshift.get_records(formatted_sql_check)[0][0])
                if check_result < min_expected_result:
                    list_of_errors.append("{}:{}".format(table, check_name))
        
        self.log.info("All data quality checks completed")
        if len(list_of_errors) == 0:
            self.log.info("All data quality checks passed successfully")
        else:
            self.log.error("The following checks failed:")
            for item in list_of_errors:
                self.log.error(item)
            raise