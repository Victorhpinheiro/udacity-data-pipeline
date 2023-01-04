from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            IGNOREHEADER 1
            DELIMITER ','
            """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.execution_date = kwargs.get('execution_date')   

    def execute(self, context):
        """
        Copy data from S3 to redshift cluster
        """
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Picking staging file for table {self.table_name} from location : {s3_path}")
        
        if self.file_format == "JSON":
            json_copy = StageToRedshiftOperator.copy_sql + "FORMAT AS json 'auto'"
            self.log_json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
            copy_query = json_copy.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key)
        else:
            csv_copy = StageToRedshiftOperator.copy_sql + "IGNOREHEADER 1 DELIMITER ','"
            copy_query = csv_copy.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key)
        
        self.log.info(f"Copy query : {copy_query}")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        redshift_hook.run(copy_query)
        self.log.info(f"Table {self.table_name} staged successfully!!")





