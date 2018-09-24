import logging
import datetime
from tempfile import NamedTemporaryFile
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin

class SFTPS3FileSizeOperator(BaseOperator):
    """
    SFTP To S3 Operator
    :param sftp_conn_id:    The destination redshift connection id.
    :type sftp_conn_id:     string
    :param sftp_path:       The path to the file on the FTP client.
    :type sftp_path:        string
    :param s3_conn_id:      The s3 connnection id.
    :type s3_conn_id:       string
    :param s3_bucket:       The destination s3 bucket.
    :type s3_bucket:        string
    :param s3_key:          The destination s3 key.
    :type s3_key:           string
    """

    template_fields = ('sftp_path',
                       's3_key')
    def __init__(self,
                 sftp_conn_id,
                 sftp_path,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        try:
            s3_hook = S3Hook(self.s3_conn_id)
            logging.info("Connected to S3 hook")
        except AirflowException as e:
            logging.info("Error in Connecting to S3 Hook")
            exit(1)
        try:
            ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
            logging.info("Connected to SSH Hook")
            ssh_client = ssh_hook.get_conn()
            sftp_client = ssh_client.open_sftp()
            logging.info("Connected to SFTP")
        except AirflowException as e:
            logging.info("Error in Connecting to SFTP")
            exit(1)
        try:
            with NamedTemporaryFile("w") as f:
                logging.info("Connecting to SFTP and using STAT to get properties of file.")
                ftpfilesizestat = sftp_client.stat(self.sftp_path)
                logging.info("Connected")
                ftpfilesize = ftpfilesizestat.st_size
                logging.info("size of current file in SFTP is calculated and size is " +str(ftpfilesize))
                logging.info("generating key name in s3")
                keyname = s3_hook.get_key(self.s3_key, bucket_name=self.s3_bucket)
                logging.info("key name is generated and calculating size of object in s3")
                awsfilesize = keyname.get()["ContentLength"]
                logging.info("size of the object in s3 is calculated and size is " +str(awsfilesize))
                if ftpfilesize == awsfilesize:
                    logging.info("file sizes are equal")
                else:
                    logging.info("file sizes are not equal")
                logging.info("SUCCEEDED")
        except AirflowException as e:
            logging.info("Transfer to S3 FAILED", str(e))
            exit(1)

class SFTPS3FileSizePlugin(AirflowPlugin):
    name = "sftp_s3_file_size_plugin"
    operators = [SFTPS3FileSizeOperator]
