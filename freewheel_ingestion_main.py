"""
Script Name        : freewheel_ingestion_main.py
Description        : This code is to process the file from source to
raw and further process to stage through given validations.
Author             : Srikanth, Sarath, Chandan
Version            : 1.0
Last Modified Date : 17/09/2018
"""
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.sftp_to_s3_plugin import SFTPToS3Operator
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
from airflow.operators.sftp_s3_file_size_plugin import SFTPS3FileSizeOperator
from airflow.operators.subdag_operator import SubDagOperator
from utils.adsales_job_config import JobConfig

JOB_ARGS = JobConfig.get_config("adsales")
logging.info("args are %s", JOB_ARGS)
PARENT_DAG_NAME = "freewheel_ingestion_main"
FREEWHEEL_FILE_NAME = JOB_ARGS['freewheel_file_name']
SFTP_PATH_DICT = JOB_ARGS['sftp_path_dict']
BUCKET_NAME_RAW = JOB_ARGS['freewheel_bucket_name_raw']
S3_RAW_BUCKET = JOB_ARGS['s3_freewheel_raw_bucket']
S3_STAGE_BUCKET = JOB_ARGS['s3_freewheel_stage_bucket']
S3_KEY_DICT = JOB_ARGS['s3_key_dict']
SFTP_FILE_NAME = JOB_ARGS['sftp_file_name']
STAGE_SFTP_FILE_NAME = JOB_ARGS['stage_sftp_file_name']
SFTP_FILE_EXTN = JOB_ARGS['sftp_file_extn']
STAGE_SFTP_FILE_EXTN = JOB_ARGS['stage_sftp_file_extn']
SFTP_NETWORK_ID = JOB_ARGS['network_id']
SFTP_CONN_DICT = JOB_ARGS['sftp_conn_dict']
ADSALES_EMR = JOB_ARGS['adsales_emr']
FW_STAGE_CODE_PATH = JOB_ARGS['fw_stage_code_path']
FW_STAGE_DQ_CODE_PATH = JOB_ARGS['fw_stage_dq_code_path']
S3_KEY_DICT_STAGE = JOB_ARGS['s3_key_dict_stage']
S3_KEY_DICT_STAGE_OUT = JOB_ARGS['s3_key_dict_stage_out']
DUPLICATE_COLUMN_LIST = JOB_ARGS['duplicate_column_list']
NULL_COLUMN_LIST = JOB_ARGS['null_column_list']
SRC_SYS_ID = JOB_ARGS['src_sys_id']
DATE_STR_FORMATTED = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m%d") }}'
DEFAULT_ARGS = {
    'owner': 'Ad-Sales',
    'start_date': datetime(2018, 9, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': '',
    'email_on_failure': True,
}


def load_file_subdag(parent_dag_name, child_dag_name, sftp_conn_id, args):
    """This dag will iteratively call the listed tasks."""
    dag_subdag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
    )
    with dag_subdag_subdag:
        file_check = \
            SFTPSensor(
                task_id='file_check',
                sftp_conn_id=sftp_conn_id,
                poke_interval=60,
                timeout=600,
                soft_fail=False,
                path='{}{}{}{}{}'.format(
                    SFTP_PATH_DICT[child_dag_name],
                    parent_dag_name.split(".")[1],
                    SFTP_FILE_NAME[child_dag_name], '%s',
                    SFTP_FILE_EXTN[child_dag_name]) % (DATE_STR_FORMATTED)
            )
        file_transfer_raw = \
            SFTPToS3Operator(
                task_id='file_transfer_raw',
                sftp_conn_id=sftp_conn_id,
                sftp_path='{}{}{}{}{}'.format(
                    SFTP_PATH_DICT[child_dag_name],
                    parent_dag_name.split(".")[1],
                    SFTP_FILE_NAME[child_dag_name], '%s',
                    SFTP_FILE_EXTN[child_dag_name]) % (DATE_STR_FORMATTED),
                s3_conn_id=JOB_ARGS['s3_conn_id'],
                s3_bucket=BUCKET_NAME_RAW[parent_dag_name.split(".")[1]],
                s3_key='{}{}{}{}{}'.format(
                    S3_KEY_DICT[child_dag_name],
                    parent_dag_name.split(".")[1],
                    SFTP_FILE_NAME[child_dag_name],
                    '%s', SFTP_FILE_EXTN[child_dag_name]) % (DATE_STR_FORMATTED)
            )
        abc_validations = \
            SFTPS3FileSizeOperator(
                task_id='abc_validations',
                sftp_conn_id=sftp_conn_id,
                sftp_path='{}{}{}{}{}'.format(
                    SFTP_PATH_DICT[child_dag_name],
                    parent_dag_name.split(".")[1],
                    SFTP_FILE_NAME[child_dag_name],
                    '%s', SFTP_FILE_EXTN[child_dag_name]) % (DATE_STR_FORMATTED),
                s3_conn_id=JOB_ARGS['s3_conn_id'],
                s3_bucket=BUCKET_NAME_RAW[parent_dag_name.split(".")[1]],
                s3_key='{}{}{}{}{}'.format(
                    S3_KEY_DICT[child_dag_name],
                    parent_dag_name.split(".")[1],
                    SFTP_FILE_NAME[child_dag_name],
                    '%s', SFTP_FILE_EXTN[child_dag_name]) % (DATE_STR_FORMATTED)
            )
        file_stage_copy = \
            SSHOperator(
                task_id='file_stage_copy',
                ssh_conn_id=ADSALES_EMR,
                command='{}{}{}'.format(
                    JOB_ARGS['spark_submit'],
                    JOB_ARGS['spark_jars'],
                    FW_STAGE_CODE_PATH
                ) + '{}{}{}{}{}{}'.format(
                    S3_RAW_BUCKET[parent_dag_name.split(".")[1]],
                    S3_KEY_DICT[child_dag_name],
                    parent_dag_name.split(".")[1],
                    SFTP_FILE_NAME[child_dag_name], '%s',
                    STAGE_SFTP_FILE_EXTN[child_dag_name]
                ) % (DATE_STR_FORMATTED) + '{}{}{}{}{}'.format(
                    S3_STAGE_BUCKET[parent_dag_name.split(".")[1]],
                    S3_KEY_DICT_STAGE[child_dag_name],
                    parent_dag_name.split(".")[1],
                    STAGE_SFTP_FILE_NAME[child_dag_name],
                    '/%s/ ') % (DATE_STR_FORMATTED
                                ) + SRC_SYS_ID[parent_dag_name.split(".")[1]]
            )
        dq_check = \
            SSHOperator(
                task_id='dq_check',
                ssh_conn_id=ADSALES_EMR,
                command='{}{}{}'.format(
                    JOB_ARGS['spark_submit'],
                    JOB_ARGS['spark_jars'],
                    FW_STAGE_DQ_CODE_PATH
                ) + ' ' + '{}{}{}{}{}'.format(
                    S3_STAGE_BUCKET[parent_dag_name.split(".")[1]],
                    S3_KEY_DICT_STAGE[child_dag_name],
                    parent_dag_name.split(".")[1],
                    STAGE_SFTP_FILE_NAME[child_dag_name],
                    '/%s/'
                ) % (DATE_STR_FORMATTED) + ' ' + '{}{}{}{}{}'.format(
                    S3_STAGE_BUCKET[parent_dag_name.split(".")[1]],
                    S3_KEY_DICT_STAGE[child_dag_name],
                    parent_dag_name.split(".")[1],
                    STAGE_SFTP_FILE_NAME[child_dag_name],
                    '/%s/'
                ) % (DATE_STR_FORMATTED) + ' ' + '{}{}{}{}{}'.format(
                    S3_STAGE_BUCKET[parent_dag_name.split(".")[1]],
                    S3_KEY_DICT_STAGE_OUT[child_dag_name],
                    parent_dag_name.split(".")[1],
                    STAGE_SFTP_FILE_NAME[child_dag_name],
                    '/%s/'
                ) % (DATE_STR_FORMATTED) + ' ' + '{}{}{}{}'.format(
                    DUPLICATE_COLUMN_LIST[child_dag_name],
                    NULL_COLUMN_LIST[child_dag_name],
                    str(JOB_ARGS['stage_dup_check']),
                    str(JOB_ARGS['stage_null_check'])
                )
            )
        file_check >> file_transfer_raw >> abc_validations >> file_stage_copy  >>dq_check
    return dag_subdag_subdag


def load_network_subdag(parent_dag_name, child_dag_name, sftp_conn_id, args):
    """This will load the source files corresponding network id."""
    dag_subdag = DAG(dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
                     default_args=args,
                     )
    with dag_subdag:
        for i in FREEWHEEL_FILE_NAME.get(child_dag_name):
            load_tasks = SubDagOperator(task_id=i,
                                        subdag=load_file_subdag(
                                            parent_dag_name + "." +
                                            child_dag_name,
                                            i,
                                            sftp_conn_id,
                                            DEFAULT_ARGS
                                        ),
                                        default_args=DEFAULT_ARGS,
                                        dag=dag_subdag,
                                        )
    return dag_subdag


with DAG('freewheel_ingestion_main',
         default_args=DEFAULT_ARGS,
         schedule_interval=JOB_ARGS["schedule_interval"],
         catchup=False,
         ) as dag:

    for j in SFTP_NETWORK_ID:
        load_network_tasks = SubDagOperator(task_id=SFTP_NETWORK_ID.get(j),
                                            subdag=load_network_subdag(
                                                'freewheel_ingestion_main',
                                                SFTP_NETWORK_ID.get(j),
                                                SFTP_CONN_DICT.get(j),
                                                DEFAULT_ARGS
                                            ),
                                            default_args=DEFAULT_ARGS,
                                            dag=dag,
                                            )
