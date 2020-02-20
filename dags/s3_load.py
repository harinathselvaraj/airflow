# ~/airflow/dags/stageload.py  
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta  

import os
import boto3
import psycopg2
s3 = boto3.client('s3')
import time
import hashlib
import json

# SLACK INTEGRATION - START ##################################################################
from slack_webhook import Slack
def task_success_slack_alert(context):
    slack_msg = """
            :large_blue_circle: Task Succeeded! 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )
    slack = Slack(url='https://hooks.slack.com/services/<get it from Slack Hook>')
    slack.post(text=slack_msg)
    return 'Finished executing success slack alert!'


def task_fail_slack_alert(context):

    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )
    slack = Slack(url='https://hooks.slack.com/services/<get it from Slack Hook>')
    slack.post(text=slack_msg)
    return 'Finished executing failure slack alert!'
# SLACK INTEGRATION - END ##################################################################


# Environmental variables
event = [
{ "command" : '''copy <table_name> from '<s3_path_with_file_prefix>' credentials 'aws_access_key_id=<access_key_id>;aws_secret_access_key=<secret_key>';'''},
]

iam_role = "arn:aws:iam::<profile_id>:role/<role_name>"
db_database = "<db_name>"
db_user = "<username>"
db_password = "<password>"
db_port = "<port_number>"
db_host = "<unique_identifier>.eu-west-1.redshift.amazonaws.com"

aws_access_key_id = '<access_key_id>'
aws_secret_access_key = '<secret_key>'

AWS_DEFAULT_REGION = 'eu-west-1'

try:
    conn = psycopg2.connect("dbname=" + db_database
                            + " user=" + db_user
                            + " password=" + db_password
                            + " port=" + db_port
                            + " host=" + db_host)
    conn.autocommit = True
    cur = conn.cursor()
    print ('Successfully connected to the Database!')
except Exception as e:
    print(e)
     
def stageload():
    characters = 'abcdefghijklmnopqrstuvwxyz0123456789'
    for i in event:
        for character in characters:
            part = "-" + character + "'"
            command = i['command'].replace("-'",part)
            try:
                cur.execute(command)
            except Exception as e:
                print(e)
            print('finished executing the below command - \n ',command, '\n')    
    return('s3 tables loaded successfully!')

# DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
    "on_success_callback": task_success_slack_alert
}
dag = DAG('stage_load_prod',
            description='Stage Load ETL from S3 Files to Stage tables',
            start_date= datetime.now() - timedelta(days= 1),
            catchup=False,
            default_args=default_args,
            schedule_interval= '0 12 * * *'
         )  

# Operators
start_operator= DummyOperator(task_id= 'start', retries= 0, dag= dag)  
stageload_operator= PythonOperator(task_id= 'stageload',
                               python_callable= stageload,
                               dag= dag)
start_operator >> stageload_operator