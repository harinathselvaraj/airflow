# ~/airflow/dags/dimensionload.py  
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta  

import os
import boto3
import psycopg2
s3 = boto3.client('s3')
import time
import hashlib

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
     
def process_data(event):
    
    curs = conn.cursor()
    curs.execute("ROLLBACK")
    conn.commit()    
        
    try:
        print('In progress: Dropping stage table if exists')
        drop_stage_table_query = event['drop_process_stage_query']
        cur.execute(drop_stage_table_query)
        try:
            print('In progress: create process stage query')
            create_process_stage_query = event['create_process_stage_query']
            cur.execute(create_process_stage_query)
            try:
                print('In progress: target load query')
                target_load_query = event['target_load_query']
                cur.execute(target_load_query)
                try:
                    print('In progress: drop stage table query')
                    drop_stage_table_query = event['drop_process_stage_query']
                    cur.execute(drop_stage_table_query)
                except Exception as e:
                    return('4th step:' + str(e))                
            except Exception as e:
                return('3rd step:' + str(e))            
        except Exception as e:
            return('2nd step:' + str(e))        
    except Exception as e:
        return('1st step:' + str(e))
    return('success')

def load_name():
    event = {
    "entity": "Country",
    "create_process_stage_query": "create table STG_SCHEMA.<table_name> (column) as (select column_name from dim_schema.d_table where 1=0);",
    "target_load_query": "begin transaction; insert into STG_SCHEMA.stage select column_name from ( select column_name from stage ); insert into dim_schema.dimension_table (column_name, active_flag, insert_user_id, insert_timestamp, last_update_user_id, last_update_timestamp) select column_name, 'Y' as active_flag, 'dbwriter' as insert_user_id, CURRENT_TIMESTAMP as insert_timestamp, 'dbwriter' as last_update_user_id, CURRENT_TIMESTAMP as last_update_timestamp from (select column_name from STG_SCHEMA.stagetable MINUS select column_name from dim_schema.dimension_table); end transaction;",
    "drop_process_stage_query": "drop table IF EXISTS STG_SCHEMA.<stage_table>;",
        }    
    if process_data(event) == 'success':
        print('success')
    else:
        print(process_data(event))
 

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
dag = DAG('dimension_load',
            description='Dimension Load ETL from Redshift Stage tables to Dimension tables',
            start_date= datetime.now() - timedelta(days= 1),
            catchup=False,
            default_args=default_args,
            schedule_interval= '0 * * * *'
         )  

# Operators
start_operator= DummyOperator(task_id= 'start', retries= 0, dag= dag)  
<load_name>_operator= PythonOperator(task_id= '<load_name>',
                               python_callable= load_name,
                               dag= dag)

stage_load_external_operator = ExternalTaskSensor(task_id='stage_load_external', external_dag_id = 'stage_load', external_task_id = None, dag=dag, mode = 'reschedule')

# Sequence
stage_load_external_operator >> start_operator >> countryload_operator