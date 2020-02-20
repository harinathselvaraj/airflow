""" dag_coin_flip.py
An example of using the Slack Alert airflow function with a simple single-task DAG.
It uses the PythonOperator to simulate a coin flip. If the coin flips "tails" it 
raises an exception, forcing a failed task Slack alert. If the coin flips "heads"
it passes and calls the succeess task Slack alert.
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import random

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


def coin_flip():
    """
    This is the simple coin flip code that raises an exception "half the time"
    Args:
        None
    Returns:
        True only if coin flips "heads"
    Raises:
        ValueError: If coin flips "tails"
    """
    flip = random.random() > 0.5
    if not flip:
        raise ValueError("Coin flipped tails. We lose!")
    print("Coin flipped heads. We win!")
    return True


# Default DAG arguments. Note the "onl_failure_callback"
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 2, 22),
    "email": ["foo@bar.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    # Here we show an example of setting a failure callback applicable to all
    # tasks in the DAG
    "on_failure_callback": task_fail_slack_alert,
}

# Create the DAG with the parameters and schedule
dag = DAG(
    "hourly_coin_flip",
    catchup=False,
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
)

"""
Create a DAG with a single operator, "coin_flip" It's a simply python script 
that "flips a coin" and raises an error if it is "tails". That way the coin 
flip DAG sometimes succeeds and sometimes fails, which triggers either the 
success or fail slack callback functions
"""
with dag:
    t1 = PythonOperator(
        task_id="coin_flip",
        python_callable=coin_flip,
        # Here we show assigning success callback just for this task
        on_success_callback=task_success_slack_alert,
    )