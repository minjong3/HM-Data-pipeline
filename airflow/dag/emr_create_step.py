from airflow import DAG

from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from slack_script import SlackAlert

slack = SlackAlert("airflow_result",'key')

def print_result(**kwargs):
    r = kwargs['task_instance'].xcom_pull(key='result_msg')
    print('message : ', r)


DAG_ID = "emr_articles_coustomers_train_transform_dag"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["test@naver.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

SPARK_STEPS = [
    {
        "Name": "partition_articles",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://11team-hm/scripts/pyspark/emr_parition_articles.py",
                "--data_source",
                "s3://11team-hm/source/articles.csv",
                "--output_uri",
                "s3://11team-hm/output/articles/"
            ],
        },
    },
    {
        "Name": "partition02_customers",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://11team-hm/scripts/pyspark/emr_parition_customers.py",
                "--data_source",
                "s3://11team-hm/source/customers.csv",
                "--output_uri",
                "s3://11team-hm/output/customers"
            ],
        },
    },
    {
        "Name": "partition03_transactions",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://11team-hm/scripts/pyspark/emr_parition_transactions.py",
                "--data_source",
                "s3://11team-hm/source/transactions_train.csv",
                "--output_uri",
                "s3://11team-hm/output/transactions"
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "11team-hm",
    "LogUri": "s3://11team-hm/logs/emr",
    "ReleaseLabel": "emr-6.15.0",
    "Applications": [
        {"Name": "Spark"},
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "JupyterEnterpriseGateway"},
        {"Name": "Livy"},
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "c5.xlarge",
                "InstanceCount": 1,
                        },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "c6g.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Task nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "TASK",
                "InstanceType": "c6g.xlarge",
                "InstanceCount": 2,
            },

        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
        "Ec2KeyName": "11team-hm",
        "Ec2SubnetId": "subnet-0257b07486cebdb95",
        'EmrManagedMasterSecurityGroup': 'sg-029a91313837deca3',
        'EmrManagedSlaveSecurityGroup': 'sg-029a91313837deca3',
    },
    "VisibleToAllUsers": True,
    "JobFlowRole": "AmazonEMR-InstanceProfile-20231126T145755",
    "ServiceRole": "EMR_DefaultRole",
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date = datetime.utcnow(),
    schedule_interval=None,
    tags=["emr"],
    on_success_callback=slack.success_msg,
    on_failure_callback=slack.fail_msg,
) as dag:
    cluster_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        region_name="ap-northeast-2",
    )

    step_adder_articles = EmrAddStepsOperator(
        task_id="add_steps_partition_articles",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=[SPARK_STEPS[0]],
    )

    step_adder_customers = EmrAddStepsOperator(
    task_id="add_steps_partition_customers",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=[SPARK_STEPS[1]], 
    )

    step_adder_transactions = EmrAddStepsOperator(
    task_id="add_steps_partition_transactions",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=[SPARK_STEPS[2]],
    )

    step_checker = EmrStepSensor(
        task_id="step_checker",
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps_partition_transactions', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    msg = PythonOperator(
        task_id='msg',
        python_callable=print_result,
    )

    cluster_creator >> step_adder_articles
    step_adder_articles >> step_adder_customers
    step_adder_customers >> step_adder_transactions 
    step_adder_transactions >> step_checker
    step_checker >> msg