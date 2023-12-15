from airflow import DAG

from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from slack_script import SlackAlert

slack = SlackAlert("airflow_result",'key')

def print_result(**kwargs):
    r = kwargs['task_instance'].xcom_pull(key='result_msg')
    print('message : ', r)

DAG_ID = "transaction_slack_dag"

DEFAULT_ARGS = {
    "owner": "mj",
    "depends_on_past": False,
    "email": ["hoplk4153@naver.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "provide_context": True,
}


SPARK_STEPS = [
    {
        "Name": "partition",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://11team-hm/scripts/pyspark/PN_transactions.py",
                "--data_source",
                "s3a://11team-hm/source/PN_transactions.parquet",
                "--output_uri",
                "s3a://11team-hm/ final_output/transaction/"
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
    start_date = datetime(2023, 12, 6),
    schedule_interval='25 5 * * *',  # 매일 11시 25분에 실행
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

    step_adder = EmrAddStepsOperator(
        task_id="step_adder",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
    )

    #step_adder 라는 이전 task의 결과를 대기하고 완료되면 다음 단계로 진행한다는 내용이랍니다.
    step_checker = EmrStepSensor(
        task_id="step_checker",
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='step_adder', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    msg = PythonOperator(
        task_id='msg',
        python_callable=print_result,
    )
    cluster_creator >> step_adder >> step_checker >> msg