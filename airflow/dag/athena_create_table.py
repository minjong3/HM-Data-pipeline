from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.dates import days_ago
from slack_script import SlackAlert

slack = SlackAlert("airflow_result",'key')

def print_result(**kwargs):
    r = kwargs['task_instance'].xcom_pull(key='result_msg')
    print('message : ', r)


DAG_ID = "create_athena_table_msck_dag"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["test@naver.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "provide_context": True
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=datetime.utcnow(),
    schedule_interval=None,
    tags=["athena"],
    on_success_callback=slack.success_msg,
    on_failure_callback=slack.fail_msg,
)

athena_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`customers` ( 
    `customer_id` string, `FN` double, 
    `Active` double, 
    `club_member_status` string, 
    `fashion_news_frequency` string, 
    `postal_code` string ) 
    
PARTITIONED BY (`age` int) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION 's3://11team-hm/doyoung/test/customers/' 
TBLPROPERTIES ('classification' = 'parquet');
"""

create_athena_table_customers_task = AthenaOperator(
    task_id="create_athena_customers_table",
    query=athena_query,
    database="default",
    output_location="s3://11team-hm/doyoung/location",
    aws_conn_id="aws_default",
    do_xcom_push=True,
    dag=dag,
)

# Task to run MSCK REPAIR TABLE
msck_repair_table_task = AthenaOperator(
    task_id="msck_repair_table_customers",
    query="MSCK REPAIR TABLE `customers`;",
    database="default",
    output_location="s3://11team-hm/doyoung/location",
    aws_conn_id="aws_default",
    do_xcom_push=True,
    dag=dag,
)

msg = PythonOperator(
        task_id='msg',
        python_callable=print_result,
    )

create_athena_table_customers_task >> msck_repair_table_task >> msg