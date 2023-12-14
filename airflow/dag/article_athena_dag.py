from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.models import DagRun
from airflow import settings
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from slack_script import SlackAlert
import pandas as pd
import s3fs
import json
from urllib.parse import urlparse, unquote
import time
import logging
from io import StringIO

s3_hook = S3Hook(aws_conn_id='aws_default')
slack = SlackAlert("airflow_result", 'key')
slack_token = 'key'
slack_channel = 'airflow_result'


# 로그 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def get_latest_csv_name(bucket_name='11team-hm', prefix='logs/athena/'):
    s3_hook = S3Hook(aws_conn_id='aws_default')

    # List objects in the bucket with the given prefix
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

    # Filter CSV files
    csv_keys = [key for key in keys if key.endswith('.csv')]

    if not csv_keys:
        return None

    # Sort CSV keys based on last modified time
    csv_keys.sort(key=lambda k: s3_hook.get_key(key=k, bucket_name=bucket_name).last_modified, reverse=True)

    # Get the latest CSV key (file name)
    latest_csv_name = csv_keys[0]

    # 변수 값 확인 및 로그 출력
    logger.info(f"Latest CSV file name: {latest_csv_name}")

    return latest_csv_name


def read_and_send_to_slack(**kwargs):
    latest_csv_name = get_latest_csv_name(bucket_name='11team-hm', prefix='logs/athena/')

    if latest_csv_name:
        s3_path = f"s3a://11team-hm/{latest_csv_name}"

        # 수정: S3Hook을 사용하여 데이터 읽어오기
        file_content = s3_hook.read_key(s3_path)

    # 문자열을 파일로 변환
        file_content = StringIO(file_content)

    # CSV 파일 읽기
        csv_data = pd.read_csv(file_content)

        json_data = csv_data.to_json(orient='records')
        
        # Send a user-friendly message to Slack using SlackWebhookOperator
         # Slack 메시지 내용 설정
        message = f"article\n{json_data}"
         # XCom에 메시지 저장
        kwargs['ti'].xcom_push(key='result_msg', value=message)

        return message
    else:
        return "No CSV file found in the specified path."
    
    logger.info(f"csv_data:\n{csv_data}")

def print_result(**kwargs):
    r = kwargs['task_instance'].xcom_pull(key='result_msg')
    print('message : ', r)

def get_execution_date(dt, **kwargs):
    session = settings.Session()
    dr = session.query(DagRun)\
        .filter(DagRun.dag_id == kwargs['task'].external_dag_id)\
        .order_by(DagRun.execution_date.desc())\
        .first()
    return dr.execution_date

def sleep_for_60_seconds(**kwargs):
    # 30초 동안 대기
    time.sleep(60)

# 무결성 테스트를 위한 쿼리
partition_query = "SELECT COUNT(*) AS partition_cnt FROM articles"
source_query = "SELECT COUNT(*) AS source_cnt FROM articles_source"
# 중복값 테스트를 위한 쿼리
duplication_query = "SELECT article_id, COUNT(*) AS duplication_cnt FROM articles \
    GROUP BY article_id HAVING COUNT(*) > 1;"

DEFAULT_ARGS = {
    "owner": "mj",
    "depends_on_past": False,
    "email": ["test@naver.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

dag_id = 'article_athena'
dag = DAG(
    dag_id=dag_id,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2023, 12, 6),
    schedule_interval='5 5 * * *', #2시 5분 시작
    tags=["athena"],
    catchup=False,
    on_success_callback=slack.success_msg,
    on_failure_callback=slack.fail_msg,
)

sensor = ExternalTaskSensor(
    task_id='wait_for_article_slack_dag',
    external_dag_id='article_slack_dag',
    external_task_id='msg',
    start_date=datetime(2023, 12, 6),
    execution_date_fn=get_execution_date,
    mode='reschedule',
    timeout=800,
)

# Athena 쿼리 실행
partition_query = AthenaOperator(
    task_id="partition_query",
    query=partition_query,
    database="11team-hm",
    output_location="s3://11team-hm/logs/athena/",
    aws_conn_id="aws_default",
    do_xcom_push=True,
    dag=dag,
)

# Athena 쿼리 실행
source_query = AthenaOperator(
    task_id="source_query",
    query=source_query,
    database="11team-hm",
    output_location="s3://11team-hm/logs/athena/",
    aws_conn_id="aws_default",
    do_xcom_push=True,
    dag=dag,
)
# Athena 쿼리 실행
duplication_query = AthenaOperator(
    task_id="duplication_query",
    query=duplication_query,
    database="11team-hm",
    output_location="s3://11team-hm/logs/athena/",
    aws_conn_id="aws_default",
    do_xcom_push=True,
    dag=dag,
)

# 성공 여부 슬랙에 알려주는 테스크
msg_athena = PythonOperator(
    task_id='msg_athena',
    python_callable=print_result,
)

# article 쿼리 결과 가져오기
get_partition_athena = PythonOperator(
    task_id='get_partition_athena',
    python_callable=read_and_send_to_slack,
    provide_context=True,
    dag=dag,
)

# article 쿼리 결과 가져오기
get_source_athena = PythonOperator(
    task_id='get_source_athena',
    python_callable=read_and_send_to_slack,
    provide_context=True,
    dag=dag,
)

# article 쿼리 결과 가져오기
get_duplication_athena = PythonOperator(
    task_id='get_duplication_athena',
    python_callable=read_and_send_to_slack,
    provide_context=True,
    dag=dag,
)

# 쿼리가 저장되는 동안 잠시 쉬기
sleep_task1 = PythonOperator(
    task_id='sleep_task1',
    python_callable=sleep_for_60_seconds,
    provide_context=True,
    dag=dag,
)

# 쿼리가 저장되는 동안 잠시 쉬기
sleep_task2 = PythonOperator(
    task_id='sleep_task2',
    python_callable=sleep_for_60_seconds,
    provide_context=True,
    dag=dag,
)

# 쿼리가 저장되는 동안 잠시 쉬기
sleep_task3 = PythonOperator(
    task_id='sleep_task3',
    python_callable=sleep_for_60_seconds,
    provide_context=True,
    dag=dag,
)

# Slack에 메시지를 보내는 task
slack_task1 = SlackAPIPostOperator(
    task_id='slack_task1',
    text="{{ task_instance.xcom_pull(task_ids='get_partition_athena', key='result_msg') }}",
    token=slack_token,  # Slack 앱의 토큰으로 교체
    channel=slack_channel,  # 전송할 채널명으로 교체
    dag=dag,
)

# Slack에 메시지를 보내는 task
slack_task2 = SlackAPIPostOperator(
    task_id='slack_task2',
    text="{{ task_instance.xcom_pull(task_ids='get_source_athena', key='result_msg') }}",
    token=slack_token,  # Slack 앱의 토큰으로 교체
    channel=slack_channel,  # 전송할 채널명으로 교체
    dag=dag,
)

# Slack에 메시지를 보내는 task
slack_task3 = SlackAPIPostOperator(
    task_id='slack_task3',
    text="{{ task_instance.xcom_pull(task_ids='get_duplication_athena', key='result_msg') }}",
    token=slack_token,  # Slack 앱의 토큰으로 교체
    channel=slack_channel,  # 전송할 채널명으로 교체
    dag=dag,
)

sensor >> partition_query >> sleep_task1 >> get_partition_athena >> slack_task1 >> \
source_query >> sleep_task2 >> get_source_athena >> slack_task2 >> \
duplication_query >> sleep_task3 >> get_duplication_athena >> slack_task3 >> msg