import argparse
import boto3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType

import pandas as pd
import os
import requests
import io
import datetime
import uuid

def predict_image(image_bytes):

    # 이미지 바이트를 BytesIO 객체로 변환
    image_data = io.BytesIO(image_bytes)
    
    # Flask API를 호출하여 예측 수행
    response = requests.post('http://11teamoracle.duckdns.org:9009/predict', files={'file': image_data})
    
    # 응답에서 예측 결과 추출
    predicted_class_index = response.json()['class_id']
    
    return predicted_class_index

def move_file(row, out_bucket, out_key):
    s3 = boto3.resource(
    's3',
    )   

    source_bucket = row['image_file'].split('/')[2]
    source_key = '/'.join(row['image_file'].split('/')[3:])
    id_slice = row['image_file'].split('/')[-1][:3]
    predicted_class_label = row["predicted_class_index"]
    destination_key = f'{out_key}/{id_slice}/{predicted_class_label}/{os.path.basename(source_key)}'
    copy_source = {
        'Bucket': source_bucket,
        'Key': source_key
    }
    s3.meta.client.copy(copy_source, out_bucket, destination_key)
    s3.Object(source_bucket, source_key).delete()


def move_files(iterator, output_uri):
    out_bucket = output_uri.split('/')[2]
    out_key = '/'.join(output_uri.split('/')[3:])
    for row in iterator:
        move_file(row, out_bucket, out_key)

def image_classification(data_source, output_uri, spark_result):

    now = datetime.datetime.now().strftime('%Y%m%d')
    file_uuid = uuid.uuid4()
    file_name = f'predicted_images_log/{now}/{file_uuid}'



    with SparkSession.builder.appName("Image_Classifier").getOrCreate() as spark:

        # 이미지 파일과 내용 읽기
        image_files = spark.read.format("binaryFile").load(data_source + "/*")
        # pandas_udf 함수로 등록
        predict_udf = spark.udf.register("predict_image", predict_image, StringType())
        # 예측 수행
        predicted_df = image_files.select('path', 'content').withColumnRenamed('path', 'image_file').withColumn("predicted_class_index", predict_udf(col("content")))
        
        # content 컬럼 제거
        predicted_df = predicted_df.drop('content')
        # 결과 DataFrame 저장
        predicted_df.write.mode('overwrite').parquet(f'{spark_result}/{file_name}')

        # 이미지 파일 이동
        predicted_df.rdd.foreachPartition(lambda iterator: move_files(iterator, output_uri))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source', help="The URI for your image data, like an S3 bucket location.")
    parser.add_argument('--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    parser.add_argument('--spark_result', help="The URI where spark_result is saved, like an S3 bucket location.")
    args = parser.parse_args()

    image_classification(args.data_source, args.output_uri, args.spark_result)
