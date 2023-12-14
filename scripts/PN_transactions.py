import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
from datetime import datetime 

def test_partition(data_source, output_uri):
    

    with SparkSession.builder.appName("transactions").getOrCreate() as spark:
        
        # CSV 파일 읽어오기
        df = spark.read.parquet(data_source, header=True, inferSchema=True)

        # DataFrame 출력하기
        #df.show()


        # '년', '월', '일' 컬럼 추가
        df = df.withColumn('year', year(df['t_dat']))
        df = df.withColumn('month', month(df['t_dat']))
        #df = df.withColumn('day', dayofmonth(df['t_dat']))


        df.write \
        .option("maxRecordsPerFile", 100000) \
        .partitionBy('year', 'month') \
        .mode("append") \
        .parquet(output_uri)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you PARQUET restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    test_partition(args.data_source, args.output_uri)
			