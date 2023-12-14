import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
from datetime import datetime 

def test_partition(data_source, output_uri):
    
    with SparkSession.builder.appName("Test_customers").getOrCreate() as spark:
                # CSV 파일 읽어오기
        df = spark.read.csv(data_source, header=True, inferSchema=True)

        # DataFrame 출력하기
        #df.show()

        df.write \
        .option("maxRecordsPerFile", 100000) \
        .partitionBy('age') \
        .mode("append") \
        .parquet(output_uri)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    test_partition(args.data_source, args.output_uri)
			