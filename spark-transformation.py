from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_expedia_data():
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("ExpediaTransformation") \
        .getOrCreate()

    expedia_data = read_expedia_data(spark_session)
    filtered_expedia = add_column_large_family(expedia_data)
    save_expedia_data(filtered_expedia)

def read_expedia_data(spark_session):
    return spark_session \
        .read \
        .format("avro") \
        .load("hdfs://host.docker.internal:9000/expedia")

def add_column_large_family(expedia_data):
    return expedia_data \
        .where("srch_children_cnt > 0") \
        .withColumn("large_family", col("srch_children_cnt") > 2)

def save_expedia_data(filtered_expedia)
    filtered_expedia \
        .write \
        .format("avro") \
        .save("hdfs://host.docker.internal:9000/transformExpediaFromAirflow")

if __name__ == '__main__':
    transform_expedia_data()
