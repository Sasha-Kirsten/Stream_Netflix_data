
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from confluent_kafka import Consumer 

#1. Set up the PySpark session
# def create_spark_session():
#     spark = SparkSession.builder \
#         .appName("Kafka_S3_Streaming") \
#         .getOrCreate()
#     return spark


# #2. Configure the Kafka consumer using confluent_kafka library
# def create_kafka_consumer():
#     conf = {
#         'bootstrap.servers': "localhost:9092",
#         'group.id': "spark-streaming",
#         'auto.offset.reset': "earliest"
#     }
#     consumer = Consumer(conf)
#     consumer.subscribe(['netflix-steam-data']) #Replace with your Kafka topic
#     return consumer

# #3. Define the streaming soure from Kafka in PySpark
# def read_kafka_stream(spark):
#     kafka_df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrp.servers", "localhost:9092") \
#         .option("subscribe", "streamTopic") \
#         .option("startingOffsets", "earliest") \
#         .load()
    
#     #Convert the Kafka messages from binary to string
#     kafka_df = kafka_df.selectExpr("CAST(value AS STRING)", "CAST(value AS STRING)")
#     return kafka_df

# # 4. Define the transformation logic
# def transform_data(kafka_df):
#     # Extract data fields from the 'value' column (assuming it's a JSON string)
#     json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_data")

#     # Here you would parse the JSON structure and select the relevant columns
#     # For example, assuming columns 'watchfrequency' and 'etag' exist
#     transformed_df = json_df.withColumn("impression",
#                                         when(col("watchfrequency") >= 5, "favorite")
#                                         .when((col("watchfrequency") >= 3) & (col("watchfrequency") < 5), "like")
#                                         .otherwise("neutral")) \
#                             .drop("etag")  # Drop the 'etag' column
#     return transformed_df

# # 5. Write the transformed DataFrame to Amazon S3 in Parquet format
# def write_to_s3(transformed_df):
#     query = transformed_df.writeStream \
#         .format("parquet") \
#         .option("path", "s3a://your-s3-bucket/path/") \
#         .option("checkpointLocation", "s3a://your-s3-bucket/checkpoints/") \
#         .start()

#     query.awaitTermination()

# # Main function to tie everything together
# if __name__ == "__main__":
#     # 1. Create Spark session
#     spark = create_spark_session()

#     # 2. Read Kafka stream as DataFrame
#     kafka_stream_df = read_kafka_stream(spark)

#     # 3. Transform the data
#     transformed_df = transform_data(kafka_stream_df)

#     # 4. Write the data to S3
#     write_to_s3(transformed_df)




#1. Set up the PySpark session with S3 configuration
def create_spark_session():
    spark = SparkSession.builder \
        .appName("Kafka_S3_Streaming") \
        .config("spark.hadoop.fs.s3a.access.key", "ACCESS_KEY") \
        .config("spark.hadoop.fs.s3a.secret.key", "SECRET_KEY") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()
    return spark

#2. Define schema for the Kafka stream data
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("location", StringType()) \
    .add("channel_id", IntegerType()) \
    .add("genre", StringType()) \
    .add("last_activity_timestamp", StringType()) \
    .add("title", StringType()) \
    .add("watch_frequency", IntegerType()) \
    .add("etag", StringType())

#3. Write data to DynamoDB
def write_to_dynamodb(batch_df, bathc_id):
    dynamdb = bato3.resource('dynamodb', region_name='eu-central-1')
    table = dynamodb.Table('netflix_data')

    for row in batch_df.collect():
        table.put_item(
            Item={
                'user_id': row.user_id,
                'location': row.location,
                'channel_id': row.channel_id,
                'genre': row.genre,
                'last_activity_timestamp': row.last_activity_timestamp,
                'title': row.title,
                'watch_frequency': row.watch_frequency,
                'etag': row.etag
            }
        )

#4. Main ETL logic
def process_kafka_stream():
    #Create Spark session
    spark = create_spark_session()

    #Read data from Kafka stream
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "netflix-steam-data") \
        .option("startingOffsets", "earliest") \
        .load()
    
    #Convert the Kafka messages from binary to string
    kafka_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")

    #Parse the Kafka message JSON into a DataFrame
    parsed_df = kafka_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

    #Add transformation logic: add 'impression' column based on 'watch_frequency'
    transformed_df = parsed_df.withColumn('impression',
        when(col("watch_frequency") < 3, "neutral")
        .when((col("watch_frequency") >= 3) & (col("watch_frequency") < 10), "like")
        .otherwise("favorite")
    ).drop("etag")  # Drop the 'etag' column

    #Write to S3 in Parquet format
    s3_write_query = transformed_df.writeStream \
        .format("parquet") \
        .option("path", "s3://arn:aws:s3:eu-central-1:194086475033:accesspoint/thefirstaccesspoint") \
        .option("checkpointLocation", "s3://arn:aws:s3:eu-central-1:194086475033:accesspoint/thefirstaccesspoint") \
        .start()

    #Write to DynamoDB
    dynamodb_write_query = transformed_df.writeStream \
        .foreachBatch(write_to_dynamodb) \
        .outputModel("update") \
        .option("checkpointLocation", "s3://arn:aws:s3:eu-central-1:194086475033:accesspoint/thefirstaccesspoint") \
        .start()
    
    #Wait for the termiantion queries to finish
    s3_write_query.awaitTermination()
    dynamodb_write_query.awaitTermination()

if __name__ == "__main__":
    process_kafka_stream()
        