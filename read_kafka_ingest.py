from pyspark.sql import SparkSession
from pyspark.sql.functions import parse_json
import logging

# Configure Spark Session
spark = SparkSession.builder \
    .appName("KafkaReadExample") \
    .getOrCreate()

logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)  # Disable Kafka logs
logging.getLogger("org").setLevel(logging.ERROR)  # General Spark logs
logging.getLogger("akka").setLevel(logging.ERROR)  # Akka logs

# Define Kafka parameters
kafka_bootstrap_servers = "localhost:7092"  # Change this to your Kafka bootstrap servers
topic_name = "customers"  # Change this to your Kafka topic name
path = "/Users/sshah/IdeaProjects/poc-projects/lakehouse/data/delta-table"

# Read messages from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()


def process_batch_message(batch_df, batch_id):
    print("============")
    print(f"Batch ID: {batch_id}")

    if batch_df.count() > 0:  # Check if there are any records
        # Convert the Kafka value to a DataFrame with JSON strings
        json_df = batch_df.selectExpr("CAST(value AS STRING) as json_string")

        # Create a DataFrame similar to df1
        df2 = json_df.select(
            parse_json(json_df.json_string).alias("json_var")
        )

        # Write to Delta table in append mode
        df2.write \
            .format("delta") \
            .mode("append") \
            .save(path)
    else:
        print("No records to process.")

    print("============")
    print("\n")


# Write the stream to the console
query = kafka_df.writeStream \
    .foreachBatch(process_batch_message) \
    .outputMode("update") \
    .trigger(processingTime="1 minute") \
    .start()

# Await termination
query.awaitTermination()
