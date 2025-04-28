#!/usr/bin/env python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


def main():
    # create spark session
    spark = SparkSession.builder.appName("IngestionJob").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # define schema for raw ingested kafka data
    raw_schema = StructType(
        [
            StructField("record_id", StringType(), True),
            StructField("value", StringType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )

    # read streaming data from input topic
    input_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka-cluster1:9092")
        .option("subscribe", "input_topic")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = input_kafka_df.selectExpr("CAST(value AS STRING) as message_value")

    # parese kafka message to json
    cleaned_df = (
        parsed_df.select(from_json(col("message_value"), raw_schema).alias("json_data"))
        .select("json_data.*")
        .filter(col("record_id").isNotNull())
        .withColumn("value", col("value"))
    )

    # write batch data to phoenix
    def write_to_phoenix(batch_df, batch_id):
        if batch_df.count() > 0:
            (
                batch_df.write.format("phoenix")
                .mode("overwrite")
                .option("table", "CDR_PREPROCESSED")
                .option("zkUrl", "hbase-cluster:2181:/hbase")
                .save()
            )

    # create event message in json
    event_messages_df = cleaned_df.select(
        to_json(struct(col("record_id"), col("event_time"))).alias("value")
    )

    # write to next kafka topic
    query_to_event_topic = (
        event_messages_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka-cluster1:9092")
        .option("topic", "events_topic")
        .option("checkpointLocation", "/tmp/checkpoints/ingestion_to_kafka")
        .start()
    )

    query_to_phoenix.awaitTermination()
    query_to_event_topic.awaitTermination()


if __name__ == "__main__":
    main()
