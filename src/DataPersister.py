#!/usr/bin/env python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


def main():
    spark = SparkSession.builder.appName("LoadJob").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # read stream data from the kafka topic
    transformed_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transformed_topic")
        .option("startingOffsets", "latest")
        .load()
    )

    # define shcema for final data structure
    final_schema = StructType(
        [
            StructField("record_id", StringType(), True),
            StructField("value_upper", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("customer_id", TimestampType(), True),
            StructField("tariff_plan", TimestampType(), True),
            StructField("fraud_alert_type", TimestampType(), True),
            StructField("total_cost", TimestampType(), True),
        ]
    )

    parsed_final_df = (
        transformed_kafka_df.selectExpr("CAST(value AS STRING) as final_value")
        .select(from_json(col("final_value"), final_schema).alias("json_data"))
        .select("json_data.*")
    )

    # write stream data to phoenix
    def write_final_to_phoenix(batch_df, batch_id):
        if batch_df.count() > 0:
            (
                batch_df.write.format("phoenix")
                .mode("append")
                .option("table", "MY_FINAL_TABLE")
                .option("zkUrl", "localhost:2181:/hbase")
                .save()
            )

    query = (
        parsed_final_df.writeStream.foreachBatch(write_final_to_phoenix)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
