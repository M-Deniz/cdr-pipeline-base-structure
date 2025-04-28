#!/usr/bin/env python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


def main():
    spark = SparkSession.builder.appName("TransformJob").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # read streaming data from event topic
    events_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka-cluster1:9092")
        .option("subscribe", "events_topic")
        .option("startingOffsets", "latest")
        .load()
    )

    # parse the event json
    event_schema = StructType(
        [
            StructField("record_id", StringType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )

    parsed_events_df = (
        events_kafka_df.selectExpr("CAST(value AS STRING) as event_value")
        .select(from_json(col("event_value"), event_schema).alias("event_json"))
        .select("event_json.*")
    )

    # for each record_id, read from phoenix/hbase to get the relevant data
    def transform_and_write_to_kafka(batch_df, batch_id):
        if batch_df.count() == 0:
            return

        # collect unique record_ids
        record_ids = [
            row["record_id"]
            for row in batch_df.select("record_id").distinct().collect()
        ]

        # read data from Phoenix
        phoenix_df = (
            spark.read.format("phoenix")
            .option("table", "CDR_PREPROCESSED")
            .option("zkUrl", "localhost:2181:/hbase")
            .load()
        )

        filtered_phoenix_df = phoenix_df.filter(col("record_id").isin(record_ids))

        customer_profiles_df = (
            spark.read.format("phoenix")
            .option("table", "CUSTOMER_PROFILES")
            .option("zkUrl", "hbase-cluster:2181:/hbase")
            .load()
        )

        filtered_customer_profiles_df = customer_profiles_df.filter(
            col("account_status") == "Active"
        )

        call_tariffs_df = (
            spark.read.format("phoenix")
            .option("table", "CALL_TARIFFS")
            .option("zkUrl", "hbase-cluster:2181:/hbase")
            .load()
        )

        filtered_call_tariffs_df = call_tariffs_df.filter(
            col("tariff_plan").isNotNull()
        )

        fraud_alerts_df = (
            spark.read.format("phoenix")
            .option("table", "FRAUD_ALERTS")
            .option("zkUrl", "hbase-cluster:2181:/hbase")
            .load()
        )

        filtered_fraud_alerts_df = fraud_alerts_df.filter(col("risk_score") > 50)

        # apply transformations and joins to phoenix data
        transformed_df = (
            filtered_phoenix_df.withColumn(
                "value_upper", col("value").cast("string").upper()
            )
            .join(filtered_customer_profiles_df, "customer_id", "left")
            .join(filtered_call_tariffs_df, "tariff_plan", "left")
            .join(filtered_fraud_alerts_df, "call_id", "left")
        )

        # write transformed data to the kafka
        output_df = transformed_df.select(
            to_json(
                col("record_id"),
                col("value_upper"),
                col("event_time"),
                col("customer_id"),
                col("tariff_plan"),
                col("fraud_alert_type"),
                col("total_cost"),
            ).alias("value")
        )

        (
            output_df.write.format("kafka")
            .option("kafka.bootstrap.servers", "kafka-cluster1:9092")
            .option("topic", "transformed_topic")
            .save()
        )

    # for each batch sink the process in micro batches
    query = (
        parsed_events_df.writeStream.foreachBatch(transform_and_write_to_kafka)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
