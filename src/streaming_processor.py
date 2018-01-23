"""
Real-Time Streaming Analytics with Kafka and Spark Structured Streaming
Processes live events for real-time metrics, alerts, and analytics
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window, count, sum as spark_sum,
    avg, max as spark_max, min as spark_min, current_timestamp, expr,
    when, lit, concat, explode, split, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, BooleanType, LongType
)
from typing import Dict, List, Optional
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StreamingProcessor:
    """Real-time streaming analytics processor"""
    
    def __init__(self, app_name: str = "StreamingAnalytics", config: Optional[Dict] = None):
        """Initialize Spark Streaming session"""
        self.config = config or {}
        
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark Streaming session initialized: {app_name}")
    
    def read_from_kafka(self, topic: str, kafka_servers: str = "localhost:9092") -> DataFrame:
        """
        Read streaming data from Kafka
        
        Args:
            topic: Kafka topic name
            kafka_servers: Kafka bootstrap servers
        
        Returns:
            Streaming DataFrame
        """
        try:
            logger.info(f"Reading from Kafka topic: {topic}")
            
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info(f"Connected to Kafka topic: {topic}")
            return df
        
        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            raise
    
    def parse_events(self, df: DataFrame, schema: StructType) -> DataFrame:
        """
        Parse JSON events from Kafka
        
        Args:
            df: Raw Kafka DataFrame
            schema: Event schema
        
        Returns:
            Parsed DataFrame
        """
        try:
            logger.info("Parsing events")
            
            # Parse JSON from Kafka value
            parsed_df = df.select(
                col("timestamp").alias("kafka_timestamp"),
                from_json(col("value").cast("string"), schema).alias("data")
            ).select("kafka_timestamp", "data.*")
            
            # Add processing timestamp
            parsed_df = parsed_df.withColumn("processing_time", current_timestamp())
            
            return parsed_df
        
        except Exception as e:
            logger.error(f"Error parsing events: {e}")
            raise
    
    def windowed_aggregation(self, df: DataFrame, window_duration: str = "1 minute",
                            slide_duration: str = "30 seconds") -> DataFrame:
        """
        Perform windowed aggregations
        
        Args:
            df: Streaming DataFrame
            window_duration: Window size
            slide_duration: Slide interval
        
        Returns:
            Aggregated DataFrame
        """
        try:
            logger.info(f"Performing windowed aggregation: {window_duration}")
            
            # Aggregate by time window
            aggregated = df.groupBy(
                window(col("event_timestamp"), window_duration, slide_duration),
                col("event_type")
            ).agg(
                count("*").alias("event_count"),
                avg("value").alias("avg_value"),
                spark_max("value").alias("max_value"),
                spark_min("value").alias("min_value"),
                spark_sum("value").alias("total_value")
            )
            
            # Extract window boundaries
            aggregated = aggregated.withColumn("window_start", col("window.start")) \
                                   .withColumn("window_end", col("window.end")) \
                                   .drop("window")
            
            return aggregated
        
        except Exception as e:
            logger.error(f"Error in windowed aggregation: {e}")
            raise
    
    def detect_anomalies(self, df: DataFrame, threshold_column: str = "value",
                        threshold: float = 100.0) -> DataFrame:
        """
        Detect anomalies in real-time
        
        Args:
            df: Streaming DataFrame
            threshold_column: Column to check
            threshold: Anomaly threshold
        
        Returns:
            DataFrame with anomaly flags
        """
        try:
            logger.info(f"Detecting anomalies with threshold: {threshold}")
            
            # Add anomaly flag
            df_with_anomalies = df.withColumn(
                "is_anomaly",
                when(col(threshold_column) > threshold, True).otherwise(False)
            ).withColumn(
                "anomaly_severity",
                when(col(threshold_column) > threshold * 2, "critical")
                .when(col(threshold_column) > threshold * 1.5, "high")
                .when(col(threshold_column) > threshold, "medium")
                .otherwise("normal")
            )
            
            return df_with_anomalies
        
        except Exception as e:
            logger.error(f"Error detecting anomalies: {e}")
            raise
    
    def stateful_processing(self, df: DataFrame, key_column: str = "user_id") -> DataFrame:
        """
        Perform stateful processing with watermarking
        
        Args:
            df: Streaming DataFrame
            key_column: Key for state management
        
        Returns:
            DataFrame with state
        """
        try:
            logger.info(f"Performing stateful processing on: {key_column}")
            
            # Add watermark for late data handling
            df_with_watermark = df.withWatermark("event_timestamp", "10 minutes")
            
            # Aggregate by key with state
            stateful_df = df_with_watermark.groupBy(
                col(key_column),
                window(col("event_timestamp"), "5 minutes")
            ).agg(
                count("*").alias("event_count"),
                spark_sum("value").alias("total_value"),
                avg("value").alias("avg_value")
            )
            
            return stateful_df
        
        except Exception as e:
            logger.error(f"Error in stateful processing: {e}")
            raise
    
    def calculate_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate real-time metrics
        
        Args:
            df: Streaming DataFrame
        
        Returns:
            DataFrame with calculated metrics
        """
        try:
            logger.info("Calculating real-time metrics")
            
            # Calculate various metrics
            metrics = df.groupBy(
                window(col("event_timestamp"), "1 minute"),
                col("event_type")
            ).agg(
                count("*").alias("count"),
                avg("latency_ms").alias("avg_latency"),
                spark_max("latency_ms").alias("max_latency"),
                spark_min("latency_ms").alias("min_latency"),
                spark_sum(when(col("status") == "error", 1).otherwise(0)).alias("error_count")
            )
            
            # Calculate error rate
            metrics = metrics.withColumn(
                "error_rate",
                (col("error_count") / col("count") * 100)
            )
            
            return metrics
        
        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            raise
    
    def generate_alerts(self, df: DataFrame, alert_conditions: Dict) -> DataFrame:
        """
        Generate real-time alerts based on conditions
        
        Args:
            df: Streaming DataFrame
            alert_conditions: Dictionary of alert conditions
        
        Returns:
            DataFrame with alerts
        """
        try:
            logger.info("Generating alerts")
            
            # Apply alert conditions
            alerts = df.filter(
                (col("error_rate") > alert_conditions.get("error_rate_threshold", 5.0)) |
                (col("avg_latency") > alert_conditions.get("latency_threshold", 1000)) |
                (col("is_anomaly") == True)
            )
            
            # Add alert metadata
            alerts = alerts.withColumn(
                "alert_type",
                when(col("error_rate") > alert_conditions.get("error_rate_threshold", 5.0), "high_error_rate")
                .when(col("avg_latency") > alert_conditions.get("latency_threshold", 1000), "high_latency")
                .when(col("is_anomaly") == True, "anomaly_detected")
                .otherwise("unknown")
            ).withColumn(
                "alert_timestamp",
                current_timestamp()
            ).withColumn(
                "alert_severity",
                when(col("error_rate") > 10, "critical")
                .when(col("avg_latency") > 2000, "critical")
                .otherwise("warning")
            )
            
            return alerts
        
        except Exception as e:
            logger.error(f"Error generating alerts: {e}")
            raise
    
    def write_to_kafka(self, df: DataFrame, topic: str, kafka_servers: str = "localhost:9092"):
        """
        Write streaming results to Kafka
        
        Args:
            df: Streaming DataFrame
            topic: Output Kafka topic
            kafka_servers: Kafka bootstrap servers
        """
        try:
            logger.info(f"Writing to Kafka topic: {topic}")
            
            # Convert to JSON
            kafka_df = df.select(
                to_json(struct("*")).alias("value")
            )
            
            # Write to Kafka
            query = kafka_df.writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_servers) \
                .option("topic", topic) \
                .option("checkpointLocation", f"/tmp/checkpoints/{topic}") \
                .outputMode("append") \
                .start()
            
            return query
        
        except Exception as e:
            logger.error(f"Error writing to Kafka: {e}")
            raise
    
    def write_to_console(self, df: DataFrame, output_mode: str = "append"):
        """
        Write streaming results to console (for debugging)
        
        Args:
            df: Streaming DataFrame
            output_mode: Output mode (append, complete, update)
        """
        try:
            logger.info("Writing to console")
            
            query = df.writeStream \
                .format("console") \
                .outputMode(output_mode) \
                .option("truncate", "false") \
                .start()
            
            return query
        
        except Exception as e:
            logger.error(f"Error writing to console: {e}")
            raise
    
    def write_to_redis(self, df: DataFrame, redis_host: str = "localhost", redis_port: int = 6379):
        """
        Write streaming results to Redis
        
        Args:
            df: Streaming DataFrame
            redis_host: Redis host
            redis_port: Redis port
        """
        try:
            logger.info(f"Writing to Redis: {redis_host}:{redis_port}")
            
            def write_to_redis_batch(batch_df, batch_id):
                """Write each batch to Redis"""
                import redis
                r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
                
                for row in batch_df.collect():
                    key = f"metrics:{row['event_type']}:{row['window_start']}"
                    value = json.dumps(row.asDict())
                    r.setex(key, 3600, value)  # Expire after 1 hour
                
                logger.info(f"Batch {batch_id} written to Redis")
            
            query = df.writeStream \
                .foreachBatch(write_to_redis_batch) \
                .outputMode("update") \
                .start()
            
            return query
        
        except Exception as e:
            logger.error(f"Error writing to Redis: {e}")
            raise
    
    def run_streaming_pipeline(self, kafka_servers: str, input_topic: str,
                               output_topic: str, alert_topic: str):
        """
        Run complete streaming analytics pipeline
        
        Args:
            kafka_servers: Kafka bootstrap servers
            input_topic: Input Kafka topic
            output_topic: Output Kafka topic for metrics
            alert_topic: Output Kafka topic for alerts
        """
        try:
            logger.info("Starting streaming analytics pipeline")
            
            # Define event schema
            event_schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_timestamp", TimestampType(), True),
                StructField("user_id", StringType(), True),
                StructField("value", DoubleType(), True),
                StructField("latency_ms", IntegerType(), True),
                StructField("status", StringType(), True)
            ])
            
            # Read from Kafka
            raw_stream = self.read_from_kafka(input_topic, kafka_servers)
            
            # Parse events
            parsed_stream = self.parse_events(raw_stream, event_schema)
            
            # Calculate metrics
            metrics = self.calculate_metrics(parsed_stream)
            
            # Detect anomalies
            metrics_with_anomalies = self.detect_anomalies(metrics, "avg_latency", 500.0)
            
            # Generate alerts
            alert_conditions = {
                "error_rate_threshold": 5.0,
                "latency_threshold": 1000
            }
            alerts = self.generate_alerts(metrics_with_anomalies, alert_conditions)
            
            # Write metrics to Kafka
            metrics_query = self.write_to_kafka(metrics_with_anomalies, output_topic, kafka_servers)
            
            # Write alerts to Kafka
            alerts_query = self.write_to_kafka(alerts, alert_topic, kafka_servers)
            
            # Write to console for monitoring
            console_query = self.write_to_console(metrics_with_anomalies, "update")
            
            logger.info("Streaming pipeline started successfully")
            
            # Wait for termination
            self.spark.streams.awaitAnyTermination()
        
        except Exception as e:
            logger.error(f"Error in streaming pipeline: {e}")
            raise
    
    def stop(self):
        """Stop all streaming queries"""
        for query in self.spark.streams.active:
            query.stop()
        self.spark.stop()
        logger.info("Streaming queries stopped")


if __name__ == "__main__":
    # Example usage
    processor = StreamingProcessor("RealTimeAnalytics")
    
    try:
        # Run streaming pipeline
        processor.run_streaming_pipeline(
            kafka_servers="localhost:9092",
            input_topic="events",
            output_topic="metrics",
            alert_topic="alerts"
        )
    
    except KeyboardInterrupt:
        logger.info("Stopping streaming pipeline...")
        processor.stop()
