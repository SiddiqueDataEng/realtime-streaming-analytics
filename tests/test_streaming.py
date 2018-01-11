"""
Unit tests for Streaming Processor
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.streaming_processor import StreamingProcessor


class TestStreamingProcessor(unittest.TestCase):
    """Test streaming processing functionality"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for tests"""
        cls.processor = StreamingProcessor("TestStreaming")
        cls.spark = cls.processor.spark
    
    @classmethod
    def tearDownClass(cls):
        """Stop Spark session"""
        cls.processor.stop()
    
    def test_spark_session_initialized(self):
        """Test Spark session is properly initialized"""
        self.assertIsNotNone(self.spark)
        self.assertEqual(self.spark.sparkContext.appName, "TestStreaming")
    
    def test_detect_anomalies(self):
        """Test anomaly detection"""
        # Create sample data
        data = [
            ("evt-1", "click", datetime.now(), "user-1", 50.0, 100, "success"),
            ("evt-2", "click", datetime.now(), "user-2", 150.0, 200, "success"),
            ("evt-3", "error", datetime.now(), "user-3", 500.0, 1500, "error")
        ]
        
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("user_id", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("latency_ms", StringType(), True),
            StructField("status", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        
        # Detect anomalies
        result = self.processor.detect_anomalies(df, "value", 100.0)
        
        self.assertIn("is_anomaly", result.columns)
        self.assertIn("anomaly_severity", result.columns)
        
        # Check that high values are flagged
        anomalies = result.filter(result.is_anomaly == True).count()
        self.assertGreater(anomalies, 0)


if __name__ == '__main__':
    unittest.main()
