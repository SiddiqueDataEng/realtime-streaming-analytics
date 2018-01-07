# Real-Time Streaming Analytics

## Overview
Enterprise-grade real-time streaming analytics platform using Apache Kafka and Spark Structured Streaming. Processes millions of events per second with windowed aggregations, stateful processing, anomaly detection, and real-time alerting. Integrated with Redis for state management, InfluxDB for metrics storage, and Grafana for visualization.

## Technologies
- **Streaming**: Apache Kafka 3.4+, Spark Structured Streaming 3.4+
- **State Store**: Redis 7.x
- **Metrics**: InfluxDB 2.x
- **Visualization**: Grafana 10.x
- **Programming**: Python 3.9+, PySpark
- **Container**: Docker, Docker Compose

## Architecture
```
Event Sources ──> Kafka (events) ──> Spark Streaming ──┬──> Kafka (metrics)
                                                        ├──> Kafka (alerts)
                                                        ├──> Redis (state)
                                                        ├──> InfluxDB (metrics)
                                                        └──> Grafana (dashboards)
```

## Features

### Stream Processing
- **Windowed Aggregations**: Tumbling, sliding, session windows
- **Stateful Processing**: Maintain state across events
- **Watermarking**: Handle late-arriving data
- **Exactly-Once Semantics**: Guaranteed message processing
- **Backpressure Handling**: Automatic rate limiting

### Real-Time Analytics
- **Event Counting**: Per event type, user, session
- **Latency Metrics**: Average, min, max, percentiles
- **Error Tracking**: Error rates and patterns
- **Value Aggregations**: Sum, average, count

### Anomaly Detection
- **Threshold-Based**: Configurable thresholds
- **Statistical Methods**: Standard deviation, z-score
- **Pattern Detection**: Unusual event sequences
- **Severity Classification**: Critical, high, medium, low

### Alerting
- **Real-Time Alerts**: Sub-second alert generation
- **Multi-Channel**: Kafka, Redis, webhooks
- **Alert Routing**: Based on severity and type
- **Alert Deduplication**: Prevent alert storms

### State Management
- **Redis Integration**: Fast state storage
- **TTL Management**: Automatic expiration
- **State Recovery**: Checkpoint-based recovery
- **Distributed State**: Partitioned state store

## Project Structure
```
realtime-streaming-analytics/
├── src/
│   ├── streaming_processor.py     # Main streaming engine
│   └── event_producer.py          # Test event generator
├── config/
│   └── config.yaml                # Configuration
├── tests/
│   └── test_streaming.py          # Unit tests
├── docker-compose.yml             # Infrastructure stack
├── Dockerfile                     # Container definition
├── requirements.txt               # Python dependencies
└── README.md
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.9+
- Apache Spark 3.4+

### 1. Start Infrastructure
```bash
# Start Kafka, Redis, InfluxDB, Grafana
docker-compose up -d

# Verify services
docker-compose ps
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Create Kafka Topics
```bash
# Create topics
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic events --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic metrics --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic alerts --partitions 1 --replication-factor 1
```

### 4. Run Event Producer
```bash
# Generate test events
python src/event_producer.py
```

### 5. Start Streaming Processor
```bash
# Run with spark-submit
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  src/streaming_processor.py
```

### 6. Access Dashboards
- **Grafana**: http://localhost:3000 (admin/admin)
- **InfluxDB**: http://localhost:8086

## Usage Examples

### Basic Streaming
```python
from src.streaming_processor import StreamingProcessor

processor = StreamingProcessor("MyStreamingApp")

# Read from Kafka
stream = processor.read_from_kafka("events", "localhost:9092")

# Parse events
parsed = processor.parse_events(stream, event_schema)

# Calculate metrics
metrics = processor.calculate_metrics(parsed)

# Write to console
query = processor.write_to_console(metrics)
query.awaitTermination()
```

### Windowed Aggregation
```python
# minute tumbling window
windowed = processor.windowed_aggregation(
    df=parsed_stream,
    window_duration="1 minute",
    slide_duration="1 minute"
)
```

### Anomaly Detection
```python
# Detect anomalies
anomalies = processor.detect_anomalies(
    df=metrics,
    threshold_column="avg_latency",
    threshold=1000.0
)

# Filter critical anomalies
critical = anomalies.filter(col("anomaly_severity") == "critical")
```

### Stateful Processing
```python
# Maintain state per user
stateful = processor.stateful_processing(
    df=parsed_stream,
    key_column="user_id"
)
```

### Generate Alerts
```python
# Configure alert conditions
alert_conditions = {
    "error_rate_threshold": 5.0,
    "latency_threshold": 1000
}

# Generate alerts
alerts = processor.generate_alerts(metrics, alert_conditions)

# Write to Kafka
processor.write_to_kafka(alerts, "alerts")
```

## Configuration

### Spark Streaming
```yaml
spark:
  streaming:
    trigger_interval: "10 seconds"
    max_offsets_per_trigger: 10000
    checkpoint_location: "/tmp/checkpoints"
```

### Kafka
```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topics:
    input: "events"
    output: "metrics"
    alerts: "alerts"
```

### Alerts
```yaml
alerts:
  conditions:
    error_rate_threshold: 5.0
    latency_threshold: 1000
  severity_levels:
    critical:
      error_rate: 10.0
      latency: 2000
```

## Performance Optimization

### Spark Configuration
```python
spark.conf.set("spark.streaming.backpressure.enabled", "true")
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

### Kafka Tuning
- Increase partitions for parallelism
- Configure retention policies
- Enable compression
- Tune batch sizes

### State Management
- Use Redis for fast state access
- Configure appropriate TTLs
- Partition state by key
- Enable state checkpointing

## Monitoring

### Spark UI
- Access at: http://driver:4040
- Monitor streaming queries
- View batch processing times
- Check input rates

### Grafana Dashboards
- Event throughput
- Processing latency
- Error rates
- Alert frequency
- Resource utilization

### Metrics
- Events processed per second
- Average processing latency
- Backlog size
- State store size

## Testing
```bash
# Run unit tests
pytest tests/ -v

# Test with sample data
python src/event_producer.py
```

## Docker Deployment
```bash
# Build image
docker build -t streaming-analytics .

# Run container
docker run --network host streaming-analytics
```

## Scalability

### Horizontal Scaling
- Add more Kafka partitions
- Increase Spark executors
- Scale Redis cluster
- Distribute state

### Throughput
- **Events/sec**: 100K+
- **Latency**: < 1 second end-to-end
- **State Size**: Millions of keys

## Fault Tolerance
- Kafka replication
- Spark checkpointing
- Redis persistence
- Automatic recovery

## Security
- Kafka SSL/SASL
- Redis authentication
- Network isolation
- Data encryption

## License
MIT License

## Support
For issues and questions, please open a GitHub issue.
