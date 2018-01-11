FROM apache/spark-py:v3.4.0

USER root

# Install dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    redis==4.6.0 \
    influxdb-client==1.37.0 \
    PyYAML==6.0 \
    pytest==7.4.0

# Copy application code
COPY src/ /app/src/
COPY config/ /app/config/

WORKDIR /app

ENV PYTHONPATH=/app
ENV SPARK_HOME=/opt/spark

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0", "src/streaming_processor.py"]
