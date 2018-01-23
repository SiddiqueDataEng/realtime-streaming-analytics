"""
Event Producer for Testing
Generates sample events and sends to Kafka
"""

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventProducer:
    """Produces sample events to Kafka"""
    
    def __init__(self, kafka_servers: str = "localhost:9092"):
        """Initialize Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Kafka producer initialized: {kafka_servers}")
    
    def generate_event(self, event_type: str = None) -> dict:
        """Generate a sample event"""
        event_types = ["page_view", "click", "purchase", "error", "api_call"]
        
        event = {
            "event_id": f"evt-{random.randint(1000, 9999)}",
            "event_type": event_type or random.choice(event_types),
            "event_timestamp": datetime.now().isoformat(),
            "user_id": f"user-{random.randint(1, 100)}",
            "value": round(random.uniform(10, 1000), 2),
            "latency_ms": random.randint(50, 2000),
            "status": random.choice(["success"] * 9 + ["error"])
        }
        
        return event
    
    def produce_events(self, topic: str, num_events: int = 100, delay: float = 0.1):
        """
        Produce events to Kafka topic
        
        Args:
            topic: Kafka topic
            num_events: Number of events to produce
            delay: Delay between events (seconds)
        """
        try:
            logger.info(f"Producing {num_events} events to topic: {topic}")
            
            for i in range(num_events):
                event = self.generate_event()
                self.producer.send(topic, value=event)
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Produced {i + 1} events")
                
                time.sleep(delay)
            
            self.producer.flush()
            logger.info(f"Successfully produced {num_events} events")
        
        except Exception as e:
            logger.error(f"Error producing events: {e}")
            raise
    
    def close(self):
        """Close producer"""
        self.producer.close()
        logger.info("Producer closed")


if __name__ == "__main__":
    producer = EventProducer()
    
    try:
        # Produce 1000 events
        producer.produce_events("events", num_events=1000, delay=0.05)
    finally:
        producer.close()
