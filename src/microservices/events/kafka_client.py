import json
import logging
from datetime import datetime
from typing import Dict, Any
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import threading

logger = logging.getLogger(__name__)


class KafkaClient:
    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumers = {}
        self.running = False
        self.threads = []
        
    def create_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"Kafka producer created for {self.bootstrap_servers}")
            return self.producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {str(e)}")
            raise
    
    def produce_event(self, topic: str, event_data: Dict[str, Any]):
        if not self.producer:
            self.create_producer()
        
        try:
            event_data['timestamp'] = datetime.utcnow().isoformat()
            
            future = self.producer.send(topic, event_data)
            
            result = future.get(timeout=10)
            logger.info(f"Event sent to topic {topic}: {event_data}")
            return result
        except KafkaError as e:
            logger.error(f"Failed to send event to Kafka: {str(e)}")
            raise
    
    def create_consumer(self, topic: str, group_id: str = "events-service-group"):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            self.consumers[topic] = consumer
            logger.info(f"Kafka consumer created for topic {topic}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer for topic {topic}: {str(e)}")
            raise
    
    def process_event(self, event: Dict[str, Any]):
        logger.info(f"Processing event: {event}")
        
        event_type = event.get('event_type', 'unknown')
        
        if 'movie' in event_type.lower():
            logger.info(f"Movie event processed: {event.get('title', 'N/A')}")
        elif 'user' in event_type.lower():
            logger.info(f"User event processed: user_id={event.get('user_id', 'N/A')}")
        elif 'payment' in event_type.lower():
            logger.info(f"Payment event processed: amount={event.get('amount', 'N/A')}")
    
    def consume_topic(self, topic: str):
        consumer = self.create_consumer(topic)
        
        logger.info(f"Starting to consume from topic {topic}")
        for message in consumer:
            try:
                event_data = message.value
                logger.info(f"Received event from topic {topic}: {event_data}")
                self.process_event(event_data)
            except Exception as e:
                logger.error(f"Error processing message from topic {topic}: {str(e)}")
    
    def start_consumers(self, topics: list):
        self.running = True
        
        for topic in topics:
            thread = threading.Thread(
                target=self.consume_topic,
                args=(topic,),
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
            logger.info(f"Started consumer thread for topic {topic}")
    
    def stop(self):
        self.running = False
        
        for consumer in self.consumers.values():
            consumer.close()
        
        if self.producer:
            self.producer.close()
        
        for thread in self.threads:
            thread.join(timeout=5)
        
        logger.info("Kafka client stopped")

kafka_client = None

def get_kafka_client():
    global kafka_client
    if kafka_client is None:
        kafka_client = KafkaClient()
    return kafka_client