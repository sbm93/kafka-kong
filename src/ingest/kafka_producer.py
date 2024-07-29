import json
import logging
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
from src.utils.config import config
from time import sleep

logger = logging.getLogger(__name__)

class CDCKafkaProducer:
    """
    A Kafka producer for ingesting CDC events into a Kafka topic.
    """
    def __init__(self):
        """Initialize the Kafka producer with configuration from the config file."""
        self.topic = config.get_nested('kafka', 'topic', default='cdc-events')
        self.producer = KafkaProducer(
            bootstrap_servers=config.get_nested('kafka', 'bootstrap_servers', default=['localhost:9092']),
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            acks=config.get_nested('kafka', 'acks', default='all'),
            retries=config.get_nested('kafka', 'retries', default=3),
            max_in_flight_requests_per_connection=config.get_nested('kafka', 'max_in_flight_requests_per_connection', default=1)
        )

    def send_event(self, event: Dict[str, Any]) -> None:
        """
        Send a single CDC event to the Kafka topic.

        :param event: The CDC event to be sent.
        """
        try:
            future = self.producer.send(self.topic, event)
            future.get(timeout=10)  # Wait for the send to complete
            logger.info(f"Successfully sent event: {event.get('after', {}).get('key')}")
        except KafkaError as e:
            logger.error(f"Failed to send event: {event.get('after', {}).get('key')}. Error: {str(e)}")

    def ingest_events(self, file_path: str) -> None:
        """
        Ingest CDC events from a file into the Kafka topic.

        :param file_path: Path to the file containing CDC events.
        """
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    event = json.loads(line)
                    self.send_event(event)
                    sleep(1)

        except IOError as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {str(e)}")
        finally:
            self.producer.flush()
            logger.info("All events have been ingested into Kafka.")

    def close(self) -> None:
        """Close the Kafka producer."""
        self.producer.close()
        logger.info("Kafka producer closed.")