import json
import logging
from typing import Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from opensearchpy import OpenSearch, OpenSearchException
from src.utils.config import config

logger = logging.getLogger(__name__)


class OpenSearchStore:
    """
    A class to consume CDC events from Kafka and store them to OpenSearch
    using: SCD Type 2.
    """

    def __init__(self):
        """Initialisation"""
        self.consumer = self._init_kafka_consumer()
        self.opensearch_client = self._init_opensearch_client()
        self.index_name = "cdc"

    @staticmethod
    def _init_kafka_consumer() -> KafkaConsumer:
        """Return Kafka consumer."""
        return KafkaConsumer(
            config.get_nested('kafka', 'topic', default='cdc-events'),
            bootstrap_servers=config.get_nested('kafka', 'bootstrap_servers', default=['localhost:9092']),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='opensearch-store',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    @staticmethod
    def _init_opensearch_client() -> OpenSearch:
        """Return OpenSearch client."""
        return OpenSearch(
            hosts=[{'host': config.get_nested('opensearch', 'host', default='localhost'),
                    'port': config.get_nested('opensearch', 'port', default=9200)}],
            http_compress=config.get_nested('opensearch', 'http_compress', default=True),
            use_ssl=config.get_nested('opensearch', 'use_ssl', default=False),
            verify_certs=config.get_nested('opensearch', 'verify_certs', default=False),
            ssl_show_warn=config.get_nested('opensearch', 'ssl_show_warn', default=False)
        )

    def process_event(self, event: Dict[str, Any]) -> None:
        """
        Process a single CDC event and store it to OpenSearch using SCD Type 2.
        :param event: The CDC event to be processed.
        """
        if event.get('after') and event['after'].get('value') and event['after']['value'].get('object'):
            object_data = event['after']['value']['object']
            doc_id = object_data.get('id')

            if doc_id:
                try:
                    self.opensearch_client.index(
                        index=self.index_name,
                        body=object_data,
                        id=doc_id,
                        refresh=True
                    )
                    logger.info(f"Indexed document {doc_id} in index {self.index_name}")
                except OpenSearchException as e:
                    logger.error(f"Failed to index document {doc_id} in index {self.index_name}. Error: {str(e)}")
            else:
                logger.warning(f"Skipped event due to missing id: {object_data}")

    def consume_and_save(self) -> None:
        """Consume messages from Kafka and store them to OpenSearch."""
        try:
            for message in self.consumer:
                self.process_event(message.value)
        except KafkaError as e:
            logger.error(f"Kafka error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
        finally:
            self.consumer.close()
            logger.info("Kafka consumer closed.")

    def close(self) -> None:
        """Close the Kafka consumer and OpenSearch client."""
        self.consumer.close()
        self.opensearch_client.close()
        logger.info("OpenSearch closed.")
