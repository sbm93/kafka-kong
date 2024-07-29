import argparse
import logging
from src.ingest.kafka_producer import CDCKafkaProducer
from src.consume.opensearch_store import OpenSearchStore

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def ingest_data(file_path: str) -> None:
    """
    Ingest data from a file into Kafka.
    :param file_path: Path to the file containing CDC events.
    """
    producer = CDCKafkaProducer()
    try:
        producer.ingest_events(file_path)
    finally:
        producer.close()


def store_data() -> None:
    """Store the data from Kafka to OpenSearch."""
    store_object = OpenSearchStore()
    try:
        store_object.consume_and_save()
    finally:
        store_object.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CDC Event Ingestion and Consumer")
    parser.add_argument('--ingest', type=str, help='Path to the file containing CDC events for ingestion')
    parser.add_argument('--consume', action='store_true', help='consume and store the data from Kafka to OpenSearch')

    args = parser.parse_args()

    if args.ingest:
        logger.info(f"Starting ingestion from file: {args.ingest}")
        ingest_data(args.ingest)
    elif args.consume:
        logger.info("Starting storing the data from Kafka to OpenSearch")
        store_data()
    else:
        parser.print_help()
