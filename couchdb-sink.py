import json
import time
from kafka import KafkaConsumer
import couchdb
import os
from dotenv import load_dotenv
import urllib.parse
import logging
import sys
import re


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")  
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
APP_NAME = os.getenv("APP_NAME")

COUCHDB_IP = os.getenv("COUCHDB_IP")
USER_NAME = os.getenv("USER_NAME")
PASSWORD = os.getenv("PASSWORD")
DB_NAME = os.getenv("DB_NAME")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")
KEY_FILTER = os.getenv("KEY_FILTER", "").strip()  # can be a regex pattern like ^sensor\..*_weights$

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,  
    format=f'{{"app_name": "{APP_NAME}", "timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}}',
    datefmt='%Y-%m-%dT%H:%M:%S'
)


def main():
    quoted_password = urllib.parse.quote(PASSWORD)
    couchdb_url = f'http://{USER_NAME}:{quoted_password}@{COUCHDB_IP}:5984'

    try:
        couch = couchdb.Server(couchdb_url)
    except Exception as e:
        logging.error("Failed to connect to CouchDB.", exc_info=True)
        sys.exit(1)

    if DB_NAME not in couch:
        logging.error(f"Database '{DB_NAME}' does not exist in CouchDB.")
        raise ValueError(f"Database '{DB_NAME}' does not exist in CouchDB.")

    db = couch[DB_NAME]

    consumer = None

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            group_id=APP_NAME,
            auto_offset_reset=AUTO_OFFSET_RESET,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
    except Exception as e:
        logging.error("Failed to create Kafka consumer.", exc_info=True)
        sys.exit(1)

    logging.info(f"Consumer is running. Listening for messages... KEY_FILTER='{KEY_FILTER or '[none]'}'")

    regex_filter = None
    if KEY_FILTER:
        try:
            regex_filter = re.compile(KEY_FILTER)
        except re.error as e:
            logging.error(f"Invalid regex in KEY_FILTER: {e}")
            sys.exit(1)

    try:
        for message in consumer:
            key_str = message.key.decode("utf-8") if message.key else None
            doc = message.value
            doc["kafka-key"] = key_str

            # Match by regex if provided
            if regex_filter is None or (key_str and regex_filter.search(key_str)):
                try:
                    db.save(doc)
                    logging.info(f"Saved to CouchDB: {doc}")
                except Exception as e:
                    logging.error("Failed to save document to CouchDB.", exc_info=True)
                    continue
            else:
                logging.info(f"Key ({key_str}) did not match regex filter: {KEY_FILTER}")
                continue

    except KeyboardInterrupt:
        logging.info("Shutting down consumer...")
    except Exception as e:
        logging.error("An unexpected error occurred while consuming messages.", exc_info=True)
    finally:
        if consumer:
            consumer.close()


if __name__ == "__main__":
    main()
