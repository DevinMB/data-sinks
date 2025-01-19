import json
import time
from kafka import KafkaConsumer
import couchdb
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")  
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
GROUP_ID = os.getenv("GROUP_ID")

COUCHDB_IP=os.getenv("COUCHDB_URL")
USER_NAME = os.getenv("USER_NAME")
PASSWORD = os.getenv("PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def main():

    couchdb_url = f'http://{USER_NAME}:{PASSWORD}@{COUCHDB_IP}:5984'
    
    print(couchdb_url)

    couch = couchdb.Server(couchdb_url)

    if DB_NAME not in couch:
        raise ValueError(
            f"Database '{DB_NAME}' does not exist in CouchDB. "
            "Please create it before running this program."
        )

    db = couch[DB_NAME]
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        group_id=GROUP_ID,
        auto_offset_reset='earliest',    
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("Consumer is running. Listening for messages...")

    try:
        for message in consumer:
    
            doc = message.value
    
            db.save(doc)

            print(f"Saved to CouchDB: {doc}")

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
