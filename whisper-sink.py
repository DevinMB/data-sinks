import json
import time
from kafka import KafkaConsumer
import couchdb
import os
from dotenv import load_dotenv
import urllib.parse


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")  
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
GROUP_ID = os.getenv("GROUP_ID")

COUCHDB_IP=os.getenv("COUCHDB_IP")
USER_NAME = os.getenv("USER_NAME")
PASSWORD = os.getenv("PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def main():

    quoted_password = urllib.parse.quote(PASSWORD)
    couchdb_url = f'http://{USER_NAME}:{quoted_password}@{COUCHDB_IP}:5984'
    
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

            if message.key:
                doc["origin"] = message.key.decode("utf-8")
            else:
                doc["origin"] = None
    
            db.save(doc)

            print(f"Saved to CouchDB: {doc}")

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
