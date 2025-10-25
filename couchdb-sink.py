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
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC")
APP_NAME                = os.getenv("APP_NAME")

COUCHDB_IP = os.getenv("COUCHDB_IP")
USER_NAME  = os.getenv("USER_NAME")
PASSWORD   = os.getenv("PASSWORD")
DB_NAME    = os.getenv("DB_NAME")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")
KEY_FILTER = os.getenv("KEY_FILTER", "").strip()  # regex for Kafka record keys

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format=f'{{"app_name":"{APP_NAME}","timestamp":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}}',
    datefmt='%Y-%m-%dT%H:%M:%S'
)

def make_doc_id_from_msg(key_str, doc):
    """
    Prefer the Kafka key as the CouchDB _id (stable for replays).
    Fallback: use timestamp (int seconds) + chat_id to reduce collisions.
    """
    if key_str:
        return key_str  # you’re setting this to the timestamp in your enricher
    ts = doc.get("timestamp")
    chat_id = doc.get("chat_id", "unknown")
    if ts is None:
        # final fallback: time-based unique id (won’t upsert deterministically)
        return f"fallback:{chat_id}:{int(time.time()*1e6)}"
    try:
        # Use int seconds to match your enricher’s output-key; include chat_id to avoid cross-chat collisions
        return f"{int(float(ts))}:{chat_id}"
    except Exception:
        return f"badts:{chat_id}:{int(time.time()*1e6)}"

def upsert_doc(db, doc_id, content, max_retries=5):
    """
    Upsert into CouchDB:
    - Try to create with _id = doc_id
    - If conflict, fetch existing _rev and resave
    - Retry a few times if racing
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            # If exists, get _rev
            try:
                existing = db.get(doc_id)
            except Exception:
                existing = None

            payload = dict(content)
            payload["_id"] = doc_id
            if existing and "_rev" in existing:
                payload["_rev"] = existing["_rev"]

            db.save(payload)
            return True
        except couchdb.http.ResourceConflict:
            if attempt >= max_retries:
                logging.error(f"Conflict upserting _id={doc_id}; max retries reached")
                return False
            # small backoff, then try again (fetch new _rev)
            time.sleep(0.1 * attempt)
            continue
        except Exception as e:
            logging.error(f"Failed to upsert _id={doc_id}: {e}", exc_info=True)
            return False

def main():
    quoted_password = urllib.parse.quote(PASSWORD or "")
    couchdb_url = f'http://{USER_NAME}:{quoted_password}@{COUCHDB_IP}:5984'

    try:
        couch = couchdb.Server(couchdb_url)
    except Exception:
        logging.error("Failed to connect to CouchDB.", exc_info=True)
        sys.exit(1)

    if DB_NAME not in couch:
        logging.error(f"Database '{DB_NAME}' does not exist in CouchDB.")
        raise ValueError(f"Database '{DB_NAME}' does not exist in CouchDB.")

    db = couch[DB_NAME]

    # Compile optional key regex
    regex_filter = None
    if KEY_FILTER:
        try:
            regex_filter = re.compile(KEY_FILTER)
        except re.error as e:
            logging.error(f"Invalid regex in KEY_FILTER: {e}")
            sys.exit(1)

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            group_id=APP_NAME,
            auto_offset_reset=AUTO_OFFSET_RESET,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            # optional tuning:
            max_poll_records=200,
            request_timeout_ms=40000,
            session_timeout_ms=15000,
            heartbeat_interval_ms=3000,
        )
    except Exception:
        logging.error("Failed to create Kafka consumer.", exc_info=True)
        sys.exit(1)

    logging.info(f"Consumer is running. Listening for messages... KEY_FILTER='{KEY_FILTER or '[none]'}'")

    try:
        for message in consumer:
            key_str = message.key  # already deserialized to str (or None)
            doc = message.value

            # Filter by Kafka record key if regex provided
            if regex_filter and not (key_str and regex_filter.search(key_str)):
                logging.info(f"Key ({key_str}) did not match regex filter: {KEY_FILTER}")
                continue

            # Preserve the Kafka key in the document (optional)
            doc["kafka-key"] = key_str

            # Determine deterministic CouchDB _id
            doc_id = make_doc_id_from_msg(key_str, doc)

            # Upsert (create or update with latest enrichment)
            ok = upsert_doc(db, doc_id, doc)
            if ok:
                logging.info(f"Upserted _id={doc_id}")
            else:
                logging.error(f"Failed to upsert _id={doc_id}")

    except KeyboardInterrupt:
        logging.info("Shutting down consumer...")
    except Exception:
        logging.error("An unexpected error occurred while consuming messages.", exc_info=True)
    finally:
        try:
            consumer.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
