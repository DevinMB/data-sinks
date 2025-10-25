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

# ---------------- Env ----------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC")
APP_NAME                = os.getenv("APP_NAME", "kafka-to-couch-sink")

COUCHDB_IP = os.getenv("COUCHDB_IP")
USER_NAME  = os.getenv("USER_NAME")
PASSWORD   = os.getenv("PASSWORD")
DB_NAME    = os.getenv("DB_NAME")

AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")
KEY_FILTER        = os.getenv("KEY_FILTER", "").strip()  # regex for Kafka record keys

# --- NEW: CouchDB _id strategy ---
#   auto            -> let CouchDB generate _id (no upsert)
#   kafka_key       -> _id = Kafka record key
#   timestamp       -> _id = int(timestamp)
#   timestamp_chat  -> _id = f"{int(timestamp)}:{chat_id}"
#   field           -> _id = doc[COUCHDB_ID_FIELD]
#   custom          -> _id formatted from COUCHDB_ID_TEMPLATE (supports {chat_id}, {int_ts}, {ts}, {key})
COUCHDB_ID_MODE      = os.getenv("COUCHDB_ID_MODE", "kafka_key").strip().lower()
COUCHDB_ID_FIELD     = os.getenv("COUCHDB_ID_FIELD", "").strip()
COUCHDB_ID_TEMPLATE  = os.getenv("COUCHDB_ID_TEMPLATE", "{int_ts}:{chat_id}").strip()

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format=f'{{"app_name":"{APP_NAME}","timestamp":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}}',
    datefmt='%Y-%m-%dT%H:%M:%S'
)

def int_ts(ts):
    """Safely coerce to int seconds; return None on failure."""
    if ts is None:
        return None
    try:
        return int(float(ts))
    except Exception:
        return None

def compute_doc_id(mode, key_str, doc):
    """
    Compute a deterministic CouchDB _id based on COUCHDB_ID_MODE.
    Return (doc_id:str | None, reason:str).
    If None is returned, caller should treat as 'auto' (no upsert).
    """
    its = int_ts(doc.get("timestamp"))
    chat_id = doc.get("chat_id", "unknown")

    if mode == "auto":
        return None, "auto"

    if mode == "kafka_key":
        if key_str:
            return key_str, "kafka_key"
        return None, "kafka_key:missing"

    if mode == "timestamp":
        if its is not None:
            return str(its), "timestamp"
        return None, "timestamp:missing"

    if mode == "timestamp_chat":
        if its is not None:
            return f"{its}:{chat_id}", "timestamp_chat"
        return None, "timestamp_chat:missing_ts"

    if mode == "field":
        if COUCHDB_ID_FIELD:
            val = doc.get(COUCHDB_ID_FIELD)
            if val is not None and str(val).strip() != "":
                return str(val), f"field:{COUCHDB_ID_FIELD}"
            return None, f"field:{COUCHDB_ID_FIELD}:missing"
        return None, "field:no_field_specified"

    if mode == "custom":
        # supported tokens: {chat_id}, {int_ts}, {ts}, {key}
        try:
            rendered = COUCHDB_ID_TEMPLATE.format(
                chat_id=chat_id,
                int_ts=its if its is not None else "",
                ts=doc.get("timestamp", ""),
                key=key_str or "",
            )
            rendered = str(rendered).strip()
            if rendered:
                return rendered, "custom"
            return None, "custom:empty_render"
        except Exception as e:
            logging.error(f"Failed to render COUCHDB_ID_TEMPLATE: {e}")
            return None, "custom:render_error"

    # Unknown mode -> behave like auto but warn
    logging.warning(f"Unknown COUCHDB_ID_MODE='{mode}', defaulting to auto for this message.")
    return None, "unknown_mode"

def upsert_doc(db, doc_id, content, max_retries=5):
    """
    Upsert into CouchDB:
    - If doc exists, fetch _rev and save.
    - If conflict, retry with backoff.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            existing = db.get(doc_id)
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
            time.sleep(0.1 * attempt)
            continue
        except Exception as e:
            logging.error(f"Failed to upsert _id={doc_id}: {e}", exc_info=True)
            return False

def main():
    # ---- CouchDB connection ----
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

    # ---- Optional Kafka key regex filter ----
    regex_filter = None
    if KEY_FILTER:
        try:
            regex_filter = re.compile(KEY_FILTER)
        except re.error as e:
            logging.error(f"Invalid regex in KEY_FILTER: {e}")
            sys.exit(1)

    # ---- Kafka consumer ----
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            group_id=APP_NAME,
            auto_offset_reset=AUTO_OFFSET_RESET,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            max_poll_records=200,
            request_timeout_ms=40000,
            session_timeout_ms=15000,
            heartbeat_interval_ms=3000,
        )
    except Exception:
        logging.error("Failed to create Kafka consumer.", exc_info=True)
        sys.exit(1)

    logging.info(
        f"Consumer is running. KEY_FILTER='{KEY_FILTER or '[none]'}', "
        f"COUCHDB_ID_MODE='{COUCHDB_ID_MODE}', "
        f"COUCHDB_ID_FIELD='{COUCHDB_ID_FIELD or '[none]'}', "
        f"COUCHDB_ID_TEMPLATE='{COUCHDB_ID_TEMPLATE if COUCHDB_ID_MODE=='custom' else '[n/a]'}'"
    )

    # ---- Main consume loop ----
    try:
        for message in consumer:
            key_str = message.key
            doc = message.value

            # Filter by Kafka record key if regex provided
            if regex_filter and not (key_str and regex_filter.search(key_str)):
                logging.info(f"Key ({key_str}) did not match regex filter: {KEY_FILTER}")
                continue

            # Always keep the Kafka key for traceability
            doc["kafka-key"] = key_str

            # Compute _id according to mode
            doc_id, reason = compute_doc_id(COUCHDB_ID_MODE, key_str, doc)

            if doc_id is None:
                # AUTO mode or failed to compute deterministic id -> plain save (no upsert)
                try:
                    db.save(doc)
                    logging.info(f"Saved (auto-id) reason={reason}")
                except Exception:
                    logging.error("Failed to save document to CouchDB (auto-id).", exc_info=True)
                continue

            # Deterministic id -> upsert
            ok = upsert_doc(db, doc_id, doc)
            if ok:
                logging.info(f"Upserted _id={doc_id} (mode={reason})")
            else:
                logging.error(f"Failed to upsert _id={doc_id} (mode={reason})")

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
