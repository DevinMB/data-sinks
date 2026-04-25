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
import string

load_dotenv()

# ---------------- Env ----------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
APP_NAME = os.getenv("APP_NAME", "kafka-to-couch-sink")
COUCHDB_IP = os.getenv("COUCHDB_IP")
USER_NAME = os.getenv("USER_NAME")
PASSWORD = os.getenv("PASSWORD")
DB_NAME = os.getenv("DB_NAME")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")
KEY_FILTER = os.getenv("KEY_FILTER", "").strip()  # regex for Kafka record keys

# --- CouchDB _id strategy ---
# auto            -> let CouchDB generate _id (no upsert)
# kafka_key       -> _id = Kafka record key
# timestamp       -> _id = int(timestamp)
# timestamp_chat  -> _id = f"{int(timestamp)}:{chat_id}"
# field           -> _id = doc[COUCHDB_ID_FIELD]   (supports dotted paths, e.g. "context.id")
# custom          -> _id formatted from COUCHDB_ID_TEMPLATE
#                    Built-in tokens: {chat_id}, {int_ts}, {ts}, {key},
#                                     {last_updated}, {last_changed}, {entity_id}
#                    Nested escape:   {doc.path.to.field}
COUCHDB_ID_MODE = os.getenv("COUCHDB_ID_MODE", "kafka_key").strip().lower()
COUCHDB_ID_FIELD = os.getenv("COUCHDB_ID_FIELD", "").strip()
COUCHDB_ID_TEMPLATE = os.getenv("COUCHDB_ID_TEMPLATE", "{int_ts}:{chat_id}").strip()

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


def get_nested(doc, path):
    """
    Resolve a dotted path against a dict. Returns None if any segment is missing.
    Examples:
        get_nested(doc, "context.id")       -> doc["context"]["id"]
        get_nested(doc, "entity_id")        -> doc["entity_id"]
    """
    if not path or not isinstance(doc, dict):
        return None
    cur = doc
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def sanitize_id_segment(value):
    """
    CouchDB _ids tolerate most characters but '/' must be URL-encoded everywhere
    it's referenced, which is a footgun. Strip it preemptively. Also coerce to str.
    """
    if value is None:
        return ""
    return str(value).replace("/", "_")


class _DocFormatter(string.Formatter):
    """
    Custom Formatter that allows {doc.foo.bar.baz} to traverse nested dict paths
    in the message document. Unknown tokens render as empty strings rather than
    raising KeyError, matching the previous best-effort behavior.
    """

    def __init__(self, doc, builtins):
        super().__init__()
        self._doc = doc
        self._builtins = builtins

    def get_field(self, field_name, args, kwargs):
        # Built-in flat tokens win first
        if field_name in self._builtins:
            return self._builtins[field_name], field_name

        # {doc.x.y} -> nested traversal
        if field_name.startswith("doc."):
            return get_nested(self._doc, field_name[4:]), field_name

        # Plain top-level field on the doc
        val = self._doc.get(field_name) if isinstance(self._doc, dict) else None
        return val, field_name

    def format_field(self, value, format_spec):
        if value is None:
            return ""
        return format(value, format_spec)


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
            return sanitize_id_segment(key_str), "kafka_key"
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
            # Supports both flat keys ("entity_id") and dotted paths ("context.id")
            val = get_nested(doc, COUCHDB_ID_FIELD) if "." in COUCHDB_ID_FIELD \
                else doc.get(COUCHDB_ID_FIELD)
            if val is not None and str(val).strip() != "":
                return sanitize_id_segment(val), f"field:{COUCHDB_ID_FIELD}"
            return None, f"field:{COUCHDB_ID_FIELD}:missing"
        return None, "field:no_field_specified"

    if mode == "custom":
        builtins = {
            "chat_id": chat_id,
            "int_ts": its if its is not None else "",
            "ts": doc.get("timestamp", ""),
            "key": key_str or "",
            "last_updated": doc.get("last_updated", ""),
            "last_changed": doc.get("last_changed", ""),
            "entity_id": doc.get("entity_id", ""),
        }
        try:
            rendered = _DocFormatter(doc, builtins).format(COUCHDB_ID_TEMPLATE)
            rendered = sanitize_id_segment(rendered).strip()
            if rendered and rendered not in (":", "::"):
                return rendered, "custom"
            return None, "custom:empty_render"
        except Exception as e:
            logging.error(f"Failed to render COUCHDB_ID_TEMPLATE: {e}")
            return None, "custom:render_error"

    logging.warning(f"Unknown COUCHDB_ID_MODE='{mode}', defaulting to auto for this message.")
    return None, "unknown_mode"
