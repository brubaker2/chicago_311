"""
Incrementally load Chicago 311 service requests from the Socrata Open Data
API into BigQuery.

This sits OUTSIDE dbt: it is the EL (extract + load) step. dbt handles the T
(transform) once the raw rows land in BigQuery.

Strategy
--------
The Socrata dataset (v6vf-nfxy) exposes `last_modified_date`, which advances
whenever any request is created or updated. On each run we:
  1. Look up the max last_modified_date already in the raw BigQuery table.
  2. Ask Socrata only for rows modified at/after that watermark ($where).
  3. Page through results and load them into BigQuery with WRITE_APPEND.

Because the same sr_number can be modified repeatedly, the raw table will hold
multiple rows per request over time. That's fine: the dbt staging model and the
snapshot dedupe/version downstream. (If you prefer, switch the load to a MERGE
on sr_number for a single current row per request in raw.)

Env vars
--------
  GCP_PROJECT       e.g. jaffleshop-500219
  BQ_RAW_DATASET    e.g. chicago_311_raw
  BQ_RAW_TABLE      e.g. service_requests
  SOCRATA_APP_TOKEN optional; raises rate limits (get one free at
                    https://data.cityofchicago.org/profile/edit/developer_settings)

Auth
----
Uses Application Default Credentials. Locally, run once:
    gcloud auth application-default login
so the script can write to BigQuery. Never hard-code a service-account key here.
"""

import os
import time
import requests
from datetime import datetime, timezone
from google.cloud import bigquery

# --- config -----------------------------------------------------------------

DATASET_ID = "v6vf-nfxy"
BASE_URL = f"https://data.cityofchicago.org/resource/{DATASET_ID}.json"

GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_RAW_DATASET = os.environ.get("BQ_RAW_DATASET", "chicago_311_raw")
BQ_RAW_TABLE = os.environ.get("BQ_RAW_TABLE", "service_requests")
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN")

PAGE_SIZE = 50000  # Socrata max per request
FULL_TABLE = f"{GCP_PROJECT}.{BQ_RAW_DATASET}.{BQ_RAW_TABLE}"


# --- helpers ----------------------------------------------------------------

def get_watermark(client: bigquery.Client) -> str | None:
    """Return the max last_modified_date already loaded, or None on first run."""
    try:
        query = f"select max(last_modified_date) as wm from `{FULL_TABLE}`"
        row = next(iter(client.query(query).result()))
        if row.wm is None:
            return None
        # Socrata expects a floating timestamp, no timezone suffix.
        return row.wm.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        # Table doesn't exist yet -> first run -> full pull.
        return None


def fetch_page(offset: int, watermark: str | None) -> list[dict]:
    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$order": "last_modified_date",
    }
    if watermark:
        params["$where"] = f"last_modified_date >= '{watermark}'"

    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    resp = requests.get(BASE_URL, params=params, headers=headers, timeout=120)
    resp.raise_for_status()
    return resp.json()


def load_rows(client: bigquery.Client, rows: list[dict]) -> None:
    """Append rows to the raw table with an explicit all-STRING schema."""
    # Socrata returns a few nested objects (e.g. `location`, a geo point that
    # duplicates the flat latitude/longitude fields). Drop them: we keep the
    # flat lat/long columns and avoid nested-type headaches at the raw layer.
    NESTED_FIELDS = {"location"}

    cleaned = []
    field_names = set()
    for row in rows:
        row = {k: v for k, v in row.items() if k not in NESTED_FIELDS}
        cleaned.append(row)
        field_names.update(row.keys())

    # Force every remaining column to STRING. Socrata returns values as strings
    # anyway, and this keeps the schema stable across batches. All real type
    # casting happens downstream in the dbt staging model.
    schema = [bigquery.SchemaField(name, "STRING") for name in sorted(field_names)]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
    )
    job = client.load_table_from_json(cleaned, FULL_TABLE, job_config=job_config)
    job.result()


# --- main -------------------------------------------------------------------

def main() -> None:
    client = bigquery.Client(project=GCP_PROJECT)

    watermark = get_watermark(client)
    mode = "incremental" if watermark else "full backfill"
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting {mode} load.")
    if watermark:
        print(f"  Watermark: rows modified >= {watermark}")

    offset = 0
    total = 0
    while True:
        rows = fetch_page(offset, watermark)
        if not rows:
            break
        load_rows(client, rows)
        total += len(rows)
        print(f"  Loaded {len(rows)} rows (running total {total}).")
        offset += PAGE_SIZE
        time.sleep(0.2)  # be polite to the API

    print(f"Done. {total} rows loaded into {FULL_TABLE}.")


if __name__ == "__main__":
    main()
