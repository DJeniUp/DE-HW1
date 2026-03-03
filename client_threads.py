import csv
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import httpx
from ratelimit import limits, sleep_and_retry

BASE_URL    = "http://127.0.0.1:8000/item/{}"
MAX_WORKERS = 10
MAX_RETRIES = 5
RATE_CALLS  = 18 
RATE_PERIOD = 1 
OUTPUT_CSV  = "items_threads.csv"
CSV_FIELDS  = ["order_id","account_id","company","status",
               "currency","subtotal","tax","total","created_at"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

@sleep_and_retry
@limits(calls=RATE_CALLS, period=RATE_PERIOD)
def _get(client: httpx.Client, item_id: int) -> httpx.Response:
    return client.get(BASE_URL.format(item_id), timeout=5.0)


def fetch_order(client: httpx.Client, item_id: int) -> dict | None:
    """Fetch one order with retry logic. Returns flat dict or None on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _get(client, item_id)

            if resp.status_code == 200:
                return _flatten(resp.json(), item_id)

            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", 1))
                log.warning("429 on id=%d attempt=%d — sleeping %.1fs", item_id, attempt, wait)
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                log.warning("5xx (%d) on id=%d attempt=%d — sleeping 1s",
                            resp.status_code, item_id, attempt)
                time.sleep(1)
                continue

            # 4xx other than 429 → non-retryable
            log.error("Non-retryable %d on id=%d — giving up", resp.status_code, item_id)
            return None

        except (httpx.TimeoutException, httpx.TransportError) as exc:
            log.warning("Transport error on id=%d attempt=%d: %s", item_id, attempt, exc)
            time.sleep(1)

    log.error("Exhausted retries for id=%d", item_id)
    return None


def _flatten(data: dict, item_id: int) -> dict:
    """Extract only the CSV fields from the raw JSON response."""
    return {
        "order_id":   data.get("order_id",   item_id),
        "account_id": data.get("account_id", ""),
        "company":    data.get("company",    ""),
        "status":     data.get("status",     ""),
        "currency":   data.get("currency",   ""),
        "subtotal":   data.get("subtotal",   ""),
        "tax":        data.get("tax",        ""),
        "total":      data.get("total",      ""),
        "created_at": data.get("created_at", ""),
    }


def main():
    ids = range(1, 1001)
    rows: list[dict] = []
    lock = Lock()

    with httpx.Client() as client, \
         ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:

        futures = {pool.submit(fetch_order, client, i): i for i in ids}

        for future in as_completed(futures):
            item_id = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                log.error("Unexpected error for id=%d: %s", item_id, exc)
                result = None

            if result:
                with lock:
                    rows.append(result)
                    log.info("Collected %d/1000 (id=%d)", len(rows), item_id)

    rows.sort(key=lambda r: r["order_id"])

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    log.info("Done — wrote %d rows to %s", len(rows), OUTPUT_CSV)


if __name__ == "__main__":
    main()