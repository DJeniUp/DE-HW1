import asyncio
import csv
import logging

import httpx
from aiolimiter import AsyncLimiter

BASE_URL   = "http://127.0.0.1:8000/item/{}"
MAX_RETRIES = 5
OUTPUT_CSV  = "items_async.csv"
CSV_FIELDS  = ["order_id","account_id","company","status",
               "currency","subtotal","tax","total","created_at"]

limiter   = AsyncLimiter(18, 1)
semaphore = asyncio.Semaphore(50)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

def _flatten(data: dict, item_id: int) -> dict:
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


async def fetch_order(client: httpx.AsyncClient, item_id: int) -> dict | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with limiter:
                    resp = await client.get(BASE_URL.format(item_id), timeout=2.0)

            if resp.status_code == 200:
                return _flatten(resp.json(), item_id)

            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", 1))
                log.warning("429 on id=%d attempt=%d — sleeping %.1fs", item_id, attempt, wait)
                await asyncio.sleep(wait)
                continue

            if resp.status_code >= 500:
                log.warning("5xx (%d) on id=%d attempt=%d — sleeping 1s",
                            resp.status_code, item_id, attempt)
                await asyncio.sleep(1)
                continue

            log.error("Non-retryable %d on id=%d — giving up", resp.status_code, item_id)
            return None

        except (httpx.TimeoutException, httpx.TransportError) as exc:
            log.warning("Transport error on id=%d attempt=%d: %s", item_id, attempt, exc)
            await asyncio.sleep(1)

    log.error("Exhausted retries for id=%d", item_id)
    return None


async def main():
    async with httpx.AsyncClient() as client:
        tasks = [fetch_order(client, i) for i in range(1, 1001)]
        results = await asyncio.gather(*tasks)

    rows = sorted(
        (r for r in results if r is not None),
        key=lambda r: r["order_id"],
    )

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    log.info("Done — wrote %d rows to %s", len(rows), OUTPUT_CSV)


if __name__ == "__main__":
    asyncio.run(main())