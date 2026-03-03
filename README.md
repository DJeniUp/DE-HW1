# Orders Client

Two Python clients that fetch 1 000 orders from a local FastAPI server and write them to a CSV file.

## Setup

```bash
pip install -e .
# or
uv sync
```

Start the server (in a separate terminal):

```bash
orders_server
```

## Usage

**Threaded (sync) client:**
```bash
python client_threads.py
# output: items_threads.csv
```

**Async client:**
```bash
python client_async.py
# output: items_async.csv
```

## How it works

### client_threads.py
- Uses `ThreadPoolExecutor(max_workers=10)` to fetch orders concurrently.
- Rate-limited to **18 req/s** via `@limits` + `@sleep_and_retry` from the `ratelimit` library.
- Retries up to 5 times on:
  - **429** — reads `Retry-After` header and sleeps accordingly.
  - **5xx** — sleeps 1 second before retry.
  - **Timeout / transport errors** — sleeps 1 second before retry.
- **4xx** (other than 429) treated as non-retryable; logged at ERROR and skipped.
- All retry events logged at WARNING; final failures at ERROR.

### client_async.py
- Uses `asyncio.gather` to run all 1 000 requests concurrently.
- `asyncio.Semaphore(50)` caps in-flight requests to avoid bursts.
- `AsyncLimiter(18, 1)` (aiolimiter) enforces the 18 req/s limit.
- Same retry / logging logic as the threaded client, using `await asyncio.sleep(...)`.

## CSV output schema

| Field | Type |
|-------|------|
| order_id | int |
| account_id | int |
| company | str |
| status | str |
| currency | str |
| subtotal | float |
| tax | float |
| total | float |
| created_at | ISO 8601 |

Nested fields (`contact`, `lines`) are intentionally omitted.