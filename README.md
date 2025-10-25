# RateLimiters

A Python library providing **rate limiting decorators** for synchronous and asynchronous functions. Supports multiple rate limiting algorithms backed by Redis/Valkey.

## Features
* Supports multiple rate limiting aglorithms
    * Token Bucket
    * Leaky Bucket
    * Fixed Window
    * Sliding Window Log
* Works with both **sync** and **async** python functions.

---

## Usage

### Configuration

```python
from ratelimiters.limiters import configure_connection, Limiter

configure_connection("redis://localhost:6379")
limiter = Limiter()
```

### Using Rate Limiting Decorators

```python
from ratelimiters.limiters import RateLimitExceeded

@limiter.token_bucket(capacity=5, refill_rate=1.0)
async def greet(name):
    return f"Hello {name}"

async def main():
    try:
        print(await greet("Alice"))
    except RateLimitExceeded:
        print("Rate limit exceeded")
```

All decorators follow the pattern:

```python
@limiter.<algorithm>(capacity=..., refill_rate|leak_rate|window_size=...)
```

* `<algorithm>` can be: `token_bucket`, `leaky_bucket`, `fixed_window`, `sliding_window_log`.

---

## Testing

Run the unit tests:

```bash
pytest .
```
---


## Example: Distributed Usage

```python
import asyncio
from ratelimiters.limiters import Limiter, configure_connection, RateLimitExceeded

configure_connection("redis://localhost:6379")
limiter = Limiter()

@limiter.token_bucket(capacity=10, refill_rate=2.0)
async def greet(name):
    print(f"Hello {name}")

async def main():
    tasks = [greet(f"user{i}") for i in range(15)]
    await asyncio.gather(*tasks, return_exceptions=True)

asyncio.run(main())
```
---

## Why This Project?

This project was created as a learning exercise for:

* Exploring Python’s `asyncio` module.
* Understanding rate limiting algorithms and their distributed implementations.
* Practicing building reusable libraries.