import asyncio
import pytest
from ratelimiters.limiters import Limiter, RateLimitExceeded, configure_connection

@pytest.mark.asyncio
async def test_token_bucket():
    configure_connection("redis://localhost:6379")
    limiter = Limiter()

    @limiter.token_bucket(capacity=3, refill_rate=1.0)
    async def greet(name):
        return f"Hello {name}"

    results = []
    for i in range(3):
        res = await greet(f"user{i}")
        results.append(res)

    assert results == ["Hello user0", "Hello user1", "Hello user2"]

    #  limit exceeded
    with pytest.raises(RateLimitExceeded):
        await greet("user4")
    
    # wait for refill
    await asyncio.sleep(2.1)

    res = await greet("user5")
    assert res == "Hello user5"

    await limiter.close_pool()


@pytest.mark.asyncio
async def test_leaky_bucket():
    configure_connection("redis://localhost:6379")
    limiter = Limiter()

    @limiter.leaky_bucket(capacity=3, leak_rate=1.0)
    async def greet(name):
        return f"Hello {name}"

    results = []
    for i in range(3):
        res = await greet(f"user{i}")
        results.append(res)

    assert results == ["Hello user0", "Hello user1", "Hello user2"]

    #  limit exceeded
    with pytest.raises(RateLimitExceeded):
        await greet("user4")
    
    # wait for bucket to leak sufficiently
    await asyncio.sleep(1.1)

    res = await greet("user5")
    assert res == "Hello user5"

    await limiter.close_pool()


@pytest.mark.asyncio
async def test_fixed_window():
    configure_connection("redis://localhost:6379")
    limiter = Limiter()

    @limiter.fixed_window(capacity=3, window_size_seconds=1)
    async def greet(name):
        return f"Hello {name}"

    results = []
    for i in range(3):
        res = await greet(f"user{i}")
        results.append(res)

    assert results == ["Hello user0", "Hello user1", "Hello user2"]

    #  limit exceeded
    with pytest.raises(RateLimitExceeded):
        await greet("user4")
    
    # wait for the next window to start
    await asyncio.sleep(1.1)

    res = await greet("user5")
    assert res == "Hi user5"

    await limiter.close_pool()


@pytest.mark.asyncio
async def test_sliding_window():
    configure_connection("redis://localhost:6379")
    limiter = Limiter()

    @limiter.sliding_window(capacity=3, window_size_seconds=1)
    async def greet(name):
        return f"Hello {name}"

    results = []
    for i in range(3):
        res = await greet(f"user{i}")
        await asyncio.sleep(0.1)
        results.append(res)

    assert results == ["Hello user0", "Hello user1", "Hello user2"]

    for i in range(2):
        res = await greet(f"user{i+3}")
        results.append(res)

    #  limit exceeded
    with pytest.raises(RateLimitExceeded):
        await greet("user4")
    
    # wait for sufficient time for window to slide
    await asyncio.sleep(.7)

    for i in range(2):
        res = await greet(f"user{i+4}")
        results.append(res)

    await limiter.close_pool()