import subprocess
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
        await greet("user3")
    
    # wait for refill
    await asyncio.sleep(1.2)

    res = await greet("user4")
    assert res == "Hello user4"

    limiter.close_pool()


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
    await asyncio.sleep(1.2)

    res = await greet("user5")
    assert res == "Hello user5"

    limiter.close_pool()


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
    await asyncio.sleep(1.2)

    res = await greet("user5")
    assert res == "Hello user5"

    limiter.close_pool()


@pytest.mark.asyncio
async def test_sliding_window():
    configure_connection("redis://localhost:6379")
    limiter = Limiter()

    @limiter.sliding_window(capacity=3, window_size_seconds=10)
    async def greet(name):
        return f"Hello {name}"

    results = []
    for i in range(3):
        res = await greet(f"user{i}")
        await asyncio.sleep(.1)
        results.append(res)

    assert results == ["Hello user0", "Hello user1", "Hello user2"]

    for i in range(3):
        res = await greet(f"user{i+3}")
        results.append(res)

    #  limit exceeded
    with pytest.raises(RateLimitExceeded):
        await greet("user5")
    
    # wait for window to slide enough
    await asyncio.sleep(0.8)

    assert await greet("user4") == "Hello user4"
    assert await greet("user5") == "Hello user5"

    limiter.close_pool()


@pytest.mark.asyncio
async def test_token_bucket_distributed_shared_limiter():
    configure_connection("redis://localhost:6379")
    limiter = Limiter()

    procs = []
    num_instances = 2
    script = "main.py"

    for _ in range(num_instances):
        proc = subprocess.Popen(
            ["python", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        procs.append(proc)

    stdout_data = []
    for proc in procs:
        proc.wait()
        stdout, _ = proc.communicate()
        output = stdout.decode().splitlines()
        stdout_data.extend(output)

    success_lines = [line for line in stdout_data if "have a nice day" in line]
    assert len(success_lines) == 10

    limiter.close_pool()
