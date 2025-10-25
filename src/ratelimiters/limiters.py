from functools import wraps
import asyncio
from urllib.parse import urlparse

import valkey

class RateLimitExceeded(Exception):
    pass


def configure_connection(valkey_url: str = ""):
    if valkey_url == "":
        raise ValueError("connection string to Valkey/Redis cannot be empty")
    
    parsed = urlparse(valkey_url)
    if not parsed.scheme.startswith("redis") and not parsed.scheme.startswith("valkey"):
        raise ValueError("Unsupported URL scheme")

    pool = valkey.ConnectionPool.from_url(valkey_url)
    Limiter._valkey = valkey.Valkey(connection_pool=pool)

class Limiter:
    _valkey: valkey.Valkey = None
    _scripts_loaded = False
    
    _token_bucket_sha = None
    _leaky_bucket_sha = None
    _fixed_window_sha = None
    _sliding_window_sha = None

    _lock = asyncio.Lock()


    @classmethod
    async def _configure(cls):
        async with cls._lock:
            if not cls._valkey:
               raise RuntimeError("Limiter not initialized. Call configure_connection() first.")
            if not cls._scripts_loaded:
                cls._token_bucket_sha = cls._valkey.script_load(_token_bucket_lua_script)
                cls._leaky_bucket_sha = cls._valkey.script_load(_leaky_bucket_lua_script)
                cls._fixed_window_sha = cls._valkey.script_load(_fixed_window_lua_script)
                cls._sliding_window_sha = cls._valkey.script_load(_sliding_window_lua_script)
                cls._scripts_loaded = True


    def __init__(self):
        pass

    def token_bucket(self, capacity:int = 10, refill_rate: float = 1.0):
        def decorator(func):
            func_unique_name = f"{func.__module__}.{func.__qualname__}"
            @wraps(func)
            async def wrapper(*args, **kwargs):
                await Limiter._configure()

                to_allow = Limiter._valkey.evalsha(Limiter._token_bucket_sha, 1, func_unique_name, 1, capacity, refill_rate)
                if to_allow:
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                else:
                    raise RateLimitExceeded(f"function {func.__name__} called too many times, please try again later")
            return wrapper
        return decorator
    
    def leaky_bucket(self, capacity:int = 10, leak_rate: float = 1.0):
        def decorator(func):
            func_unique_name = f"{func.__module__}.{func.__qualname__}"
            @wraps(func)
            async def wrapper(*args, **kwargs):
                await Limiter._configure()
                
                to_allow = Limiter._valkey.evalsha(Limiter._leaky_bucket_sha, 1, func_unique_name, 1, capacity, leak_rate)
                if to_allow:
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                else:
                    raise RateLimitExceeded(f"function {func.__name__} called too many times, please try again later")
            return wrapper
        return decorator
    
    def fixed_window(self, capacity:int = 10, window_size_seconds: int = 1):
        def decorator(func):
            func_unique_name = f"{func.__module__}.{func.__qualname__}"
            @wraps(func)
            async def wrapper(*args, **kwargs):
                await Limiter._configure()
                
                to_allow = Limiter._valkey.evalsha(Limiter._fixed_window_sha, 1, func_unique_name, 1, capacity, window_size_seconds)
                if to_allow:
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                else:
                    raise RateLimitExceeded(f"function {func.__name__} called too many times, please try again later")
            return wrapper
        return decorator
    
    def sliding_window(self, capacity:int = 10, window_size_seconds: int = 1):
        def decorator(func):
            func_unique_name = f"{func.__module__}.{func.__qualname__}"
            @wraps(func)
            async def wrapper(*args, **kwargs):
                await Limiter._configure()
                
                to_allow = Limiter._valkey.evalsha(Limiter._sliding_window_sha, 1, func_unique_name, 1, capacity, window_size_seconds)
                if to_allow:
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                else:
                    raise RateLimitExceeded(f"function {func.__name__} called too many times, please try again later")
            return wrapper
        return decorator
    
    async def close_pool(self):
        await Limiter._valkey.connection_pool.close()



_token_bucket_lua_script = """
local key = KEYS[1]
local tokens_requested = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])

local time_data = redis.call("TIME")
local now = tonumber(time_data[1]) * 1000 + math.floor(tonumber(time_data[2]) / 1000)

local bucket = redis.call("HMGET", key, "tokens", "last_time")
if bucket == false then
    redis.call("HMSET", key, "tokens", capacity, "last_time", now)
    local ttl = math.ceil(capacity / refill_rate) + 1
    redis.call("EXPIRE", key, ttl)
    return 0 
end
local tokens = tonumber(bucket[1]) or capacity
local last_time = tonumber(bucket[2]) or now

local elapsed = now - last_time
local tokens_to_add = (elapsed/1000) * refill_rate
tokens = math.min(capacity, tokens + tokens_to_add)

local allowed = 0
if tokens >= tokens_requested then
    tokens = tokens - tokens_requested
    allowed = 1
end

redis.call("HMSET", key, "tokens", tokens, "last_time", now)
local ttl = math.ceil(capacity / refill_rate) + 1
redis.call("EXPIRE", key, ttl)

return allowed
"""

_leaky_bucket_lua_script = """
local key = KEYS[1]
local tokens_requested = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local leak_rate = tonumber(ARGV[3])

local time_data = redis.call("TIME")
local now = tonumber(time_data[1]) * 1000 + math.floor(tonumber(time_data[2]) / 1000)

local bucket = redis.call("HMGET", key, "tokens", "last_time")
if bucket == false then
    redis.call("HMSET", key, "tokens", 0, "last_time", now)
    local ttl = math.ceil(capacity / leak_rate) + 1
    redis.call("EXPIRE", key, ttl)
    return 0 
end

local tokens = tonumber(bucket[1]) or 0
local last_time = tonumber(bucket[2]) or now

local elapsed = now - last_time
local tokens_leaked = (elapsed/1000) * leak_rate
tokens = math.max(0, tokens - tokens_leaked)

local allowed = 0
if tokens_requested <= capacity - tokens then
    tokens = tokens + tokens_requested
    allowed = 1
end

redis.call("HMSET", key, "tokens", tokens, "last_time", now)
local ttl = math.ceil(capacity / leak_rate ) + 1
redis.call("EXPIRE", key, ttl)

return allowed
"""

_fixed_window_lua_script = """
local key = KEYS[1]
local tokens_requested = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local window_size_seconds = tonumber(ARGV[3])

local bucket = redis.call("GET", key)

if bucket == false then
    if tokens_requested <= capacity then
        local remaining_tokens = capacity - tokens_requested
        redis.call("SETEX", key, window_size_seconds, remaining_tokens)
        return 1
    else
        return 0
    end
else
    local tokens = tonumber(bucket)
    if tokens_requested <= tokens then
        local remaining_tokens = tokens - tokens_requested
        local ttl = redis.call("TTL", key)
        if ttl < 0 then
            ttl = window_size_seconds
        end
        redis.call("SETEX", key, ttl, remaining_tokens)
        return 1
    else
        return 0
    end
end
"""

_sliding_window_lua_script = """
local key = KEYS[1]
local tokens_requested = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local window_size = tonumber(ARGV[3])

local time_data = redis.call("TIME")
local now = tonumber(time_data[1]) * 1000 + math.floor(tonumber(time_data[2]) / 1000)

redis.call("ZREMRANGEBYSCORE", key, 0, now - window_size)
local entries = redis.call("ZRANGEBYSCORE", key, now - window_size, now)
local total_tokens = 0

for i, item in ipairs(entries) do
    local _, _,  tokens = string.match(item, "([^:]+):([^:]+):([^:]+)")
    total_tokens = total_tokens + tonumber(tokens)
end

if total_tokens + tokens_requested <= capacity then
    local random = math.random(1000, 9999)
    local member = tostring(now) .. ":" .. tostring(random) .. ":" .. tostring(tokens_requested)
    redis.call("ZADD", key, now, member)
    return 1
else
    return 0
end
"""