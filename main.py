import asyncio

from ratelimiters.limiters import Limiter, configure_connection, RateLimitExceeded

configure_connection("redis://localhost:6379")
limiter = Limiter()


@limiter.token_bucket()
def greeter(name):
    print(f"Hi {name}, have a nice day!!")


async def main():
    for i in range(10):
        try:
            await greeter(i)
            # await asyncio.sleep(.1)
        except RateLimitExceeded as e:
            print(f"{i} not called")

    # comment the following lines when running pytest
    await asyncio.sleep(1.2)
    print("After refill")
    try:
        await greeter(11)
    except RateLimitExceeded as e:
            print("11 not called")

    # uncomment the following for distributed usage
    # tasks = [greeter(f"user{i}") for i in range(15)]
    # await asyncio.gather(*tasks, return_exceptions=True)
    

if __name__ == '__main__':
    asyncio.run(main())