import asyncio

from ratelimiters import Limiter, configure_connection, RateLimitExceeded

configure_connection("redis://localhost:6379")
limiter = Limiter()

@limiter.token_bucket()
def greeter(name):
    print(f"Hi {name}, have a nice day!!")


async def main():
    for i in range(11):
        try:
            await greeter(i)
        except RateLimitExceeded as e:
            print(e)

    await asyncio.sleep(.3)
    print("After refill")
    await greeter(11)

    

if __name__ == '__main__':
    asyncio.run(main())