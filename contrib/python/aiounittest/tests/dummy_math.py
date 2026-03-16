from asyncio import sleep


async def add(x, y):
    await sleep(666)
    return x + y
