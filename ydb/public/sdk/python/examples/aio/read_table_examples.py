import asyncio

import ydb
import os


async def create_tables():
    from session_pool_example import main

    await main()


async def insert_data():
    from transaction_example import main

    await main()


async def main():
    await create_tables()
    await insert_data()
    endpoint = os.getenv("YDB_ENDPOINT")
    database = os.getenv("YDB_DATABASE")
    driver = ydb.aio.Driver(endpoint=endpoint, database=database)
    pool = ydb.aio.SessionPool(driver, size=10)  # Max number of available session
    session = await pool.acquire()
    async for sets in await session.read_table(database + "/episodes"):
        print(*[row[::] for row in sets.rows], sep="\n")
    await pool.release(session)
    await pool.stop()
    await driver.stop()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
