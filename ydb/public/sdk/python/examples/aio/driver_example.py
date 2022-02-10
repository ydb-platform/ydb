import os
import ydb
import asyncio


async def describe_database():
    endpoint = os.getenv("YDB_ENDPOINT")
    database = os.getenv("YDB_DATABASE")
    driver = ydb.aio.Driver(
        endpoint=endpoint, database=database
    )  # Creating new database driver to execute queries

    await driver.wait(timeout=10)  # Wait until driver can execute calls

    try:
        res = await driver.scheme_client.describe_path(database)
        print(" name: ", res.name, "\n", "owner: ", res.owner, "\n", "type: ", res.type)
    except ydb.Error as e:
        print("Cannot execute query. Reason: %s" % e)

    await driver.stop()  # Stops driver and close all connections


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(describe_database())
