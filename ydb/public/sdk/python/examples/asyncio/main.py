import os

import ydb
import ydb.aio
import asyncio


async def execute_query(pool):
    # checkout a session to execute query.
    with pool.async_checkout() as session_holder:
        try:
            # wait for the session checkout to complete.
            session = await asyncio.wait_for(
                asyncio.wrap_future(session_holder), timeout=5
            )
        except asyncio.TimeoutError:
            raise ydb.SessionPoolEmpty()

        return await asyncio.wrap_future(
            session.transaction().async_execute(
                "select 1 as cnt;",
                commit_tx=True,
                settings=ydb.BaseRequestSettings()
                .with_timeout(3)
                .with_operation_timeout(2),
            )
        )


async def main():
    with ydb.Driver(
        endpoint=os.getenv("YDB_ENDPOINT"), database=os.getenv("YDB_DATABASE")
    ) as driver:
        # Wait for the driver to become active for requests.
        await asyncio.wait_for(
            asyncio.wrap_future(driver.async_wait(fail_fast=True)), timeout=5
        )

        # Create the session pool instance to manage YDB session.
        session_pool = ydb.SessionPool(driver)

        # Execute query with the retry_operation helper.
        # The retry operation helper can be used to retry a coroutine that raises YDB specific
        # exceptions using specified retry settings.
        result = await ydb.aio.retry_operation(execute_query, None, session_pool)

        assert result[0].rows[0].cnt == 1


asyncio.run(main())
