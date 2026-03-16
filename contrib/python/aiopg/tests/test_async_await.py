import asyncio

import pytest

import aiopg
import aiopg.sa
from aiopg.sa import SAConnection


async def test_cursor_await(make_connection):
    conn = await make_connection()

    cursor = await conn.cursor()
    await cursor.execute("SELECT 42;")
    resp = await cursor.fetchone()
    assert resp == (42,)
    cursor.close()


async def test_connect_context_manager(pg_params):
    async with aiopg.connect(**pg_params) as conn:
        cursor = await conn.cursor()
        await cursor.execute("SELECT 42")
        resp = await cursor.fetchone()
        assert resp == (42,)
        cursor.close()
    assert conn.closed


async def test_connection_context_manager(make_connection):
    conn = await make_connection()
    assert not conn.closed
    async with conn:
        cursor = await conn.cursor()
        await cursor.execute("SELECT 42;")
        resp = await cursor.fetchone()
        assert resp == (42,)
        cursor.close()
    assert conn.closed


async def test_cursor_create_with_context_manager(make_connection):
    conn = await make_connection()

    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 42;")
        resp = await cursor.fetchone()
        assert resp == (42,)
        assert not cursor.closed

    assert cursor.closed


@pytest.mark.skip("TODO: fails in arcadia build somehow")
async def test_pool_context_manager_timeout(pg_params, loop):
    async with aiopg.create_pool(**pg_params, minsize=1, maxsize=1) as pool:
        cursor_ctx = await pool.cursor()
        with pytest.warns(ResourceWarning, match="Invalid transaction status"):
            with cursor_ctx as cursor:
                hung_task = cursor.execute("SELECT pg_sleep(10000);")
                # start task
                loop.create_task(hung_task)
                # sleep for a bit so it gets going
                await asyncio.sleep(1)

        cursor_ctx = await pool.cursor()
        with cursor_ctx as cursor:
            resp = await cursor.execute("SELECT 42;")
            resp = await cursor.fetchone()
            assert resp == (42,)

    assert cursor.closed
    assert pool.closed


async def test_cursor_with_context_manager(make_connection):
    conn = await make_connection()
    cursor = await conn.cursor()
    await cursor.execute("SELECT 42;")

    assert not cursor.closed
    async with cursor:
        resp = await cursor.fetchone()
        assert resp == (42,)
    assert cursor.closed


async def test_cursor_lightweight(make_connection):
    conn = await make_connection()
    cursor = await conn.cursor()
    await cursor.execute("SELECT 42;")

    assert not cursor.closed
    async with cursor:
        pass
    assert cursor.closed


async def test_pool_context_manager(pg_params):
    pool = await aiopg.create_pool(**pg_params)

    async with pool:
        conn = await pool.acquire()
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 42;")
            resp = await cursor.fetchone()
            assert resp == (42,)
        pool.release(conn)
    assert cursor.closed
    assert pool.closed


async def test_create_pool_context_manager(pg_params):
    async with aiopg.create_pool(**pg_params) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 42;")
                resp = await cursor.fetchone()
                assert resp == (42,)

    assert cursor.closed
    assert conn.closed
    assert pool.closed


async def test_cursor_aiter(make_connection):
    result = []
    conn = await make_connection()
    assert not conn.closed
    async with conn:
        cursor = await conn.cursor()
        await cursor.execute("SELECT generate_series(1, 5);")
        async for v in cursor:
            result.append(v)
        assert result == [(1,), (2,), (3,), (4,), (5,)]
        cursor.close()
    assert conn.closed


async def test_engine_context_manager(pg_params):
    engine = await aiopg.sa.create_engine(**pg_params)
    async with engine:
        conn = await engine.acquire()
        assert isinstance(conn, SAConnection)
        engine.release(conn)
    assert engine.closed


async def test_create_engine_context_manager(pg_params):
    async with aiopg.sa.create_engine(**pg_params) as engine:
        async with engine.acquire() as conn:
            assert isinstance(conn, SAConnection)
    assert engine.closed


async def test_result_proxy_aiter(pg_params):
    sql = "SELECT generate_series(1, 5);"
    result = []
    async with aiopg.sa.create_engine(**pg_params) as engine:
        async with engine.acquire() as conn:
            async with conn.execute(sql) as cursor:
                async for v in cursor:
                    result.append(v)
                assert result == [(1,), (2,), (3,), (4,), (5,)]
            assert cursor.closed
    assert conn.closed


async def test_transaction_context_manager(pg_params):
    sql = "SELECT generate_series(1, 5);"
    result = []
    async with aiopg.sa.create_engine(**pg_params) as engine:
        async with engine.acquire() as conn:
            async with conn.begin() as tr:
                async with conn.execute(sql) as cursor:
                    async for v in cursor:
                        result.append(v)
                    assert tr.is_active
                assert result == [(1,), (2,), (3,), (4,), (5,)]
                assert cursor.closed
            assert not tr.is_active

            tr2 = await conn.begin()
            async with tr2:
                assert tr2.is_active
                async with conn.execute("SELECT 1;") as cursor:
                    rec = await cursor.scalar()
                    assert rec == 1
                    cursor.close()
            assert not tr2.is_active

    assert conn.closed


async def test_transaction_context_manager_error(pg_params):
    async with aiopg.sa.create_engine(**pg_params) as engine:
        async with engine.acquire() as conn:
            with pytest.raises(RuntimeError) as ctx:
                async with conn.begin() as tr:
                    assert tr.is_active
                    raise RuntimeError("boom")
            assert str(ctx.value) == "boom"
            assert not tr.is_active
    assert conn.closed


async def test_transaction_context_manager_commit_once(pg_params):
    async with aiopg.sa.create_engine(**pg_params) as engine:
        async with engine.acquire() as conn:
            async with conn.begin() as tr:
                # check that in context manager we do not execute
                # commit for second time. Two commits in row causes
                # InvalidRequestError exception
                await tr.commit()
            assert not tr.is_active

            tr2 = await conn.begin()
            async with tr2:
                assert tr2.is_active
                # check for double commit one more time
                await tr2.commit()
            assert not tr2.is_active
    assert conn.closed


async def test_transaction_context_manager_nested_commit(pg_params):
    sql = "SELECT generate_series(1, 5);"
    result = []
    async with aiopg.sa.create_engine(**pg_params) as engine:
        async with engine.acquire() as conn:
            async with conn.begin_nested() as tr1:
                async with conn.begin_nested() as tr2:
                    async with conn.execute(sql) as cursor:
                        async for v in cursor:
                            result.append(v)
                        assert tr1.is_active
                        assert tr2.is_active
                    assert result == [(1,), (2,), (3,), (4,), (5,)]
                    assert cursor.closed
                assert not tr2.is_active

                tr2 = await conn.begin_nested()
                async with tr2:
                    assert tr2.is_active
                    async with conn.execute("SELECT 1;") as cursor:
                        rec = await cursor.scalar()
                        assert rec == 1
                        cursor.close()
                assert not tr2.is_active
            assert not tr1.is_active

    assert conn.closed


async def test_sa_connection_execute(pg_params):
    sql = "SELECT generate_series(1, 5);"
    result = []
    async with aiopg.sa.create_engine(**pg_params) as engine:
        async with engine.acquire() as conn:
            async for value in conn.execute(sql):
                result.append(value)
            assert result == [(1,), (2,), (3,), (4,), (5,)]
    assert conn.closed
