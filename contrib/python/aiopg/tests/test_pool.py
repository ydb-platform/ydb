import asyncio
from unittest import mock

import pytest
from psycopg2.extensions import TRANSACTION_STATUS_INTRANS

import aiopg
from aiopg.connection import TIMEOUT, Connection
from aiopg.pool import Pool


async def test_create_pool(create_pool):
    pool = await create_pool()
    assert isinstance(pool, Pool)
    assert 1 == pool.minsize
    assert 10 == pool.maxsize
    assert 1 == pool.size
    assert 1 == pool.freesize
    assert TIMEOUT == pool.timeout
    assert not pool.echo


async def test_create_pool2(create_pool):
    pool = await create_pool(minsize=5, maxsize=20)
    assert isinstance(pool, Pool)
    assert 5 == pool.minsize
    assert 20 == pool.maxsize
    assert 5 == pool.size
    assert 5 == pool.freesize
    assert TIMEOUT == pool.timeout


async def test_acquire(create_pool):
    pool = await create_pool()
    conn = await pool.acquire()
    assert isinstance(conn, Connection)
    assert not conn.closed
    cur = await conn.cursor()
    await cur.execute("SELECT 1")
    val = await cur.fetchone()
    assert (1,) == val
    pool.release(conn)


async def test_release(create_pool):
    pool = await create_pool(minsize=10)
    conn = await pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    pool.release(conn)
    assert 10 == pool.freesize
    assert not pool._used


async def test_release_closed(create_pool):
    pool = await create_pool(minsize=10)
    conn = await pool.acquire()
    assert 9 == pool.freesize
    await conn.close()
    pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert 9 == pool.size

    conn2 = await pool.acquire()
    assert 9 == pool.freesize
    assert 10 == pool.size
    pool.release(conn2)


async def test_bad_context_manager_usage(create_pool):
    pool = await create_pool()
    with pytest.raises(RuntimeError):
        with pool:
            pass  # noqa


async def test_context_manager(create_pool):
    pool = await create_pool(minsize=10)
    with (await pool) as conn:
        assert isinstance(conn, Connection)
        assert 9 == pool.freesize
        assert {conn} == pool._used
    assert 10 == pool.freesize


async def test_clear(create_pool):
    pool = await create_pool()
    await pool.clear()
    assert 0 == pool.freesize


async def test_initial_empty(create_pool):
    pool = await create_pool(minsize=0)
    assert 10 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    with (await pool):
        assert 1 == pool.size
        assert 0 == pool.freesize
    assert 1 == pool.size
    assert 1 == pool.freesize

    conn1 = await pool.acquire()
    assert 1 == pool.size
    assert 0 == pool.freesize

    conn2 = await pool.acquire()
    assert 2 == pool.size
    assert 0 == pool.freesize

    pool.release(conn1)
    assert 2 == pool.size
    assert 1 == pool.freesize

    pool.release(conn2)
    assert 2 == pool.size
    assert 2 == pool.freesize


async def test_parallel_tasks(create_pool):
    pool = await create_pool(minsize=0, maxsize=2)
    assert 2 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    fut1 = pool.acquire()
    fut2 = pool.acquire()

    conn1, conn2 = await asyncio.gather(fut1, fut2)
    assert 2 == pool.size
    assert 0 == pool.freesize
    assert {conn1, conn2} == pool._used

    pool.release(conn1)
    assert 2 == pool.size
    assert 1 == pool.freesize
    assert {conn2} == pool._used

    pool.release(conn2)
    assert 2 == pool.size
    assert 2 == pool.freesize
    assert not conn1.closed
    assert not conn2.closed

    conn3 = await pool.acquire()
    assert conn3 is conn1
    pool.release(conn3)


async def test_parallel_tasks_more(create_pool):
    pool = await create_pool(minsize=0, maxsize=3)

    fut1 = pool.acquire()
    fut2 = pool.acquire()
    fut3 = pool.acquire()

    conn1, conn2, conn3 = await asyncio.gather(fut1, fut2, fut3)
    assert 3 == pool.size
    assert 0 == pool.freesize
    assert {conn1, conn2, conn3} == pool._used

    pool.release(conn1)
    assert 3 == pool.size
    assert 1 == pool.freesize
    assert {conn2, conn3} == pool._used

    pool.release(conn2)
    assert 3 == pool.size
    assert 2 == pool.freesize
    assert {conn3} == pool._used
    assert not conn1.closed
    assert not conn2.closed

    pool.release(conn3)
    assert 3 == pool.size
    assert 3 == pool.freesize
    assert not pool._used
    assert not conn1.closed
    assert not conn2.closed
    assert not conn3.closed

    conn4 = await pool.acquire()
    assert conn4 is conn1
    pool.release(conn4)


async def test_release_with_invalid_status(create_pool):
    pool = await create_pool(minsize=10)
    conn = await pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    cur = await conn.cursor()
    await cur.execute("BEGIN")
    cur.close()

    with mock.patch("aiopg.pool.warnings") as m_log:
        pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert conn.closed
    m_log.warn.assert_called_with(
        f"Invalid transaction status on "
        f"released connection: {TRANSACTION_STATUS_INTRANS}",
        ResourceWarning,
    )


async def test_default_event_loop(create_pool, loop):
    asyncio.set_event_loop(loop)

    pool = await create_pool()
    assert pool._loop is loop


async def test_cursor(create_pool):
    pool = await create_pool()
    with (await pool.cursor()) as cur:
        await cur.execute("SELECT 1")
        ret = await cur.fetchone()
        assert (1,) == ret
    assert cur.closed


async def test_release_with_invalid_status_wait_release(create_pool):
    pool = await create_pool(minsize=10)
    conn = await pool.acquire()
    assert 9 == pool.freesize
    assert {conn} == pool._used
    cur = await conn.cursor()
    await cur.execute("BEGIN")
    cur.close()

    with mock.patch("aiopg.pool.warnings") as m_log:
        await pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert conn.closed
    m_log.warn.assert_called_with(
        f"Invalid transaction status on "
        f"released connection: {TRANSACTION_STATUS_INTRANS}",
        ResourceWarning,
    )


async def test_fill_free(create_pool):
    pool = await create_pool(minsize=0)
    with (await pool):
        assert 0 == pool.freesize
        assert 1 == pool.size

        conn = await asyncio.wait_for(pool.acquire(), timeout=0.5)
        assert 0 == pool.freesize
        assert 2 == pool.size
        pool.release(conn)
        assert 1 == pool.freesize
        assert 2 == pool.size
    assert 2 == pool.freesize
    assert 2 == pool.size


async def test_connect_from_acquire(create_pool):
    pool = await create_pool(minsize=0)
    assert 0 == pool.freesize
    assert 0 == pool.size
    with (await pool):
        assert 1 == pool.size
        assert 0 == pool.freesize
    assert 1 == pool.size
    assert 1 == pool.freesize


async def test_create_pool_with_timeout(create_pool):
    timeout = 0.1
    pool = await create_pool(timeout=timeout)
    assert timeout == pool.timeout
    conn = await pool.acquire()
    assert timeout == conn.timeout
    pool.release(conn)


async def test_cursor_with_timeout(create_pool):
    timeout = 0.1
    pool = await create_pool()
    with (await pool.cursor(timeout=timeout)) as cur:
        assert timeout == cur.timeout


async def test_concurrency(create_pool):
    pool = await create_pool(minsize=2, maxsize=4)
    c1 = await pool.acquire()
    c2 = await pool.acquire()
    assert 0 == pool.freesize
    assert 2 == pool.size
    pool.release(c1)
    pool.release(c2)


async def test_invalid_minsize(create_pool):
    with pytest.raises(ValueError):
        await create_pool(minsize=-1)


async def test_invalid__maxsize(create_pool):
    with pytest.raises(ValueError):
        await create_pool(minsize=5, maxsize=2)


async def test_true_parallel_tasks(create_pool):
    pool = await create_pool(minsize=0, maxsize=1)
    assert 1 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    maxsize = 0
    minfreesize = 100

    async def inner():
        nonlocal maxsize, minfreesize
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        conn = await pool.acquire()
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        await asyncio.sleep(0.01)
        pool.release(conn)
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)

    await asyncio.gather(inner(), inner())

    assert 1 == maxsize
    assert 0 == minfreesize


async def test_wait_closed(create_pool):
    pool = await create_pool(minsize=10)

    c1 = await pool.acquire()
    c2 = await pool.acquire()
    assert 10 == pool.size
    assert 8 == pool.freesize

    ops = []

    async def do_release(conn):
        await asyncio.sleep(0)
        pool.release(conn)
        ops.append("release")

    async def wait_closed():
        await pool.wait_closed()
        ops.append("wait_closed")

    pool.close()
    await asyncio.gather(wait_closed(), do_release(c1), do_release(c2))
    assert ["release", "release", "wait_closed"] == ops
    assert 0 == pool.freesize


async def test_echo(create_pool):
    pool = await create_pool(echo=True)
    assert pool.echo

    with (await pool) as conn:
        assert conn.echo


async def test_cannot_acquire_after_closing(create_pool):
    pool = await create_pool()
    pool.close()

    with pytest.raises(RuntimeError):
        await pool.acquire()


async def test_terminate_with_acquired_connections(create_pool):
    pool = await create_pool()
    conn = await pool.acquire()
    pool.terminate()
    await pool.wait_closed()

    assert conn.closed


async def test_release_closed_connection(create_pool):
    pool = await create_pool()
    conn = await pool.acquire()
    conn.close()

    pool.release(conn)


async def test_wait_closing_on_not_closed(create_pool):
    pool = await create_pool()

    with pytest.raises(RuntimeError):
        await pool.wait_closed()


async def test_release_terminated_pool(create_pool):
    pool = await create_pool()
    conn = await pool.acquire()
    pool.terminate()
    await pool.wait_closed()

    pool.release(conn)


async def test_release_terminated_pool_with_wait_release(create_pool):
    pool = await create_pool()
    conn = await pool.acquire()
    pool.terminate()
    await pool.wait_closed()

    await pool.release(conn)


async def test_close_with_acquired_connections(create_pool):
    pool = await create_pool()
    await pool.acquire()
    pool.close()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(pool.wait_closed(), 0.1)


async def test___del__(pg_params, warning):
    pool = await aiopg.create_pool(**pg_params)
    with warning(ResourceWarning):
        del pool


async def test_unlimited_size(create_pool):
    pool = await create_pool(maxsize=0)
    assert 1 == pool.minsize
    assert pool._free.maxlen is None


async def test_connection_closed_after_timeout(create_pool):
    async def sleep(conn):
        cur = await conn.cursor()
        await cur.execute("SELECT pg_sleep(10);")

    pool = await create_pool(minsize=1, maxsize=1, timeout=0.1)
    with (await pool) as conn:
        with pytest.raises(asyncio.TimeoutError):
            await sleep(conn)

    assert 0 == pool.freesize

    with (await pool) as conn:
        cur = await conn.cursor()
        await cur.execute("SELECT 1;")
        val = await cur.fetchone()
        assert (1,) == val


async def test_pool_with_connection_recycling(create_pool):
    pool = await create_pool(minsize=1, maxsize=1, pool_recycle=3)
    with (await pool) as conn:
        cur = await conn.cursor()
        await cur.execute("SELECT 1;")
        val = await cur.fetchone()
        assert (1,) == val

    await asyncio.sleep(5)

    assert 1 == pool.freesize
    with (await pool) as conn:
        cur = await conn.cursor()
        await cur.execute("SELECT 1;")
        val = await cur.fetchone()
        assert (1,) == val


async def test_connection_in_good_state_after_timeout_in_transaction(
    create_pool,
):
    async def sleep(conn):
        cur = await conn.cursor()
        await cur.execute("BEGIN;")
        await cur.execute("SELECT pg_sleep(10);")

    pool = await create_pool(minsize=1, maxsize=1, timeout=0.1)
    with (await pool) as conn:
        with pytest.raises(asyncio.TimeoutError):
            await sleep(conn)
        conn.close()

    assert 0 == pool.freesize
    assert 0 == pool.size
    with (await pool) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1;")
            val = await cur.fetchone()
            assert (1,) == val


async def test_drop_connection_if_timedout(make_connection, create_pool):
    async def _kill_connections():
        # Drop all connections on server
        conn = await make_connection()
        cur = await conn.cursor()
        await cur.execute(
            """WITH inactive_connections_list AS (
        SELECT pid FROM  pg_stat_activity WHERE pid <> pg_backend_pid())
        SELECT pg_terminate_backend(pid) FROM inactive_connections_list"""
        )
        cur.close()
        conn.close()

    pool = await create_pool(minsize=3)
    await _kill_connections()
    await asyncio.sleep(0.5)

    assert len(pool._free) == 3
    assert all([c.closed for c in pool._free])

    conn = await pool.acquire()
    cur = await conn.cursor()
    await cur.execute("SELECT 1;")
    pool.release(conn)
    conn.close()
    pool.close()
    await pool.wait_closed()


async def test_close_running_cursor(create_pool):
    pool = await create_pool(minsize=3)

    with pytest.raises(asyncio.TimeoutError):
        with (await pool.cursor(timeout=0.1)) as cur:
            await cur.execute("SELECT pg_sleep(10)")


@pytest.mark.parametrize("pool_minsize", [0, 1])
async def test_pool_on_connect(create_pool, pool_minsize):
    cb_called_times = 0

    async def cb(connection):
        nonlocal cb_called_times
        async with connection.cursor() as cur:
            await cur.execute("SELECT 1")
            data = await cur.fetchall()
            assert [(1,)] == data
            cb_called_times += 1

    pool = await create_pool(minsize=pool_minsize, maxsize=1, on_connect=cb)

    with (await pool.cursor()) as cur:
        await cur.execute("SELECT 1")

    with (await pool.cursor()) as cur:
        await cur.execute("SELECT 1")

    assert cb_called_times == 1


async def test_acquire_timeout_no_connections_available(create_pool):
    pool = await create_pool(minsize=1, maxsize=1, timeout=5)
    async with pool.acquire():
        with pytest.raises(asyncio.TimeoutError):
            async with pool.acquire():
                pytest.fail("Should not be here")
