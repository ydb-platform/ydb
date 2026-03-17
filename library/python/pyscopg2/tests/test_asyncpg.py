import pytest
from asyncpg import Connection

from pyscopg2.asyncpg import PoolManager


@pytest.fixture
async def pool_manager(pg_dsn):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        pool_factory_kwargs={
            "min_size": 10,
            "max_size": 10,
        },
    )
    try:
        await pg_pool.ready()
        yield pg_pool
    finally:
        await pg_pool.close()


@pytest.mark.asyncio
async def test_acquire_with_context(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert isinstance(conn, Connection)
        assert await conn.fetch("SELECT 1") == [(1,)]


@pytest.mark.asyncio
async def test_acquire_without_context(pool_manager):
    conn = await pool_manager.acquire_master()
    assert isinstance(conn, Connection)
    assert await conn.fetch("SELECT 1") == [(1,)]


@pytest.mark.asyncio
async def test_close(pool_manager):
    asyncpg_pool = await pool_manager.balancer.get_pool(read_only=False)
    await pool_manager.close()
    assert asyncpg_pool._closed


@pytest.mark.asyncio
async def test_terminate(pool_manager):
    asyncpg_pool = await pool_manager.balancer.get_pool(read_only=False)
    await pool_manager.terminate()
    assert asyncpg_pool._closed


@pytest.mark.asyncio
async def test_release(pool_manager):
    asyncpg_pool = await pool_manager.balancer.get_pool(read_only=False)
    assert pool_manager.get_pool_freesize(asyncpg_pool) == 10
    conn = await pool_manager.acquire_master()
    assert pool_manager.get_pool_freesize(asyncpg_pool) == 9
    await pool_manager.release(conn)
    assert pool_manager.get_pool_freesize(asyncpg_pool) == 10
