import pytest

try:
    from aiopg.sa import SAConnection
    from pyscopg2.aiopg_sa import PoolManager
except Exception:
    SAConnection = None
    PoolManager = None


@pytest.fixture
async def pool_manager(pg_dsn):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
    )
    try:
        await pg_pool.ready()
        yield pg_pool
    finally:
        await pg_pool.close()


@pytest.mark.skipif(SAConnection is None, reason="Not supported for musl")
@pytest.mark.asyncio
async def test_acquire_with_context(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert isinstance(conn, SAConnection)
        cursor = await conn.execute("SELECT 1")
        assert await cursor.fetchall() == [(1,)]


@pytest.mark.skipif(SAConnection is None, reason="Not supported for musl")
@pytest.mark.asyncio
async def test_acquire_without_context(pool_manager):
    conn = await pool_manager.acquire_master()
    assert isinstance(conn, SAConnection)
    cursor = await conn.execute("SELECT 1")
    assert await cursor.fetchall() == [(1,)]
