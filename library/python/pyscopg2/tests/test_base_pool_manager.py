import asyncio
from typing import Optional

import pytest
from async_timeout import timeout as timeout_context

from pyscopg2.base import BasePoolManager
from library.python.pyscopg2.tests.mocks import TestPoolManager


@pytest.fixture
def dsn():
    return "postgresql://test:test@master,replica1,replica2/test"


@pytest.fixture
async def pool_manager(dsn):
    pool_manager = TestPoolManager(dsn, refresh_timeout=0.2, refresh_delay=0.1)
    try:
        yield pool_manager
    finally:
        await pool_manager.close()


def pool_is_master(pool_manager: BasePoolManager, pool):
    assert pool_manager.pool_is_master(pool)
    assert not pool_manager.pool_is_replica(pool)


def pool_is_replica(pool_manager: BasePoolManager, pool):
    assert pool_manager.pool_is_replica(pool)
    assert not pool_manager.pool_is_master(pool)


@pytest.mark.asyncio
async def test_wait_next_pool_check(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    master_pool.shutdown()
    assert pool_manager.master_pool_count == 1
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0


@pytest.mark.asyncio
async def test_ready_all_hosts(pool_manager: BasePoolManager):
    await pool_manager.ready()
    assert len(pool_manager.dsn) == pool_manager.available_pool_count


@pytest.mark.asyncio
async def test_ready_min_count_hosts(pool_manager: BasePoolManager):
    await pool_manager.ready()
    replica_pools = await pool_manager.get_replica_pools()
    for replica_pool in replica_pools:
        replica_pool.shutdown()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    master_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0
    assert pool_manager.replica_pool_count == 0
    master_pool.startup()
    master_pool.set_master(True)
    await pool_manager.ready(masters_count=1, replicas_count=0)
    assert pool_manager.master_pool_count == 1
    assert pool_manager.replica_pool_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ["masters_count", "replicas_count"],
    [
        [-1, 5],
        [2, -10],
        [1, None],
        [None, 2],
    ],
)
async def test_ready_with_invalid_arguments(
        pool_manager: BasePoolManager,
        masters_count: Optional[int],
        replicas_count: Optional[int]
):
    with pytest.raises(ValueError):
        await pool_manager.ready(masters_count, replicas_count)


@pytest.mark.asyncio
async def test_wait_db_restart(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    assert pool_manager.pool_is_master(master_pool)
    master_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0
    master_pool.startup()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0
    assert pool_manager.pool_is_replica(master_pool)


@pytest.mark.asyncio
async def test_master_shutdown(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    assert pool_manager.pool_is_master(master_pool)
    master_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0


@pytest.mark.asyncio
async def test_replica_shutdown(pool_manager: BasePoolManager):
    await pool_manager.ready()
    replica_pool = await pool_manager.balancer.get_pool(read_only=True)
    assert pool_manager.pool_is_replica(replica_pool)
    assert pool_manager.replica_pool_count == 2
    replica_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.replica_pool_count == 1


@pytest.mark.asyncio
async def test_change_master(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    replica_pool = await pool_manager.balancer.get_pool(read_only=True)
    pool_is_master(pool_manager, master_pool)
    pool_is_replica(pool_manager, replica_pool)
    master_pool.set_master(False)
    replica_pool.set_master(True)
    await pool_manager.wait_next_pool_check()
    pool_is_master(pool_manager, replica_pool)
    pool_is_replica(pool_manager, master_pool)


@pytest.mark.asyncio
async def test_define_roles(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    replica_pool = await pool_manager.balancer.get_pool(read_only=True)
    pool_is_master(pool_manager, master_pool)
    pool_is_replica(pool_manager, replica_pool)


@pytest.mark.asyncio
async def test_acquire_master_and_release(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    init_freesize = pool_manager.get_pool_freesize(master_pool)
    connection = await pool_manager.acquire_master()
    assert pool_manager.get_pool_freesize(master_pool) + 1 == init_freesize
    assert connection in master_pool.used
    await pool_manager.release(connection)
    assert connection not in master_pool.used
    assert pool_manager.get_pool_freesize(master_pool) == init_freesize


@pytest.mark.asyncio
async def test_acquire_with_context(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    init_freesize = pool_manager.get_pool_freesize(master_pool)
    async with pool_manager.acquire_master() as connection:
        assert pool_manager.get_pool_freesize(master_pool) + 1 == init_freesize
        assert connection in master_pool.used
    assert connection not in master_pool.used
    assert pool_manager.get_pool_freesize(master_pool) == init_freesize


@pytest.mark.asyncio
async def test_acquire_replica_with_fallback_master_is_true(
        pool_manager: BasePoolManager
):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    replica_pools = await pool_manager.get_replica_pools()
    for replica_pool in replica_pools:
        assert pool_manager.pool_is_replica(replica_pool)
        replica_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.replica_pool_count == 0
    async with timeout_context(1):
        async with pool_manager.acquire_replica(fallback_master=True) as connection:
            assert connection in master_pool.used


@pytest.mark.asyncio
async def test_acquire_replica_with_fallback_master_is_false(
        pool_manager: BasePoolManager
):
    await pool_manager.ready()
    replica_pools = await pool_manager.get_replica_pools()
    for replica_pool in replica_pools:
        assert pool_manager.pool_is_replica(replica_pool)
        replica_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.replica_pool_count == 0
    with pytest.raises(asyncio.TimeoutError):
        async with timeout_context(1):
            await pool_manager.acquire_replica(fallback_master=False)


@pytest.mark.asyncio
async def test_close(pool_manager: BasePoolManager):
    await pool_manager.ready()
    assert pool_manager.master_pool_count > 0
    assert pool_manager.replica_pool_count > 0
    await pool_manager.close()
    assert pool_manager.master_pool_count == 0
    assert pool_manager.replica_pool_count == 0
    for pool in pool_manager:
        assert pool is not None
        assert all(pool_manager.is_connection_closed(conn) for conn in pool.connections)
        assert all(conn.close.call_count == 1 for conn in pool.connections)


@pytest.mark.asyncio
async def test_terminate(pool_manager: BasePoolManager):
    await pool_manager.ready()
    assert pool_manager.master_pool_count > 0
    assert pool_manager.replica_pool_count > 0
    await pool_manager.terminate()
    assert pool_manager.master_pool_count == 0
    assert pool_manager.replica_pool_count == 0
    for pool in pool_manager:
        assert pool is not None
        assert all(pool_manager.is_connection_closed(conn) for conn in pool.connections)
        assert all(conn.terminate.call_count == 1 for conn in pool.connections)


@pytest.mark.asyncio
async def test_master_behind_firewall(pool_manager: BasePoolManager):
    await pool_manager.ready()
    assert pool_manager.master_pool_count == 1
    master_pool = (await pool_manager.get_master_pools())[0]
    master_pool.behind_firewall(True)
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0
    master_pool.behind_firewall(False)
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 1


@pytest.mark.asyncio
async def test_replica_behind_firewall(pool_manager: BasePoolManager):
    await pool_manager.ready()
    replica_pool_count = 2
    assert pool_manager.replica_pool_count == replica_pool_count
    replica_pools = await pool_manager.get_replica_pools()
    for replica_pool in replica_pools:
        replica_pool.behind_firewall(True)
        await pool_manager.wait_next_pool_check()
        assert pool_manager.replica_pool_count == replica_pool_count - 1
        replica_pool.behind_firewall(False)
        await pool_manager.wait_next_pool_check()
        assert pool_manager.replica_pool_count == replica_pool_count
