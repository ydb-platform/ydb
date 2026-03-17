import asyncio

import pytest
from async_timeout import timeout

from pyscopg2.balancer_policy import (
    GreedyBalancerPolicy, RandomWeightedBalancerPolicy,
    RoundRobinBalancerPolicy,
)
from library.python.pyscopg2.tests.mocks import TestPoolManager


balancer_policies = pytest.mark.parametrize(
    "balancer_policy",
    [
        GreedyBalancerPolicy,
        RandomWeightedBalancerPolicy,
        RoundRobinBalancerPolicy,
    ],
)


@pytest.fixture
def make_dsn():
    def make(replicas_count: int):
        dsn = "postgresql://test:test@master:5432"
        replica_hosts = [f"replica{i}" for i in range(1, replicas_count + 1)]
        if replica_hosts:
            dsn += "," + ",".join(replica_hosts)
        return dsn + "/test"
    return make


@pytest.fixture
def make_pool_manager(make_dsn):
    async def make(
            balancer_policy, replicas_count: int = 2
    ):
        pool_manager = TestPoolManager(
            dsn=make_dsn(replicas_count),
            balancer_policy=balancer_policy,
            refresh_timeout=0.2,
            refresh_delay=0.1,
            acquire_timeout=0.1,
        )
        return pool_manager
    return make


@balancer_policies
@pytest.mark.asyncio
async def test_acquire_master(make_pool_manager, balancer_policy):
    pool_manager = await make_pool_manager(balancer_policy)
    async with timeout(1):
        async with pool_manager.acquire_master() as conn:
            assert await conn.is_master()


@balancer_policies
@pytest.mark.asyncio
async def test_acquire_replica(make_pool_manager, balancer_policy):
    pool_manager = await make_pool_manager(balancer_policy)
    async with timeout(1):
        async with pool_manager.acquire_replica() as conn:
            assert not await conn.is_master()


@balancer_policies
@pytest.mark.asyncio
async def test_acquire_replica_with_fallback_master(
        make_pool_manager, balancer_policy
):
    pool_manager = await make_pool_manager(balancer_policy, replicas_count=0)
    async with timeout(1):
        async with pool_manager.acquire_replica(fallback_master=True) as conn:
            assert await conn.is_master()


@balancer_policies
@pytest.mark.asyncio
async def test_acquire_master_as_replica(make_pool_manager, balancer_policy):
    pool_manager = await make_pool_manager(balancer_policy, replicas_count=0)
    async with timeout(1):
        async with pool_manager.acquire_replica(
                master_as_replica_weight=1.,
        ) as conn:
            assert await conn.is_master()


@balancer_policies
@pytest.mark.asyncio
async def test_dont_acquire_master_as_replica(
        make_pool_manager, balancer_policy
):
    pool_manager = await make_pool_manager(balancer_policy, replicas_count=0)
    with pytest.raises(asyncio.TimeoutError):
        async with pool_manager.acquire_replica(
                master_as_replica_weight=0.,
        ):
            pass
