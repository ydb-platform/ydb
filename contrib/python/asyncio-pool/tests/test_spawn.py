import pytest
import asyncio as aio
from asyncio_pool import AioPool


@pytest.mark.asyncio
async def test_spawns_behaviour():
    started = []

    async def wrk(n):
        nonlocal started
        started.append(n)
        await aio.sleep(0.1)

    async with AioPool(size=2) as pool:
        for i in range(1,6):
            await pool.spawn(wrk(i))  # waits for pool to be available
        assert len(started) != 0  # so atm some of workers should start

    started.clear()

    async with AioPool(size=2) as pool:
        for i in range(1,6):
            pool.spawn_n(wrk(i))  # does not wait for pool, just spawns waiting coros
        assert len(started) == 0  # so atm no worker should be able to start


@pytest.mark.asyncio
async def test_spawn_crash():
    async def wrk(n):
        return 1 / n

    futures = []
    async with AioPool(size=1) as pool:
        for i in (2, 1, 0):
            futures.append(await pool.spawn(wrk(i)))

    with pytest.raises(ZeroDivisionError):
        futures[-1].result()


@pytest.mark.asyncio
async def test_spawn_and_exec():

    order = []
    marker = 9999

    async def wrk(n):
        nonlocal order
        order.append(n)
        if n == marker:
            await aio.sleep(0.5)
        else:
            await aio.sleep(1 / n)
        order.append(n)
        return n

    task = range(1, 11)
    futures = []
    async with AioPool(size=7) as pool:
        for i in task:
            futures.append(await pool.spawn(wrk(i)))
        assert marker == await pool.exec(wrk(marker))

    assert [f.result() for f in futures] == list(task)
    assert pool._executed == len(task) + 1
    assert order != list(sorted(order))

    ix = order.index(marker)
    iy = order.index(marker, ix+1)
    assert iy - ix > 1
