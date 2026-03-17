import pytest
import asyncio as aio
from asyncio_pool import AioPool, getres


async def wrk(n):
    await aio.sleep(1 / n)
    return n*10


@pytest.mark.asyncio
async def test_map_simple():
    task = range(1,11)
    pool = AioPool(size=7)
    res = await pool.map(wrk, task)
    assert res == [i*10 for i in task]


@pytest.mark.asyncio
async def test_map_crash():
    task = range(5)
    pool = AioPool(size=10)

    # exc as result
    res = await pool.map(wrk, task, get_result=getres.flat)
    assert isinstance(res[0], Exception)
    assert res[1:] == [i*10 for i in task[1:]]

    # tuple as result
    res = await pool.map(wrk, task, get_result=getres.pair)
    assert res[0][0] is None and isinstance(res[0][1], ZeroDivisionError)
    assert [r[0] for r in res[1:]] == [i*10 for i in task[1:]] and \
        not any(r[1] for r in res[1:])


@pytest.mark.asyncio
async def test_itermap():

    async def wrk(n):
        await aio.sleep(n)
        return n

    async with AioPool(size=3) as pool:
        i = 0
        async for res in pool.itermap(wrk, [0.5] * 4, flat=False, timeout=0.6):
            if i == 0:
                assert 15 == int(sum(res) * 10)
            elif i == 1:
                assert 5 == int(sum(res) * 10)
            else:
                assert False  # should not get here
            i += 1  # does not support enumerate btw (


@pytest.mark.asyncio
async def test_itermap_cancel():

    async def wrk(n):
        await aio.sleep(n / 100)
        return n

    todo = range(1, 101)

    async with AioPool(5) as pool:
        async for res in pool.itermap(wrk, todo, yield_when=aio.FIRST_COMPLETED):
            if res == 13:
                cancelled, _ = await pool.cancel()
                break
    assert cancelled == 100 - 13