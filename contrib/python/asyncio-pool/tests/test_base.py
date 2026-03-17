import pytest
import asyncio as aio
from asyncio_pool import AioPool
from async_timeout import timeout


@pytest.mark.asyncio
async def test_concurrency():
    todo = range(1,21)
    coros_active = {n:False for n in todo}

    async def wrk(n):
        nonlocal coros_active
        coros_active[n] = True
        await aio.sleep(1 / n)
        coros_active[n] = False
        return n

    pool_size = 5
    async with AioPool(size=pool_size) as pool:
        futures = pool.map_n(wrk, todo)

        await aio.sleep(0.01)

        while not pool.is_empty:
            n_active = sum(filter(None, coros_active.values()))
            assert n_active <= pool_size
            await aio.sleep(0.01)

    assert sum(todo) == sum([f.result() for f in futures])


@pytest.mark.asyncio
async def test_timeout_cancel():
    async def wrk(sem):
        async with sem:
            await aio.sleep(1)

    sem = aio.Semaphore(value=2)

    async with timeout(0.2):
        with pytest.raises(aio.CancelledError):
            await aio.gather(*[wrk(sem) for _ in range(3)])


@pytest.mark.asyncio
async def test_outer_join():

    todo, to_release = range(1,15), range(10)
    done, released = [], []

    async def inner(n):
        nonlocal done
        await aio.sleep(1 / n)
        done.append(n)

    async def outer(n, pool):
        nonlocal released
        await pool.join()
        released.append(n)

    loop = aio.get_event_loop()
    pool = AioPool(size=100)

    pool.map_n(inner, todo)
    joined = [loop.create_task(outer(j, pool)) for j in to_release]
    await pool.join()

    assert len(released) <= len(to_release)
    await aio.wait(joined)
    assert len(todo) == len(done) and len(released) == len(to_release)


@pytest.mark.asyncio
async def test_cancel():

    async def wrk(*arg, **kw):
        await aio.sleep(0.5)
        return 1

    async def wrk_safe(*arg, **kw):
        try:
            await aio.sleep(0.5)
        except aio.CancelledError:
            await aio.sleep(0.1)  # simulate cleanup
        return 1

    pool = AioPool(size=5)

    f_quick = pool.spawn_n(aio.sleep(0.15))
    f_safe = await pool.spawn(wrk_safe())
    f3 = await pool.spawn(wrk())
    pool.spawn_n(wrk())
    f567 = pool.map_n(wrk, range(3))

    # cancel some
    await aio.sleep(0.1)
    cancelled, results = await pool.cancel(f3, f567[2])  # running and waiting
    assert cancelled == len(results) == 2  # none of them had time to finish
    assert all(isinstance(res, aio.CancelledError) for res in results)

    # cancel all others
    await aio.sleep(0.1)

    # not interrupted and finished successfully
    assert f_quick.done() and f_quick.result() is None

    cancelled, results = await pool.cancel()  # all
    assert cancelled == len(results) == 4
    assert f_safe.done() and f_safe.result() == 1  # could recover
    # the others could not
    assert sum(isinstance(res, aio.CancelledError) for res in results) == 3

    assert await pool.join()  # joins successfully (basically no-op)


@pytest.mark.asyncio
async def test_internal_join():

    async def wrk(pool):
        return await pool.join()  # deadlock

    pool = AioPool(size=10)
    fut = await pool.spawn(wrk(pool))

    await aio.sleep(0.5)
    assert not fut.done()  # dealocked, will never return

    await pool.cancel(fut)
    await pool.join()
