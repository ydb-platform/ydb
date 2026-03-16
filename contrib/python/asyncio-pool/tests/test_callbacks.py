import pytest
import asyncio as aio
from asyncio_pool import AioPool, getres


async def cb(res, err, ctx):
    if err:
        exc, tb = err
        assert tb
        return exc

    await aio.sleep(1 / (res - 1))
    return res * 2


async def wrk(n):
    await aio.sleep(1 / n)
    return n


@pytest.mark.asyncio
async def test_spawn_n():
    todo = range(5)
    futures = []
    async with AioPool(size=2) as pool:
        for i in todo:
            ctx = (pool, i)
            fut = pool.spawn_n(wrk(i), cb, ctx)
            futures.append(fut)

    results = [getres.flat(f) for f in futures]
    assert all(isinstance(e, ZeroDivisionError) for e in results[:2])
    assert sum(results[2:]) == 2 * (sum(todo) - 0 - 1)


@pytest.mark.asyncio
async def test_map():
    todo = range(2,11)
    async with AioPool(size=3) as pool:
        results = await pool.map(wrk, todo, cb)

    assert 2 * sum(todo) == sum(results)


@pytest.mark.asyncio
async def test_map_n():
    todo = range(2,11)
    async with AioPool(size=3) as pool:
        futures = pool.map_n(wrk, todo, cb)

    results = [getres.flat(f) for f in futures]
    assert 2 * sum(todo) == sum(results)


@pytest.mark.parametrize('timeout', [None, 0.1, 2])
@pytest.mark.asyncio
async def test_itermap(timeout):
    todo = range(2,11)

    res = 0
    async with AioPool(size=3) as pool:
        async for i in pool.itermap(wrk, todo, cb, timeout=timeout):
            res += i

    assert 2 * sum(todo) == res


@pytest.mark.asyncio
async def test_callback_types():

    def cbsync(res, err, ctx):
        return res, err, ctx

    async def cb0():
        await aio.sleep(0)
        return 'x'

    async def cb1(res):
        assert res
        await aio.sleep(0)
        return res

    async def cb2(res, err):
        await aio.sleep(0)
        return res, err

    async def cb3(res, err, ctx):
        await aio.sleep(0)
        return res, err, ctx

    class CbCls:
        def cbsync(self, res):
            return res

        async def cb0(self):
            await aio.sleep(0)
            return 'y'

        async def cb1(self, res):
            assert res
            await aio.sleep(0)
            return res

        async def cb2(self, res, err):
            await aio.sleep(0)
            return res, err

        async def cb3(self, res, err, ctx):
            assert res
            await aio.sleep(0)
            return res, err, ctx

        @staticmethod
        async def cb_static(res, err, ctx):
            assert res
            await aio.sleep(0)
            return res, err, ctx

        @classmethod
        async def cb_cls(cls, res):
            assert res
            await aio.sleep(0)
            return res

    async def wrk(n):
        if isinstance(n, int):
            assert n - 500
        await aio.sleep(0)
        return n

    _r = getres.flat
    _ctx = (123, 456, [1,2], {3,4})
    inst = CbCls()

    async with AioPool() as pool:
        # non-async callbacks
        with pytest.raises(TypeError):
            await pool.exec(wrk(1), cbsync, _ctx)
        with pytest.raises(TypeError):
            await pool.exec(wrk(1), inst.cbsync, _ctx)

        # zero args
        with pytest.raises(RuntimeError):
            await pool.exec(wrk(1), cb0, _ctx)
        with pytest.raises(RuntimeError):
            await pool.exec(wrk(1), inst.cb0, _ctx)

        # one arg
        assert 123 == await pool.exec(wrk(123), cb1, _ctx)\
            == await pool.exec(wrk(123), inst.cb1, _ctx)\
            == await pool.exec(wrk(123), CbCls.cb_cls, _ctx)

        with pytest.raises(AssertionError):
            await pool.exec(wrk(False), cb1, _ctx)

        futs = (await pool.spawn(wrk(500), cb1, _ctx),
            await pool.spawn(wrk(False), cb1, _ctx))
        await aio.wait(futs)
        assert all(isinstance(_r(f), AssertionError) for f in futs)

        # two args
        assert (123, None) == await pool.exec(wrk(123), cb2, _ctx)\
            == await pool.exec(wrk(123), inst.cb2, _ctx)

        assert (False, None) == await pool.exec(wrk(False), inst.cb2, _ctx)

        res, (exc, tb) = await pool.exec(wrk(500), cb2, _ctx)
        assert res is None and isinstance(exc, AssertionError) and \
            tb.startswith('Traceback (most recent call last)')

        # three args
        assert (123, None, _ctx) == await pool.exec(wrk(123), cb3, _ctx) \
            == await pool.exec(wrk(123), inst.cb3, _ctx) \
            == await pool.exec(wrk(123), CbCls.cb_static, _ctx)

        with pytest.raises(AssertionError):
            await pool.exec(wrk(False), inst.cb3, _ctx)

        res, (exc, tb), ctx = await pool.exec(wrk(500), cb3, _ctx)
        assert res is None and isinstance(exc, AssertionError) and \
            tb.startswith('Traceback (most recent call last)')


def inspect(obj):
    names = dir(obj)
    pad = len(max(names, key=lambda n: len(n)))
    for name in names:
        print(name.ljust(pad),':', getattr(obj, name))
