# flake8: noqa
# test code was mostly copied from stdlib, can't be fixing this mad stuff...
import copy
import inspect
import pickle
import re
import sys
import types
import warnings

import pytest

from lazy_object_proxy.utils import await_

pypyxfail = pytest.mark.xfail('hasattr(sys, "pypy_version_info")')
graalpyxfail = pytest.mark.xfail('sys.implementation.name == "graalpy"')


class AsyncYieldFrom:
    def __init__(self, obj):
        self.obj = obj

    def __await__(self):
        yield from self.obj


class AsyncYield:
    def __init__(self, value):
        self.value = value

    def __await__(self):
        yield self.value


def run_async(coro):
    assert coro.__class__ in {types.GeneratorType, types.CoroutineType}

    buffer = []
    result = None
    while True:
        try:
            buffer.append(coro.send(None))
        except StopIteration as ex:
            result = ex.args[0] if ex.args else None
            break
    return buffer, result


def run_async__await__(coro):
    assert coro.__class__ is types.CoroutineType
    aw = coro.__await__()
    buffer = []
    result = None
    i = 0
    while True:
        try:
            if i % 2:
                buffer.append(next(aw))
            else:
                buffer.append(aw.send(None))
            i += 1
        except StopIteration as ex:
            result = ex.args[0] if ex.args else None
            break
    return buffer, result


async def proxy(ob):  # workaround
    return await ob


def test_gen_1(lop):
    def gen():
        yield

    assert not hasattr(gen, '__await__')


@graalpyxfail
def test_func_1(lop):
    async def foo():
        return 10

    f = lop.Proxy(foo)
    assert isinstance(f, types.CoroutineType)
    assert bool(foo.__code__.co_flags & inspect.CO_COROUTINE)
    assert not bool(foo.__code__.co_flags & inspect.CO_GENERATOR)
    assert bool(f.cr_code.co_flags & inspect.CO_COROUTINE)
    assert not bool(f.cr_code.co_flags & inspect.CO_GENERATOR)
    assert run_async(f) == ([], 10)

    assert run_async__await__(foo()) == ([], 10)

    def bar():
        pass

    assert not bool(bar.__code__.co_flags & inspect.CO_COROUTINE)


@graalpyxfail
def test_func_2(lop):
    async def foo():
        raise StopIteration

    with pytest.raises(RuntimeError, match='coroutine raised StopIteration'):
        run_async(lop.Proxy(foo))


def test_func_3(lop):
    async def foo():
        raise StopIteration

    coro = lop.Proxy(foo)
    assert re.search('^<coroutine object.* at 0x.*>$', str(coro))
    coro.close()


def test_func_4(lop):
    async def foo():
        raise StopIteration

    coro = lop.Proxy(foo)

    check = lambda: pytest.raises(TypeError, match="'coroutine' object is not iterable")

    with check():
        list(coro)

    with check():
        tuple(coro)

    with check():
        sum(coro)

    with check():
        iter(coro)

    with check():
        for i in coro:
            pass

    with check():
        [i for i in coro]

    coro.close()


def test_func_5(lop):
    @types.coroutine
    def bar():
        yield 1

    async def foo():
        await lop.Proxy(bar)

    check = lambda: pytest.raises(TypeError, match="'coroutine' object is not iterable")

    coro = lop.Proxy(foo)
    with check():
        for el in coro:
            pass
    coro.close()

    # the following should pass without an error
    for el in lop.Proxy(bar):
        assert el == 1
    assert [el for el in lop.Proxy(bar)] == [1]
    assert tuple(lop.Proxy(bar)) == (1,)
    assert next(iter(lop.Proxy(bar))) == 1


def test_func_6(lop):
    @types.coroutine
    def bar():
        yield 1
        yield 2

    async def foo():
        await lop.Proxy(bar)

    f = lop.Proxy(foo)
    assert f.send(None) == 1
    assert f.send(None) == 2
    with pytest.raises(StopIteration):
        f.send(None)


def test_func_7(lop):
    async def bar():
        return 10

    coro = lop.Proxy(bar)

    def foo():
        yield from coro

    with pytest.raises(
        TypeError,
        match="'coroutine' object is not iterable",
        # looks like python has some special error rewrapping?!
        # match="cannot 'yield from' a coroutine object in "
        #       "a non-coroutine generator"
    ):
        list(lop.Proxy(foo))

    coro.close()


def test_func_8(lop):
    @types.coroutine
    def bar():
        return (yield from coro)

    async def foo():
        return 'spam'

    coro = await_(lop.Proxy(foo))
    # coro = lop.Proxy(foo)
    assert run_async(lop.Proxy(bar)) == ([], 'spam')
    coro.close()


def test_func_10(lop):
    N = 0

    @types.coroutine
    def gen():
        nonlocal N
        try:
            a = yield
            yield (a**2)
        except ZeroDivisionError:
            N += 100
            raise
        finally:
            N += 1

    async def foo():
        await lop.Proxy(gen)

    coro = lop.Proxy(foo)
    aw = coro.__await__()
    assert aw is iter(aw)
    next(aw)
    assert aw.send(10) == 100

    assert N == 0
    aw.close()
    assert N == 1

    coro = foo()
    aw = coro.__await__()
    next(aw)
    with pytest.raises(ZeroDivisionError):
        aw.throw(ZeroDivisionError)
    assert N == 102


def test_func_11(lop):
    async def func():
        pass

    coro = lop.Proxy(func)
    # Test that PyCoro_Type and _PyCoroWrapper_Type types were properly
    # initialized
    assert '__await__' in dir(coro)
    awaitable = coro.__await__()
    assert '__iter__' in dir(awaitable)
    assert 'coroutine_wrapper' in str(awaitable)
    # avoid RuntimeWarnings
    awaitable.close()
    coro.close()


@graalpyxfail
def test_func_12(lop):
    async def g():
        i = me.send(None)
        await foo

    me = lop.Proxy(g)
    with pytest.raises(ValueError, match='coroutine already executing'):
        me.send(None)


@graalpyxfail
def test_func_13(lop):
    async def g():
        pass

    coro = lop.Proxy(g)
    with pytest.raises(TypeError, match="can't send non-None value to a just-started coroutine"):
        coro.send('spam')

    coro.close()


@graalpyxfail
def test_func_14(lop):
    @types.coroutine
    def gen():
        yield

    async def coro():
        try:
            await lop.Proxy(gen)
        except GeneratorExit:
            await lop.Proxy(gen)

    c = lop.Proxy(coro)
    c.send(None)
    with pytest.raises(RuntimeError, match='coroutine ignored GeneratorExit'):
        c.close()


def test_func_15(lop):
    # See http://bugs.python.org/issue25887 for details

    async def spammer():
        return 'spam'

    async def reader(coro):
        return await coro

    spammer_coro = lop.Proxy(spammer)

    with pytest.raises(StopIteration, match='spam'):
        reader(spammer_coro).send(None)

    with pytest.raises(RuntimeError, match='cannot reuse already awaited coroutine'):
        reader(spammer_coro).send(None)


def test_func_16(lop):
    # See http://bugs.python.org/issue25887 for details

    @types.coroutine
    def nop():
        yield

    async def send():
        await nop()
        return 'spam'

    async def read(coro):
        await nop()
        return await coro

    spammer = lop.Proxy(send)

    reader = lop.Proxy(lambda: read(spammer))
    reader.send(None)
    reader.send(None)
    with pytest.raises(Exception, match='ham'):
        reader.throw(Exception('ham'))

    reader = read(spammer)
    reader.send(None)
    with pytest.raises(RuntimeError, match='cannot reuse already awaited coroutine'):
        reader.send(None)

    with pytest.raises(RuntimeError, match='cannot reuse already awaited coroutine'):
        reader.throw(Exception('wat'))


def test_func_17(lop):
    # See http://bugs.python.org/issue25887 for details

    async def coroutine():
        return 'spam'

    coro = lop.Proxy(coroutine)
    with pytest.raises(StopIteration, match='spam'):
        coro.send(None)

    with pytest.raises(RuntimeError, match='cannot reuse already awaited coroutine'):
        coro.send(None)

    with pytest.raises(RuntimeError, match='cannot reuse already awaited coroutine'):
        coro.throw(Exception('wat'))

    # Closing a coroutine shouldn't raise any exception even if it's
    # already closed/exhausted (similar to generators)
    coro.close()
    coro.close()


def test_func_18(lop):
    # See http://bugs.python.org/issue25887 for details

    async def coroutine():
        return 'spam'

    coro = lop.Proxy(coroutine)
    await_iter = coro.__await__()
    it = iter(await_iter)

    with pytest.raises(StopIteration, match='spam'):
        it.send(None)

    with pytest.raises(RuntimeError, match='cannot reuse already awaited coroutine'):
        it.send(None)

    with pytest.raises(RuntimeError, match='cannot reuse already awaited coroutine'):
        # Although the iterator protocol requires iterators to
        # raise another StopIteration here, we don't want to do
        # that.  In this particular case, the iterator will raise
        # a RuntimeError, so that 'yield from' and 'await'
        # expressions will trigger the error, instead of silently
        # ignoring the call.
        next(it)

    with pytest.raises(RuntimeError, match='cannot reuse already awaited coroutine'):
        it.throw(Exception('wat'))

    with pytest.raises(RuntimeError, match='cannot reuse already awaited coroutine'):
        it.throw(Exception('wat'))

    # Closing a coroutine shouldn't raise any exception even if it's
    # already closed/exhausted (similar to generators)
    it.close()
    it.close()


def test_func_19(lop):
    CHK = 0

    @types.coroutine
    def foo():
        nonlocal CHK
        yield
        try:
            yield
        except GeneratorExit:
            CHK += 1

    async def coroutine():
        await foo()

    coro = lop.Proxy(coroutine)

    coro.send(None)
    coro.send(None)

    assert CHK == 0
    coro.close()
    assert CHK == 1

    for _ in range(3):
        # Closing a coroutine shouldn't raise any exception even if it's
        # already closed/exhausted (similar to generators)
        coro.close()
        assert CHK == 1


def test_coro_wrapper_send_tuple(lop):
    async def foo():
        return (10,)

    result = run_async__await__(lop.Proxy(foo))
    assert result == ([], (10,))


def test_coro_wrapper_send_stop_iterator(lop):
    async def foo():
        return StopIteration(10)

    result = run_async__await__(lop.Proxy(foo))
    assert isinstance(result[1], StopIteration)
    assert result[1].value == 10


def test_cr_await(lop):
    @types.coroutine
    def a():
        assert inspect.getcoroutinestate(coro_b) == inspect.CORO_RUNNING
        assert coro_b.cr_await is None
        yield
        assert inspect.getcoroutinestate(coro_b) == inspect.CORO_RUNNING
        assert coro_b.cr_await is None

    async def c():
        await lop.Proxy(a)

    async def b():
        assert coro_b.cr_await is None
        await lop.Proxy(c)
        assert coro_b.cr_await is None

    coro_b = lop.Proxy(b)
    assert inspect.getcoroutinestate(coro_b) == inspect.CORO_CREATED
    assert coro_b.cr_await is None

    coro_b.send(None)
    assert inspect.getcoroutinestate(coro_b) == inspect.CORO_SUSPENDED

    with pytest.raises(StopIteration):
        coro_b.send(None)  # complete coroutine
    assert inspect.getcoroutinestate(coro_b) == inspect.CORO_CLOSED
    assert coro_b.cr_await is None


def test_await_1(lop):
    async def foo():
        await 1

    with pytest.raises(TypeError, match='int.*can.t.*await'):
        run_async(lop.Proxy(foo))


def test_await_2(lop):
    async def foo():
        await []

    with pytest.raises(TypeError, match='list.*can.t.*await'):
        run_async(lop.Proxy(foo))


def test_await_3(lop):
    async def foo():
        await AsyncYieldFrom([1, 2, 3])

    assert run_async(lop.Proxy(foo)) == ([1, 2, 3], None)
    assert run_async__await__(lop.Proxy(foo)) == ([1, 2, 3], None)


def test_await_4(lop):
    async def bar():
        return 42

    async def foo():
        return await lop.Proxy(bar)

    assert run_async(lop.Proxy(foo)) == ([], 42)


def test_await_5(lop):
    class Awaitable:
        def __await__(self):
            return

    async def foo():
        return await lop.Proxy(Awaitable)

    with pytest.raises(TypeError, match='__await__.*returned non-iterator of type'):
        run_async(lop.Proxy(foo))


def test_await_6(lop):
    class Awaitable:
        def __await__(self):
            return iter([52])

    async def foo():
        return await lop.Proxy(Awaitable)

    assert run_async(lop.Proxy(foo)) == ([52], None)


def test_await_7(lop):
    class Awaitable:
        def __await__(self):
            yield 42
            return 100

    async def foo():
        return await lop.Proxy(Awaitable)

    assert run_async(lop.Proxy(foo)) == ([42], 100)


def test_await_8(lop):
    class Awaitable:
        pass

    async def foo():
        return await lop.Proxy(Awaitable)

    with pytest.raises(TypeError):
        run_async(lop.Proxy(foo))


def test_await_9(lop):
    def wrap():
        return bar

    async def bar():
        return 42

    async def foo():
        db = {'b': lambda: wrap}

        class DB:
            b = wrap

        return (
            await lop.Proxy(bar)
            + await lop.Proxy(wrap)()
            + await lop.Proxy(lambda: db['b']()()())
            + await lop.Proxy(bar) * 1000
            + await DB.b()()
        )

    async def foo2():
        return -await lop.Proxy(bar)

    assert run_async(lop.Proxy(foo)) == ([], 42168)
    assert run_async(lop.Proxy(foo2)) == ([], -42)


def test_await_10(lop):
    async def baz():
        return 42

    async def bar():
        return lop.Proxy(baz)

    async def foo():
        return await (await lop.Proxy(bar))

    assert run_async(lop.Proxy(foo)) == ([], 42)


def test_await_11(lop):
    def ident(val):
        return val

    async def bar():
        return 'spam'

    async def foo():
        return ident(val=await lop.Proxy(bar))

    async def foo2():
        return await lop.Proxy(bar), 'ham'

    assert run_async(lop.Proxy(foo2)) == ([], ('spam', 'ham'))


def test_await_12(lop):
    async def coro():
        return 'spam'

    c = coro()

    class Awaitable:
        def __await__(self):
            return c

    async def foo():
        return await lop.Proxy(Awaitable)

    with pytest.raises(TypeError, match=r'__await__\(\) returned a coroutine'):
        run_async(lop.Proxy(foo))

    c.close()


def test_await_13(lop):
    class Awaitable:
        def __await__(self):
            return self

    async def foo():
        return await lop.Proxy(Awaitable)

    with pytest.raises(TypeError, match='__await__.*returned non-iterator of type'):
        run_async(lop.Proxy(foo))


def test_await_14(lop):
    class Wrapper:
        # Forces the interpreter to use CoroutineType.__await__
        def __init__(self, coro):
            assert coro.__class__ is types.CoroutineType
            self.coro = coro

        def __await__(self):
            return self.coro.__await__()

    class FutureLike:
        def __await__(self):
            return (yield)

    class Marker(Exception):
        pass

    async def coro1():
        try:
            return await lop.Proxy(FutureLike)
        except ZeroDivisionError:
            raise Marker

    async def coro2():
        return await lop.Proxy(lambda: Wrapper(lop.Proxy(coro1)))

    c = lop.Proxy(coro2)
    c.send(None)
    with pytest.raises(StopIteration, match='spam'):
        c.send('spam')

    c = lop.Proxy(coro2)
    c.send(None)
    with pytest.raises(Marker):
        c.throw(ZeroDivisionError)


def test_await_15(lop):
    @types.coroutine
    def nop():
        yield

    async def coroutine():
        await nop()

    async def waiter(coro):
        await coro

    coro = lop.Proxy(coroutine)
    coro.send(None)

    with pytest.raises(RuntimeError, match='coroutine is being awaited already'):
        waiter(coro).send(None)


def test_await_16(lop):
    # See https://bugs.python.org/issue29600 for details.

    async def f():
        return ValueError()

    async def g():
        try:
            raise KeyError
        except:
            return await lop.Proxy(f)

    _, result = run_async(lop.Proxy(g))
    assert result.__context__ is None


def test_with_1(lop):
    class Manager:
        def __init__(self, name):
            self.name = name

        async def __aenter__(self):
            await AsyncYieldFrom(['enter-1-' + self.name, 'enter-2-' + self.name])
            return self

        async def __aexit__(self, *args):
            await AsyncYieldFrom(['exit-1-' + self.name, 'exit-2-' + self.name])

            if self.name == 'B':
                return True

    async def foo():
        async with lop.Proxy(lambda: Manager('A')) as a, lop.Proxy(lambda: Manager('B')) as b:
            await lop.Proxy(lambda: AsyncYieldFrom([('managers', a.name, b.name)]))
            1 / 0

    f = lop.Proxy(foo)
    result, _ = run_async(f)

    assert result == [
        'enter-1-A',
        'enter-2-A',
        'enter-1-B',
        'enter-2-B',
        ('managers', 'A', 'B'),
        'exit-1-B',
        'exit-2-B',
        'exit-1-A',
        'exit-2-A',
    ]

    async def foo():
        async with lop.Proxy(lambda: Manager('A')) as a, lop.Proxy(lambda: Manager('C')) as c:
            await lop.Proxy(lambda: AsyncYieldFrom([('managers', a.name, c.name)]))
            1 / 0

    with pytest.raises(ZeroDivisionError):
        run_async(lop.Proxy(foo))


@graalpyxfail
def test_with_2(lop):
    class CM:
        def __aenter__(self):
            pass

    body_executed = False

    async def foo():
        async with lop.Proxy(CM):
            body_executed = True

    with pytest.raises(TypeError):
        run_async(lop.Proxy(foo))
    assert not body_executed


def test_with_3(lop):
    class CM:
        def __aexit__(self):
            pass

    body_executed = False

    async def foo():
        async with lop.Proxy(CM):
            body_executed = True

    with pytest.raises(AttributeError, match='__aenter__'):
        run_async(lop.Proxy(foo))
    assert not body_executed


def test_with_4(lop):
    class CM:
        pass

    body_executed = False

    async def foo():
        async with lop.Proxy(CM):
            body_executed = True

    with pytest.raises(AttributeError, match='__aenter__'):
        run_async(lop.Proxy(foo))
    assert not body_executed


def test_with_5(lop):
    # While this test doesn't make a lot of sense,
    # it's a regression test for an early bug with opcodes
    # generation

    class CM:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            pass

    async def func():
        async with lop.Proxy(CM):
            assert (1,) == 1

    with pytest.raises(AssertionError):
        run_async(lop.Proxy(func))


@pypyxfail
@graalpyxfail
def test_with_6(lop):
    class CM:
        def __aenter__(self):
            return 123

        def __aexit__(self, *e):
            return 456

    async def foo():
        async with lop.Proxy(CM):
            pass

    with pytest.raises(TypeError, match="'async with' received an object from __aenter__ that does not implement __await__: int"):
        # it's important that __aexit__ wasn't called
        run_async(lop.Proxy(foo))


@pypyxfail
@graalpyxfail
def test_with_7(lop):
    class CM:
        async def __aenter__(self):
            return self

        def __aexit__(self, *e):
            return 444

    # Exit with exception
    async def foo():
        async with lop.Proxy(CM):
            1 / 0

    try:
        run_async(lop.Proxy(foo))
    except TypeError as exc:
        assert re.search("'async with' received an object from __aexit__ that does not implement __await__: int", exc.args[0])
        assert exc.__context__ is not None
        assert isinstance(exc.__context__, ZeroDivisionError)
    else:
        pytest.fail('invalid asynchronous context manager did not fail')


@pypyxfail
@graalpyxfail
def test_with_8(lop):
    CNT = 0

    class CM:
        async def __aenter__(self):
            return self

        def __aexit__(self, *e):
            return 456

    # Normal exit
    async def foo():
        nonlocal CNT
        async with lop.Proxy(CM):
            CNT += 1

    with pytest.raises(TypeError, match="'async with' received an object from __aexit__ that does not implement __await__: int"):
        run_async(lop.Proxy(foo))
    assert CNT == 1

    # Exit with 'break'
    async def foo():
        nonlocal CNT
        for i in range(2):
            async with lop.Proxy(CM):
                CNT += 1
                break

    with pytest.raises(TypeError, match="'async with' received an object from __aexit__ that does not implement __await__: int"):
        run_async(lop.Proxy(foo))
    assert CNT == 2

    # Exit with 'continue'
    async def foo():
        nonlocal CNT
        for i in range(2):
            async with lop.Proxy(CM):
                CNT += 1
                continue

    with pytest.raises(TypeError, match="'async with' received an object from __aexit__ that does not implement __await__: int"):
        run_async(lop.Proxy(foo))
    assert CNT == 3

    # Exit with 'return'
    async def foo():
        nonlocal CNT
        async with lop.Proxy(CM):
            CNT += 1
            return

    with pytest.raises(TypeError, match="'async with' received an object from __aexit__ that does not implement __await__: int"):
        run_async(lop.Proxy(foo))
    assert CNT == 4


def test_with_9(lop):
    CNT = 0

    class CM:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *e):
            1 / 0

    async def foo():
        nonlocal CNT
        async with lop.Proxy(CM):
            CNT += 1

    with pytest.raises(ZeroDivisionError):
        run_async(lop.Proxy(foo))

    assert CNT == 1


def test_with_10(lop):
    CNT = 0

    class CM:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *e):
            1 / 0

    async def foo():
        nonlocal CNT
        async with lop.Proxy(CM):
            async with lop.Proxy(CM):
                raise RuntimeError

    try:
        run_async(lop.Proxy(foo))
    except ZeroDivisionError as exc:
        assert exc.__context__ is not None
        assert isinstance(exc.__context__, ZeroDivisionError)
        assert isinstance(exc.__context__.__context__, RuntimeError)
    else:
        pytest.fail('exception from __aexit__ did not propagate')


@graalpyxfail
def test_with_11(lop):
    CNT = 0

    class CM:
        async def __aenter__(self):
            raise NotImplementedError

        async def __aexit__(self, *e):
            1 / 0

    async def foo():
        nonlocal CNT
        async with lop.Proxy(CM):
            raise RuntimeError

    try:
        run_async(lop.Proxy(foo))
    except NotImplementedError as exc:
        assert exc.__context__ is None
    else:
        pytest.fail('exception from __aenter__ did not propagate')


def test_with_12(lop):
    CNT = 0

    class CM:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *e):
            return True

    async def foo():
        nonlocal CNT
        async with lop.Proxy(CM) as cm:
            assert cm.__class__ is CM
            raise RuntimeError

    run_async(lop.Proxy(foo))


@graalpyxfail
def test_with_13(lop):
    CNT = 0

    class CM:
        async def __aenter__(self):
            1 / 0

        async def __aexit__(self, *e):
            return True

    async def foo():
        nonlocal CNT
        CNT += 1
        async with lop.Proxy(CM):
            CNT += 1000
        CNT += 10000

    with pytest.raises(ZeroDivisionError):
        run_async(lop.Proxy(foo))
    assert CNT == 1


def test_for_1(lop):
    aiter_calls = 0

    class AsyncIter:
        def __init__(self):
            self.i = 0

        def __aiter__(self):
            nonlocal aiter_calls
            aiter_calls += 1
            return self

        async def __anext__(self):
            self.i += 1

            if not (self.i % 10):
                await lop.Proxy(lambda: AsyncYield(self.i * 10))

            if self.i > 100:
                raise StopAsyncIteration

            return self.i, self.i

    buffer = []

    async def test1():
        async for i1, i2 in lop.Proxy(AsyncIter):
            buffer.append(i1 + i2)

    yielded, _ = run_async(lop.Proxy(test1))
    # Make sure that __aiter__ was called only once
    assert aiter_calls == 1
    assert yielded == [i * 100 for i in range(1, 11)]
    assert buffer == [i * 2 for i in range(1, 101)]

    buffer = []

    async def test2():
        nonlocal buffer
        async for i in lop.Proxy(AsyncIter):
            buffer.append(i[0])
            if i[0] == 20:
                break
        else:
            buffer.append('what?')
        buffer.append('end')

    yielded, _ = run_async(lop.Proxy(test2))
    # Make sure that __aiter__ was called only once
    assert aiter_calls == 2
    assert yielded == [100, 200]
    assert buffer == [i for i in range(1, 21)] + ['end']

    buffer = []

    async def test3():
        nonlocal buffer
        async for i in lop.Proxy(AsyncIter):
            if i[0] > 20:
                continue
            buffer.append(i[0])
        else:
            buffer.append('what?')
        buffer.append('end')

    yielded, _ = run_async(lop.Proxy(test3))
    # Make sure that __aiter__ was called only once
    assert aiter_calls == 3
    assert yielded == [i * 100 for i in range(1, 11)]
    assert buffer == [i for i in range(1, 21)] + ['what?', 'end']


@pypyxfail
def test_for_2(lop):
    tup = (1, 2, 3)
    refs_before = sys.getrefcount(tup)

    async def foo():
        async for i in lop.Proxy(lambda: tup):
            print('never going to happen')

    with pytest.raises(AttributeError, match="'tuple' object has no attribute '__aiter__'"):
        run_async(lop.Proxy(foo))

    assert sys.getrefcount(tup) == refs_before


@pypyxfail
def test_for_3(lop):
    class I:
        def __aiter__(self):
            return self

    aiter = lop.Proxy(I)
    refs_before = sys.getrefcount(aiter)

    async def foo():
        async for i in aiter:
            print('never going to happen')

    with pytest.raises(TypeError):
        run_async(lop.Proxy(foo))

    assert sys.getrefcount(aiter) == refs_before


@pypyxfail
def test_for_4(lop):
    class I:
        def __aiter__(self):
            return self

        def __anext__(self):
            return ()

    aiter = lop.Proxy(I)
    refs_before = sys.getrefcount(aiter)

    async def foo():
        async for i in aiter:
            print('never going to happen')

    with pytest.raises(TypeError, match="async for' received an invalid object.*__anext__.*tuple"):
        run_async(lop.Proxy(foo))

    assert sys.getrefcount(aiter) == refs_before


@pypyxfail
def test_for_6(lop):
    I = 0

    class Manager:
        async def __aenter__(self):
            nonlocal I
            I += 10000

        async def __aexit__(self, *args):
            nonlocal I
            I += 100000

    class Iterable:
        def __init__(self):
            self.i = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.i > 10:
                raise StopAsyncIteration
            self.i += 1
            return self.i

    ##############

    manager = lop.Proxy(Manager)
    iterable = lop.Proxy(Iterable)
    mrefs_before = sys.getrefcount(manager)
    irefs_before = sys.getrefcount(iterable)

    async def main():
        nonlocal I

        async with manager:
            async for i in iterable:
                I += 1
        I += 1000

    with warnings.catch_warnings():
        warnings.simplefilter('error')
        # Test that __aiter__ that returns an asynchronous iterator
        # directly does not throw any warnings.
        run_async(main())
    assert I == 111011

    assert sys.getrefcount(manager) == mrefs_before
    assert sys.getrefcount(iterable) == irefs_before

    ##############

    async def main():
        nonlocal I

        async with lop.Proxy(Manager):
            async for i in lop.Proxy(Iterable):
                I += 1
        I += 1000

        async with lop.Proxy(Manager):
            async for i in lop.Proxy(Iterable):
                I += 1
        I += 1000

    run_async(main())
    assert I == 333033

    ##############

    async def main():
        nonlocal I

        async with lop.Proxy(Manager):
            I += 100
            async for i in lop.Proxy(Iterable):
                I += 1
            else:
                I += 10000000
        I += 1000

        async with lop.Proxy(Manager):
            I += 100
            async for i in lop.Proxy(Iterable):
                I += 1
            else:
                I += 10000000
        I += 1000

    run_async(lop.Proxy(main))
    assert I == 20555255


def test_for_7(lop):
    CNT = 0

    class AI:
        def __aiter__(self):
            1 / 0

    async def foo():
        nonlocal CNT
        async for i in lop.Proxy(AI):
            CNT += 1
        CNT += 10

    with pytest.raises(ZeroDivisionError):
        run_async(lop.Proxy(foo))
    assert CNT == 0


def test_for_8(lop):
    CNT = 0

    class AI:
        def __aiter__(self):
            1 / 0

    async def foo():
        nonlocal CNT
        async for i in lop.Proxy(AI):
            CNT += 1
        CNT += 10

    with pytest.raises(ZeroDivisionError):
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            # Test that if __aiter__ raises an exception it propagates
            # without any kind of warning.
            run_async(lop.Proxy(foo))
    assert CNT == 0


def test_for_11(lop):
    class F:
        def __aiter__(self):
            return self

        def __anext__(self):
            return self

        def __await__(self):
            1 / 0

    async def main():
        async for _ in lop.Proxy(F):
            pass

    with pytest.raises(TypeError, match='an invalid object from __anext__') as c:
        lop.Proxy(main).send(None)

    err = c.value
    assert isinstance(err.__cause__, ZeroDivisionError)


def test_for_tuple(lop):
    class Done(Exception):
        pass

    class AIter(tuple):
        i = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.i >= len(self):
                raise StopAsyncIteration
            self.i += 1
            return self[self.i - 1]

    result = []

    async def foo():
        async for i in lop.Proxy(lambda: AIter([42])):
            result.append(i)
        raise Done

    with pytest.raises(Done):
        lop.Proxy(foo).send(None)
    assert result == [42]


def test_for_stop_iteration(lop):
    class Done(Exception):
        pass

    class AIter(StopIteration):
        i = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.i:
                raise StopAsyncIteration
            self.i += 1
            return self.value

    result = []

    async def foo():
        async for i in lop.Proxy(lambda: AIter(42)):
            result.append(i)
        raise Done

    with pytest.raises(Done):
        lop.Proxy(foo).send(None)
    assert result == [42]


def test_comp_1(lop):
    async def f(i):
        return i

    async def run_list():
        return [await c for c in [lop.Proxy(lambda: f(1)), lop.Proxy(lambda: f(41))]]

    async def run_set():
        return {await c for c in [lop.Proxy(lambda: f(1)), lop.Proxy(lambda: f(41))]}

    async def run_dict1():
        return {await c: 'a' for c in [lop.Proxy(lambda: f(1)), lop.Proxy(lambda: f(41))]}

    async def run_dict2():
        return {i: await c for i, c in enumerate([lop.Proxy(lambda: f(1)), lop.Proxy(lambda: f(41))])}

    assert run_async(run_list()) == ([], [1, 41])
    assert run_async(run_set()) == ([], {1, 41})
    assert run_async(run_dict1()) == ([], {1: 'a', 41: 'a'})
    assert run_async(run_dict2()) == ([], {0: 1, 1: 41})


def test_comp_2(lop):
    async def f(i):
        return i

    async def run_list():
        return [
            s
            for c in [lop.Proxy(lambda: f('')), lop.Proxy(lambda: f('abc')), lop.Proxy(lambda: f('')), lop.Proxy(lambda: f(['de', 'fg']))]
            for s in await c
        ]

    assert run_async(lop.Proxy(run_list)) == ([], ['a', 'b', 'c', 'de', 'fg'])

    async def run_set():
        return {
            d
            for c in [lop.Proxy(lambda: f([lop.Proxy(lambda: f([10, 30])), lop.Proxy(lambda: f([20]))]))]
            for s in await c
            for d in await s
        }

    assert run_async(lop.Proxy(run_set)) == ([], {10, 20, 30})

    async def run_set2():
        return {await s for c in [lop.Proxy(lambda: f([lop.Proxy(lambda: f(10)), lop.Proxy(lambda: f(20))]))] for s in await c}

    assert run_async(lop.Proxy(run_set2)) == ([], {10, 20})


def test_comp_3(lop):
    async def f(it):
        for i in it:
            yield i

    async def run_list():
        return [i + 1 async for i in f([10, 20])]

    assert run_async(run_list()) == ([], [11, 21])

    async def run_set():
        return {i + 1 async for i in f([10, 20])}

    assert run_async(run_set()) == ([], {11, 21})

    async def run_dict():
        return {i + 1: i + 2 async for i in f([10, 20])}

    assert run_async(run_dict()) == ([], {11: 12, 21: 22})

    async def run_gen():
        gen = (i + 1 async for i in f([10, 20]))
        return [g + 100 async for g in gen]

    assert run_async(run_gen()) == ([], [111, 121])


def test_comp_4(lop):
    async def f(it):
        for i in it:
            yield i

    async def run_list():
        return [i + 1 async for i in f([10, 20]) if i > 10]

    assert run_async(run_list()) == ([], [21])

    async def run_set():
        return {i + 1 async for i in f([10, 20]) if i > 10}

    assert run_async(run_set()) == ([], {21})

    async def run_dict():
        return {i + 1: i + 2 async for i in f([10, 20]) if i > 10}

    assert run_async(run_dict()) == ([], {21: 22})

    async def run_gen():
        gen = (i + 1 async for i in f([10, 20]) if i > 10)
        return [g + 100 async for g in gen]

    assert run_async(run_gen()) == ([], [121])


def test_comp_4_2(lop):
    async def f(it):
        for i in it:
            yield i

    async def run_list():
        return [i + 10 async for i in f(range(5)) if 0 < i < 4]

    assert run_async(run_list()) == ([], [11, 12, 13])

    async def run_set():
        return {i + 10 async for i in f(range(5)) if 0 < i < 4}

    assert run_async(run_set()) == ([], {11, 12, 13})

    async def run_dict():
        return {i + 10: i + 100 async for i in f(range(5)) if 0 < i < 4}

    assert run_async(run_dict()) == ([], {11: 101, 12: 102, 13: 103})

    async def run_gen():
        gen = (i + 10 async for i in f(range(5)) if 0 < i < 4)
        return [g + 100 async for g in gen]

    assert run_async(run_gen()) == ([], [111, 112, 113])


def test_comp_5(lop):
    async def f(it):
        for i in it:
            yield i

    async def run_list():
        return [i + 1 for pair in ([10, 20], [30, 40]) if pair[0] > 10 async for i in f(pair) if i > 30]

    assert run_async(run_list()) == ([], [41])


def test_comp_6(lop):
    async def f(it):
        for i in it:
            yield i

    async def run_list():
        return [i + 1 async for seq in f([(10, 20), (30,)]) for i in seq]

    assert run_async(run_list()) == ([], [11, 21, 31])


def test_comp_7(lop):
    async def f():
        yield 1
        yield 2
        raise Exception('aaa')

    async def run_list():
        return [i async for i in f()]

    with pytest.raises(Exception, match='aaa'):
        run_async(run_list())


def test_comp_8(lop):
    async def f():
        return [i for i in [1, 2, 3]]

    assert run_async(f()) == ([], [1, 2, 3])


def test_comp_9(lop):
    async def gen():
        yield 1
        yield 2

    async def f():
        l = [i async for i in gen()]
        return [i for i in l]

    assert run_async(f()) == ([], [1, 2])


def test_comp_10(lop):
    async def f():
        xx = {i for i in [1, 2, 3]}
        return {x: x for x in xx}

    assert run_async(f()) == ([], {1: 1, 2: 2, 3: 3})


def test_copy(lop):
    async def func():
        pass

    coro = func()
    with pytest.raises(TypeError):
        copy.copy(coro)

    aw = coro.__await__()
    try:
        with pytest.raises(TypeError):
            copy.copy(aw)
    finally:
        aw.close()


def test_pickle(lop):
    async def func():
        pass

    coro = func()
    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
        with pytest.raises((TypeError, pickle.PicklingError)):
            pickle.dumps(coro, proto)

    aw = coro.__await__()
    try:
        for proto in range(pickle.HIGHEST_PROTOCOL + 1):
            with pytest.raises((TypeError, pickle.PicklingError)):
                pickle.dumps(aw, proto)
    finally:
        aw.close()


@pytest.mark.skipif('sys.version_info[1] < 8')
def test_for_assign_raising_stop_async_iteration(lop):
    class BadTarget:
        def __setitem__(self, key, value):
            raise StopAsyncIteration(42)

    tgt = BadTarget()

    async def source():
        yield 10

    async def run_for():
        with pytest.raises(StopAsyncIteration) as cm:
            async for tgt[0] in source():
                pass
        assert cm.value.args == (42,)
        return 'end'

    assert run_async(run_for()) == ([], 'end')

    async def run_list():
        with pytest.raises(StopAsyncIteration) as cm:
            return [0 async for tgt[0] in lop.Proxy(source)]
        assert cm.value.args == (42,)
        return 'end'

    assert run_async(run_list()) == ([], 'end')

    async def run_gen():
        gen = (0 async for tgt[0] in lop.Proxy(source))
        a = gen.asend(None)
        with pytest.raises(RuntimeError) as cm:
            await a
        assert isinstance(cm.value.__cause__, StopAsyncIteration)
        assert cm.value.__cause__.args == (42,)
        return 'end'

    assert run_async(run_gen()) == ([], 'end')


@pytest.mark.skipif('sys.version_info[1] < 8')
def test_for_assign_raising_stop_async_iteration_2(lop):
    class BadIterable:
        def __iter__(self):
            raise StopAsyncIteration(42)

    async def badpairs():
        yield BadIterable()

    async def run_for():
        with pytest.raises(StopAsyncIteration) as cm:
            async for i, j in lop.Proxy(badpairs):
                pass
        assert cm.value.args == (42,)
        return 'end'

    assert run_async(run_for()) == ([], 'end')

    async def run_list():
        with pytest.raises(StopAsyncIteration) as cm:
            return [0 async for i, j in badpairs()]
        assert cm.value.args == (42,)
        return 'end'

    assert run_async(run_list()) == ([], 'end')

    async def run_gen():
        gen = (0 async for i, j in badpairs())
        a = gen.asend(None)
        with pytest.raises(RuntimeError) as cm:
            await a
        assert isinstance(cm.value.__cause__, StopAsyncIteration)
        assert cm.value.__cause__.args == (42,)
        return 'end'

    assert run_async(run_gen()) == ([], 'end')


def test_asyncio_1(lop):
    import asyncio

    class MyException(Exception):
        pass

    buffer = []

    class CM:
        async def __aenter__(self):
            buffer.append(1)
            await lop.Proxy(lambda: asyncio.sleep(0.01))
            buffer.append(2)
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            await lop.Proxy(lambda: asyncio.sleep(0.01))
            buffer.append(exc_type.__name__)

    async def f():
        async with lop.Proxy(CM) as c:
            await lop.Proxy(lambda: asyncio.sleep(0.01))
            raise MyException
        buffer.append('unreachable')

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(f())
    except MyException:
        pass
    finally:
        loop.close()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            asyncio.set_event_loop_policy(None)

    assert buffer == [1, 2, 'MyException']
