"""Tests for queues.py"""

import asyncio
import time
import re

import pytest

import janus


async def close(_q):
    for i in range(5):
        time.sleep(0.001)
        if not _q._sync_mutex.locked():
            break
    else:
        assert not _q._sync_mutex.locked()

    await _q.aclose()
    assert _q.closed
    assert _q.sync_q.closed
    assert _q.async_q.closed


class TestQueueBasic:
    async def _test_repr_or_str(self, fn, expect_id):
        """Test Queue's repr or str.

        fn is repr or str. expect_id is True if we expect the Queue's id to
        appear in fn(Queue()).
        """

        _q = janus.Queue()
        q = _q.async_q
        assert fn(q).startswith("<Queue")
        id_is_present = hex(id(q)) in fn(q)
        assert expect_id == id_is_present
        loop = asyncio.get_running_loop()

        async def add_getter():
            _q = janus.Queue()
            q = _q.async_q
            # Start a task that waits to get.
            loop.create_task(q.get())
            # Let it start waiting.
            await asyncio.sleep(0.1)
            assert "_getters[1]" in fn(q)
            # resume q.get coroutine to finish generator
            q.put_nowait(0)

        await add_getter()

        async def add_putter():
            _q = janus.Queue(maxsize=1)
            q = _q.async_q
            q.put_nowait(1)
            # Start a task that waits to put.
            loop.create_task(q.put(2))
            # Let it start waiting.
            await asyncio.sleep(0.1)
            assert "_putters[1]" in fn(q)
            # resume q.put coroutine to finish generator
            q.get_nowait()

        await add_putter()

        _q = janus.Queue()
        q = _q.async_q
        q.put_nowait(1)
        assert "_queue=[1]" in fn(q)

    # def test_repr(self):
    #     self._test_repr_or_str(repr, True)

    # def test_str(self):
    #     self._test_repr_or_str(str, False)

    @pytest.mark.asyncio
    async def test_empty(self):
        _q = janus.Queue()
        q = _q.async_q
        assert q.empty()
        q.put_nowait(1)
        assert not q.empty()
        assert 1 == q.get_nowait()
        assert q.empty()

        await close(_q)

    @pytest.mark.asyncio
    async def test_full(self):
        _q = janus.Queue()
        q = _q.async_q
        assert not q.full()

        _q = janus.Queue(maxsize=1)
        q = _q.async_q
        q.put_nowait(1)
        assert q.full()

        await close(_q)

    @pytest.mark.asyncio
    async def test_order(self):
        _q = janus.Queue()
        q = _q.async_q
        for i in [1, 3, 2]:
            q.put_nowait(i)

        items = [q.get_nowait() for _ in range(3)]
        assert [1, 3, 2] == items

        await close(_q)

    @pytest.mark.asyncio
    async def test_maxsize(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue(maxsize=2)
        q = _q.async_q
        assert 2 == q.maxsize
        have_been_put = []

        fut = loop.create_future()

        async def putter():
            for i in range(3):
                await q.put(i)
                have_been_put.append(i)
                if i == q.maxsize - 1:
                    fut.set_result(None)
            return True

        t = loop.create_task(putter())
        await fut

        # The putter is blocked after putting two items.
        assert [0, 1] == have_been_put
        assert 0 == q.get_nowait()

        # Let the putter resume and put last item.
        await t
        assert [0, 1, 2] == have_been_put
        assert 1 == q.get_nowait()
        assert 2 == q.get_nowait()

        await close(_q)


class TestQueueGetTests:
    @pytest.mark.asyncio
    async def test_blocking_get(self):
        _q = janus.Queue()
        q = _q.async_q
        q.put_nowait(1)

        res = await q.get()
        assert 1 == res

        await close(_q)

    @pytest.mark.asyncio
    async def test_get_with_putters(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue(1)
        q = _q.async_q
        q.put_nowait(1)

        fut = loop.create_future()

        async def put():
            t = asyncio.ensure_future(q.put(2))
            await asyncio.sleep(0.01)
            fut.set_result(None)
            return t

        t = await put()

        res = await q.get()
        assert 1 == res

        await t
        assert 1 == q.qsize()

        await close(_q)

    @pytest.mark.asyncio
    async def test_blocking_get_wait(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue()
        q = _q.async_q
        started = asyncio.Event()
        finished = False

        async def queue_get():
            nonlocal finished
            started.set()
            res = await q.get()
            finished = True
            return res

        async def queue_put():
            loop.call_later(0.01, q.put_nowait, 1)
            queue_get_task = loop.create_task(queue_get())
            await started.wait()
            assert not finished
            res = await queue_get_task
            assert finished
            return res

        res = await queue_put()
        assert 1 == res

        await close(_q)

    @pytest.mark.asyncio
    async def test_nonblocking_get(self):
        _q = janus.Queue()
        q = _q.async_q
        q.put_nowait(1)
        assert 1 == q.get_nowait()

        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_nonblocking_get_exception(self):
        _q = janus.Queue()
        pytest.raises(asyncio.QueueEmpty, _q.async_q.get_nowait)

        await close(_q)

    @pytest.mark.asyncio
    async def test_get_cancelled(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue()
        q = _q.async_q

        async def queue_get():
            return await asyncio.wait_for(q.get(), 0.051)

        async def test():
            get_task = loop.create_task(queue_get())
            await asyncio.sleep(0.01)  # let the task start
            q.put_nowait(1)
            return await get_task

        assert 1 == await test()

        await close(_q)

    @pytest.mark.asyncio
    async def test_get_cancelled_race(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue()
        q = _q.async_q

        f1 = loop.create_future()

        async def g1():
            f1.set_result(None)
            await q.get()

        t1 = loop.create_task(g1())
        t2 = loop.create_task(q.get())

        await f1
        await asyncio.sleep(0.01)
        t1.cancel()

        with pytest.raises(asyncio.CancelledError):
            await t1
        assert t1.done()
        q.put_nowait("a")

        await t2
        assert t2.result() == "a"

        await close(_q)

    @pytest.mark.asyncio
    async def test_get_with_waiting_putters(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue(maxsize=1)
        q = _q.async_q

        loop.create_task(q.put("a"))
        loop.create_task(q.put("b"))

        await asyncio.sleep(0.01)

        assert await q.get() == "a"
        assert await q.get() == "b"

        await close(_q)


class TestQueuePut:
    @pytest.mark.asyncio
    async def test_blocking_put(self):
        _q = janus.Queue()
        q = _q.async_q

        # No maxsize, won't block.
        await q.put(1)

        await close(_q)

    @pytest.mark.asyncio
    async def test_blocking_put_wait(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue(maxsize=1)
        q = _q.async_q
        started = asyncio.Event()
        finished = False

        async def queue_put():
            nonlocal finished
            started.set()
            await q.put(1)
            await q.put(2)
            finished = True

        async def queue_get():
            loop.call_later(0.01, q.get_nowait)
            queue_put_task = loop.create_task(queue_put())
            await started.wait()
            assert not finished
            await queue_put_task
            assert finished

        await queue_get()

        await close(_q)

    @pytest.mark.asyncio
    async def test_nonblocking_put(self):
        _q = janus.Queue()
        q = _q.async_q
        q.put_nowait(1)
        assert 1 == q.get_nowait()

        await close(_q)

    @pytest.mark.asyncio
    async def test_nonblocking_put_exception(self):
        _q = janus.Queue(maxsize=1)
        q = _q.async_q
        q.put_nowait(1)
        pytest.raises(asyncio.QueueFull, q.put_nowait, 2)

        await close(_q)

    @pytest.mark.asyncio
    async def test_float_maxsize(self):
        _q = janus.Queue(maxsize=1.3)
        q = _q.async_q
        q.put_nowait(1)
        q.put_nowait(2)
        assert q.full()
        pytest.raises(asyncio.QueueFull, q.put_nowait, 3)

        _q.close()
        await _q.wait_closed()

        _q = janus.Queue(maxsize=1.3)
        q = _q.async_q

        async def queue_put():
            await q.put(1)
            await q.put(2)
            assert q.full()

        await queue_put()

        await close(_q)

    @pytest.mark.asyncio
    async def test_put_cancelled(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue()
        q = _q.async_q

        async def queue_put():
            await q.put(1)
            return True

        async def test():
            return await q.get()

        t = loop.create_task(queue_put())
        assert 1 == await test()
        assert t.done()
        assert t.result()

        await close(_q)

    @pytest.mark.asyncio
    async def test_put_cancelled_race(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue(maxsize=1)
        q = _q.async_q

        put_a = loop.create_task(q.put("a"))
        put_b = loop.create_task(q.put("b"))
        put_c = loop.create_task(q.put("X"))

        await put_a
        assert not put_b.done()

        put_c.cancel()

        with pytest.raises(asyncio.CancelledError):
            await put_c

        a = await q.get()
        assert a == "a"
        b = await q.get()
        assert b == "b"
        assert put_b.done()

        assert q.qsize() == 0

        await close(_q)

    @pytest.mark.asyncio
    async def test_put_with_waiting_getters(self):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        async def go():
            fut.set_result(None)
            ret = await q.get()
            return ret

        async def put():
            await q.put("a")

        _q = janus.Queue()
        q = _q.async_q
        t = loop.create_task(go())
        await fut
        await put()
        assert await t == "a"

        await close(_q)


class TestQueueShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_empty(self):
        _q = janus.Queue()
        q = _q.async_q

        q.shutdown()
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.put("data")
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.get()
        with pytest.raises(janus.AsyncQueueShutDown):
            q.get_nowait()

    @pytest.mark.asyncio
    async def test_shutdown_nonempty(self):
        _q = janus.Queue()
        q = _q.async_q

        await q.put("data")
        q.shutdown()
        await q.get()
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.get()

    @pytest.mark.asyncio
    async def test_shutdown_nonempty_get_nowait(self):
        _q = janus.Queue()
        q = _q.async_q

        await q.put("data")
        q.shutdown()
        q.get_nowait()
        with pytest.raises(janus.AsyncQueueShutDown):
            q.get_nowait()

    @pytest.mark.asyncio
    async def test_shutdown_immediate(self):
        _q = janus.Queue()
        q = _q.async_q

        await q.put("data")
        q.shutdown(immediate=True)
        with pytest.raises(janus.AsyncQueueShutDown):
            await q.get()
        with pytest.raises(janus.AsyncQueueShutDown):
            q.get_nowait()

    @pytest.mark.asyncio
    async def test_shutdown_immediate_with_undone_tasks(self):
        _q = janus.Queue()
        q = _q.async_q

        await q.put(1)
        await q.put(2)
        # artificial .task_done() without .get() for covering specific codeline
        # in .shutdown(True)
        q.task_done()

        q.shutdown(True)
        await close(_q)

    @pytest.mark.asyncio
    async def test_shutdown_putter(self):
        _q = janus.Queue(maxsize=1)
        q = _q.async_q

        await q.put(1)

        async def putter():
            await q.put(2)

        task = asyncio.create_task(putter())
        # wait for the task start
        await asyncio.sleep(0.01)

        q.shutdown()

        with pytest.raises(janus.AsyncQueueShutDown):
            await task

        await close(_q)

    @pytest.mark.asyncio
    async def test_shutdown_many_putters(self):
        _q = janus.Queue(maxsize=1)
        q = _q.async_q

        await q.put(1)

        async def putter(n):
            await q.put(n)

        tasks = []
        for i in range(2):
            tasks.append(asyncio.create_task(putter(i)))
        # wait for the task start
        await asyncio.sleep(0.01)

        q.shutdown()

        for task in tasks:
            with pytest.raises(janus.AsyncQueueShutDown):
                await task

        await close(_q)

    @pytest.mark.asyncio
    async def test_shutdown_getter(self):
        _q = janus.Queue()
        q = _q.async_q

        async def getter():
            await q.get()

        task = asyncio.create_task(getter())
        # wait for the task start
        await asyncio.sleep(0.01)

        q.shutdown()

        with pytest.raises(janus.AsyncQueueShutDown):
            await task

        await close(_q)

    @pytest.mark.asyncio
    async def test_shutdown_early_getter(self):
        _q = janus.Queue()
        q = _q.async_q

        q.shutdown()

        with pytest.raises(janus.AsyncQueueShutDown):
            await q.get()

        await close(_q)


class TestLifoQueue:
    @pytest.mark.asyncio
    async def test_order(self):
        _q = janus.LifoQueue()
        q = _q.async_q
        for i in [1, 3, 2]:
            q.put_nowait(i)

        items = [q.get_nowait() for _ in range(3)]
        assert [2, 3, 1] == items

        await close(_q)


class TestPriorityQueue:
    @pytest.mark.asyncio
    async def test_order(self):
        _q = janus.PriorityQueue()
        q = _q.async_q
        for i in [1, 3, 2]:
            q.put_nowait(i)

        items = [q.get_nowait() for _ in range(3)]
        assert [1, 2, 3] == items

        await close(_q)


class _QueueJoinTestMixin:
    q_class = None

    @pytest.mark.asyncio
    async def test_task_done_underflow(self):
        _q = self.q_class()
        q = _q.async_q
        with pytest.raises(
            ValueError, match=re.escape("task_done() called too many times")
        ):
            q.task_done()

        await close(_q)

    @pytest.mark.asyncio
    async def test_task_done(self):
        loop = asyncio.get_running_loop()
        _q = self.q_class()
        q = _q.async_q
        for i in range(100):
            q.put_nowait(i)

        accumulator = 0

        # Two workers get items from the queue and call task_done after each.
        # Join the queue and assert all items have been processed.
        running = True

        async def worker():
            nonlocal accumulator

            while running:
                item = await q.get()
                accumulator += item
                q.task_done()

        async def test():
            tasks = [loop.create_task(worker()) for index in range(2)]

            await q.join()
            return tasks

        tasks = await test()
        assert sum(range(100)) == accumulator

        # close running generators
        running = False
        for i in range(len(tasks)):
            q.put_nowait(0)
        await asyncio.wait(tasks)

        await close(_q)

    @pytest.mark.asyncio
    async def test_join_empty_queue(self):
        _q = self.q_class()
        q = _q.async_q

        # Test that a queue join()s successfully, and before anything else
        # (done twice for insurance).

        async def join():
            await q.join()
            await q.join()

        await join()

        await close(_q)


class TestQueueJoin(_QueueJoinTestMixin):
    q_class = janus.Queue


class TestLifoQueueJoin(_QueueJoinTestMixin):
    q_class = janus.LifoQueue


class TestPriorityQueueJoin(_QueueJoinTestMixin):
    q_class = janus.PriorityQueue
