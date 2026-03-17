# Some simple queue module tests, plus some failure conditions
# to ensure the Queue locks remain stable.
import asyncio
import queue
import re
import sys
import threading
import time
from unittest.mock import patch

import pytest

import janus

QUEUE_SIZE = 5


def qfull(q):
    return q._parent._maxsize > 0 and q.qsize() == q._parent._maxsize


# A thread to run a function that unclogs a blocked Queue.
class _TriggerThread(threading.Thread):
    def __init__(self, fn, args):
        self.fn = fn
        self.args = args
        self.startedEvent = threading.Event()
        threading.Thread.__init__(self)

    def run(self):
        # The sleep isn't necessary, but is intended to give the blocking
        # function in the main thread a chance at actually blocking before
        # we unclog it.  But if the sleep is longer than the timeout-based
        # tests wait in their blocking functions, those tests will fail.
        # So we give them much longer timeout values compared to the
        # sleep here (I aimed at 10 seconds for blocking functions --
        # they should never actually wait that long - they should make
        # progress as soon as we call self.fn()).
        time.sleep(0.1)
        self.startedEvent.set()
        self.fn(*self.args)


# Execute a function that blocks, and in a separate thread, a function that
# triggers the release.  Returns the result of the blocking function.  Caution:
# block_func must guarantee to block until trigger_func is called, and
# trigger_func must guarantee to change queue state so that block_func can make
# enough progress to return.  In particular, a block_func that just raises an
# exception regardless of whether trigger_func is called will lead to
# timing-dependent sporadic failures, and one of those went rarely seen but
# undiagnosed for years.  Now block_func must be unexceptional.  If block_func
# is supposed to raise an exception, call do_exceptional_blocking_test()
# instead.


class BlockingTestMixin:
    def do_blocking_test(self, block_func, block_args, trigger_func, trigger_args):
        self.t = _TriggerThread(trigger_func, trigger_args)
        self.t.start()
        self.result = block_func(*block_args)
        # If block_func returned before our thread made the call, we failed!
        if not self.t.startedEvent.is_set():
            pytest.fail("blocking function '%r' appeared not to block" % block_func)
        self.t.join(10)  # make sure the thread terminates
        if self.t.is_alive():
            pytest.fail("trigger function '%r' appeared to not return" % trigger_func)
        return self.result

    # Call this instead if block_func is supposed to raise an exception.
    def do_exceptional_blocking_test(
        self,
        block_func,
        block_args,
        trigger_func,
        trigger_args,
        expected_exception_class,
    ):
        self.t = _TriggerThread(trigger_func, trigger_args)
        self.t.start()
        try:
            try:
                block_func(*block_args)
            except expected_exception_class:
                raise
            else:
                pytest.fail("expected exception of kind %r" % expected_exception_class)
        finally:
            self.t.join(10)  # make sure the thread terminates
            if self.t.is_alive():
                pytest.fail(
                    "trigger function '%r' appeared to not return" % trigger_func
                )
            if not self.t.startedEvent.is_set():
                pytest.fail("trigger thread ended but event never set")


class BaseQueueTestMixin(BlockingTestMixin):
    cum = 0
    cumlock = threading.Lock()

    def simple_queue_test(self, _q):
        q = _q.sync_q
        if q.qsize():
            raise RuntimeError("Call this function with an empty queue")
        assert q.empty()
        assert not q.full()
        # I guess we better check things actually queue correctly a little :)
        q.put(111)
        q.put(333)
        q.put(222)
        target_order = dict(
            Queue=[111, 333, 222],
            LifoQueue=[222, 333, 111],
            PriorityQueue=[111, 222, 333],
        )
        actual_order = [q.get(), q.get(), q.get()]
        assert actual_order == target_order[_q.__class__.__name__]
        for i in range(QUEUE_SIZE - 1):
            q.put(i)
            assert q.qsize()
        assert not qfull(q)
        last = 2 * QUEUE_SIZE
        full = 3 * 2 * QUEUE_SIZE
        q.put(last)
        assert qfull(q)
        assert not q.empty()
        assert q.full()
        try:
            q.put(full, block=0)
            pytest.fail("Didn't appear to block with a full queue")
        except queue.Full:
            pass
        try:
            q.put(full, timeout=0.01)
            pytest.fail("Didn't appear to time-out with a full queue")
        except queue.Full:
            pass
        # Test a blocking put
        self.do_blocking_test(q.put, (full,), q.get, ())
        self.do_blocking_test(q.put, (full, True, 10), q.get, ())
        # Empty it
        for i in range(QUEUE_SIZE):
            q.get()
        assert not q.qsize()
        try:
            q.get(block=0)
            pytest.fail("Didn't appear to block with an empty queue")
        except queue.Empty:
            pass
        try:
            q.get(timeout=0.01)
            pytest.fail("Didn't appear to time-out with an empty queue")
        except queue.Empty:
            pass
        # Test a blocking get
        self.do_blocking_test(q.get, (), q.put, ("empty",))
        self.do_blocking_test(q.get, (True, 10), q.put, ("empty",))

    def worker(self, q):
        try:
            while True:
                x = q.get()
                if x < 0:
                    q.task_done()
                    return
                with self.cumlock:
                    self.cum += x
                q.task_done()
        except Exception as ex:
            from traceback import print_exc

            print_exc(ex)

    def queue_join_test(self, q):
        self.cum = 0
        for i in (0, 1):
            threading.Thread(target=self.worker, args=(q,)).start()
        for i in range(100):
            q.put(i)
        q.join()
        assert self.cum == sum(range(100))
        for i in (0, 1):
            q.put(-1)  # instruct the threads to close
        q.join()  # verify that you can join twice

    @pytest.mark.asyncio
    async def test_queue_task_done(self):
        # Test to make sure a queue task completed successfully.
        _q = self.type2test()
        q = _q.sync_q
        with pytest.raises(
            ValueError, match=re.escape("task_done() called too many times")
        ):
            q.task_done()
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_queue_join(self):
        # Test that a queue join()s successfully, and before anything else
        # (done twice for insurance).
        _q = self.type2test()
        q = _q.sync_q
        self.queue_join_test(q)
        self.queue_join_test(q)
        with pytest.raises(
            ValueError, match=re.escape("task_done() called too many times")
        ):
            q.task_done()
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_simple_queue(self):
        # Do it a couple of times on the same queue.
        # Done twice to make sure works with same instance reused.
        _q = self.type2test(QUEUE_SIZE)
        self.simple_queue_test(_q)
        self.simple_queue_test(_q)
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_negative_timeout_raises_exception(self):
        _q = self.type2test(QUEUE_SIZE)
        q = _q.sync_q
        with pytest.raises(ValueError, match="timeout' must be a non-negative number"):
            q.put(1, timeout=-1)
        with pytest.raises(ValueError, match="timeout' must be a non-negative number"):
            q.get(1, timeout=-1)
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_nowait(self):
        _q = self.type2test(QUEUE_SIZE)
        q = _q.sync_q
        for i in range(QUEUE_SIZE):
            q.put_nowait(1)
        with pytest.raises(queue.Full):
            q.put_nowait(1)

        for i in range(QUEUE_SIZE):
            q.get_nowait()
        with pytest.raises(queue.Empty):
            q.get_nowait()
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_shrinking_queue(self):
        # issue 10110
        _q = self.type2test(3)
        q = _q.sync_q
        q.put(1)
        q.put(2)
        q.put(3)
        with pytest.raises(queue.Full):
            q.put_nowait(4)
        assert q.qsize() == 3
        q._maxsize = 2  # shrink the queue
        with pytest.raises(queue.Full):
            q.put_nowait(4)
        _q.close()
        await _q.wait_closed()

    @pytest.mark.asyncio
    async def test_maxsize(self):
        # Test to make sure a queue task completed successfully.
        _q = self.type2test(5)
        q = _q.sync_q
        assert q.maxsize == 5
        _q.close()
        await _q.wait_closed()


class TestQueue(BaseQueueTestMixin):
    type2test = janus.Queue


class TestLifoQueue(BaseQueueTestMixin):
    type2test = janus.LifoQueue


class TestPriorityQueue(BaseQueueTestMixin):
    type2test = janus.PriorityQueue


# A Queue subclass that can provoke failure at a moment's notice :)
class FailingQueueException(Exception):
    pass


class FailingQueue(janus.Queue):
    def __init__(self, *args, **kwargs):
        self.fail_next_put = False
        self.fail_next_get = False
        super().__init__(*args, **kwargs)

    def _put(self, item):
        if self.fail_next_put:
            self.fail_next_put = False
            raise FailingQueueException("You Lose")
        return super()._put(item)

    def _get(self):
        if self.fail_next_get:
            self.fail_next_get = False
            raise FailingQueueException("You Lose")
        return super()._get()


class TestFailingQueue(BlockingTestMixin):
    def failing_queue_test(self, _q):
        q = _q.sync_q
        if q.qsize():
            raise RuntimeError("Call this function with an empty queue")
        for i in range(QUEUE_SIZE - 1):
            q.put(i)
        # Test a failing non-blocking put.
        _q.fail_next_put = True
        try:
            q.put("oops", block=0)
            pytest.fail("The queue didn't fail when it should have")
        except FailingQueueException:
            pass
        _q.fail_next_put = True
        try:
            q.put("oops", timeout=0.1)
            pytest.fail("The queue didn't fail when it should have")
        except FailingQueueException:
            pass
        q.put("last")
        assert qfull(q)
        # Test a failing blocking put
        _q.fail_next_put = True
        try:
            self.do_blocking_test(q.put, ("full",), q.get, ())
            pytest.fail("The queue didn't fail when it should have")
        except FailingQueueException:
            pass
        # Check the Queue isn't damaged.
        # put failed, but get succeeded - re-add
        q.put("last")
        # Test a failing timeout put
        _q.fail_next_put = True
        try:
            self.do_exceptional_blocking_test(
                q.put, ("full", True, 10), q.get, (), FailingQueueException
            )
            pytest.fail("The queue didn't fail when it should have")
        except FailingQueueException:
            pass
        # Check the Queue isn't damaged.
        # put failed, but get succeeded - re-add
        q.put("last")
        assert qfull(q)
        q.get()
        assert not qfull(q)
        q.put("last")
        assert qfull(q)
        # Test a blocking put
        self.do_blocking_test(q.put, ("full",), q.get, ())
        # Empty it
        for i in range(QUEUE_SIZE):
            q.get()
        assert not q.qsize()
        q.put("first")
        _q.fail_next_get = True
        try:
            q.get()
            pytest.fail("The queue didn't fail when it should have")
        except FailingQueueException:
            pass
        assert q.qsize()
        _q.fail_next_get = True
        try:
            q.get(timeout=0.1)
            pytest.fail("The queue didn't fail when it should have")
        except FailingQueueException:
            pass
        assert q.qsize()
        q.get()
        assert not q.qsize()
        _q.fail_next_get = True
        try:
            self.do_exceptional_blocking_test(
                q.get, (), q.put, ("empty",), FailingQueueException
            )
            pytest.fail("The queue didn't fail when it should have")
        except FailingQueueException:
            pass
        # put succeeded, but get failed.
        assert q.qsize()
        q.get()
        assert not q.qsize()

    @pytest.mark.asyncio
    async def test_failing_queue(self):
        # Test to make sure a queue is functioning correctly.
        # Done twice to the same instance.
        q = FailingQueue(QUEUE_SIZE)
        self.failing_queue_test(q)
        self.failing_queue_test(q)
        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_closed_loop_non_failing(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue(QUEUE_SIZE)
        q = _q.sync_q
        # we are pacthing loop to follow setUp/tearDown agreement
        with patch.object(loop, "is_closed") as func:
            func.return_value = True
            task = loop.create_task(_q.async_q.get())
            await asyncio.sleep(0)
            try:
                q.put_nowait(1)
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            assert func.call_count == 1
        _q.close()
        await _q.wait_closed()


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="Python 3.10+ is required",
)
def test_sync_only_api():
    q = janus.Queue()
    q.sync_q.put(1)
    assert q.sync_q.get() == 1


class TestQueueShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_empty(self):
        _q = janus.Queue()
        q = _q.sync_q

        q.shutdown()
        with pytest.raises(janus.SyncQueueShutDown):
            q.put("data")
        with pytest.raises(janus.SyncQueueShutDown):
            q.get()
        with pytest.raises(janus.SyncQueueShutDown):
            q.get_nowait()

    @pytest.mark.asyncio
    async def test_shutdown_nonempty(self):
        _q = janus.Queue()
        q = _q.sync_q

        q.put("data")
        q.shutdown()
        q.get()
        with pytest.raises(janus.SyncQueueShutDown):
            q.get()

    @pytest.mark.asyncio
    async def test_shutdown_nonempty_get_nowait(self):
        _q = janus.Queue()
        q = _q.sync_q

        q.put("data")
        q.shutdown()
        q.get_nowait()
        with pytest.raises(janus.SyncQueueShutDown):
            q.get_nowait()

    @pytest.mark.asyncio
    async def test_shutdown_immediate(self):
        _q = janus.Queue()
        q = _q.sync_q

        q.put("data")
        q.shutdown(immediate=True)
        with pytest.raises(janus.SyncQueueShutDown):
            q.get()
        with pytest.raises(janus.SyncQueueShutDown):
            q.get_nowait()

    @pytest.mark.asyncio
    async def test_shutdown_immediate_with_undone_tasks(self):
        _q = janus.Queue()
        q = _q.sync_q

        q.put(1)
        q.put(2)
        # artificial .task_done() without .get() for covering specific codeline
        # in .shutdown(True)
        q.task_done()

        q.shutdown(True)

    @pytest.mark.asyncio
    async def test_shutdown_putter(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue(maxsize=1)
        q = _q.sync_q

        q.put(1)

        def putter():
            q.put(2)

        fut = loop.run_in_executor(None, putter)
        # wait for the task start
        await asyncio.sleep(0.01)

        q.shutdown()

        with pytest.raises(janus.SyncQueueShutDown):
            await fut

        await _q.aclose()

    @pytest.mark.asyncio
    async def test_shutdown_many_putters(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue(maxsize=1)
        q = _q.sync_q

        q.put(1)

        def putter(n):
            q.put(n)

        futs = []
        for i in range(2):
            futs.append(loop.run_in_executor(None, putter, i))
        # wait for the task start
        await asyncio.sleep(0.01)

        q.shutdown()

        for fut in futs:
            with pytest.raises(janus.SyncQueueShutDown):
                await fut

        await _q.aclose()

    @pytest.mark.asyncio
    async def test_shutdown_many_putters_with_timeout(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue(maxsize=1)
        q = _q.sync_q

        q.put(1)

        def putter(n):
            q.put(n, timeout=60)

        futs = []
        for i in range(2):
            futs.append(loop.run_in_executor(None, putter, i))
        # wait for the task start
        await asyncio.sleep(0.01)

        q.shutdown()

        for fut in futs:
            with pytest.raises(janus.SyncQueueShutDown):
                await fut

        await _q.aclose()

    @pytest.mark.asyncio
    async def test_shutdown_getter(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue()
        q = _q.sync_q

        def getter():
            q.get()

        fut = loop.run_in_executor(None, getter)
        # wait for the task start
        await asyncio.sleep(0.01)

        q.shutdown()

        with pytest.raises(janus.SyncQueueShutDown):
            await fut

        await _q.aclose()

    @pytest.mark.asyncio
    async def test_shutdown_getter_with_timeout(self):
        loop = asyncio.get_running_loop()
        _q = janus.Queue()
        q = _q.sync_q

        def getter():
            q.get(timeout=60)

        fut = loop.run_in_executor(None, getter)
        # wait for the task start
        await asyncio.sleep(0.01)

        q.shutdown()

        with pytest.raises(janus.SyncQueueShutDown):
            await fut

        await _q.aclose()

    @pytest.mark.asyncio
    async def test_shutdown_early_getter(self):
        _q = janus.Queue()
        q = _q.sync_q

        q.shutdown()

        with pytest.raises(janus.SyncQueueShutDown):
            q.get()

        await _q.aclose()
