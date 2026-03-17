import asyncio
import sys

from concurrent.futures import ThreadPoolExecutor

import pytest

import janus


class TestMixedMode:
    @pytest.mark.skipif(
        sys.version_info >= (3, 10),
        reason="Python 3.10+ supports delayed initialization",
    )
    def test_ctor_noloop(self):
        with pytest.raises(RuntimeError):
            janus.Queue()

    @pytest.mark.asyncio
    async def test_get_loop_ok(self):
        q = janus.Queue()
        loop = asyncio.get_running_loop()
        assert q._get_loop() is loop
        assert q._loop is loop

    @pytest.mark.asyncio
    async def test_get_loop_different_loop(self):
        q = janus.Queue()
        # emulate binding another loop
        loop = q._loop = asyncio.new_event_loop()
        with pytest.raises(RuntimeError, match="is bound to a different event loop"):
            q._get_loop()
        loop.close()

    @pytest.mark.asyncio
    async def test_maxsize(self):
        q = janus.Queue(5)
        assert 5 == q.maxsize

    @pytest.mark.asyncio
    async def test_maxsize_named_param(self):
        q = janus.Queue(maxsize=7)
        assert 7 == q.maxsize

    @pytest.mark.asyncio
    async def test_maxsize_default(self):
        q = janus.Queue()
        assert 0 == q.maxsize

    @pytest.mark.asyncio
    async def test_unfinished(self):
        q = janus.Queue()
        assert q.sync_q.unfinished_tasks == 0
        assert q.async_q.unfinished_tasks == 0
        q.sync_q.put(1)
        assert q.sync_q.unfinished_tasks == 1
        assert q.async_q.unfinished_tasks == 1
        q.sync_q.get()
        assert q.sync_q.unfinished_tasks == 1
        assert q.async_q.unfinished_tasks == 1
        q.sync_q.task_done()
        assert q.sync_q.unfinished_tasks == 0
        assert q.async_q.unfinished_tasks == 0
        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_put_async_get(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        def threaded():
            for i in range(5):
                q.sync_q.put(i)

        async def go():
            f = loop.run_in_executor(None, threaded)
            for i in range(5):
                val = await q.async_q.get()
                assert val == i

            assert q.async_q.empty()

            await f

        for i in range(3):
            await go()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_put_async_join(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        for i in range(5):
            q.sync_q.put(i)

        async def do_work():
            await asyncio.sleep(0.1)
            while not q.async_q.empty():
                await q.async_q.get()
                q.async_q.task_done()

        task = loop.create_task(do_work())

        async def wait_for_empty_queue():
            await q.async_q.join()
            await task

        await wait_for_empty_queue()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_async_put_sync_get(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        def threaded():
            for i in range(5):
                val = q.sync_q.get()
                assert val == i

        async def go():
            f = loop.run_in_executor(None, threaded)
            for i in range(5):
                await q.async_q.put(i)

            await f
            assert q.async_q.empty()

        for i in range(3):
            await go()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_join_async_done(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        def threaded():
            for i in range(5):
                q.sync_q.put(i)
            q.sync_q.join()

        async def go():
            f = loop.run_in_executor(None, threaded)
            for i in range(5):
                val = await q.async_q.get()
                assert val == i
                q.async_q.task_done()

            assert q.async_q.empty()

            await f

        for i in range(3):
            await go()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_async_join_async_done(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        def threaded():
            for i in range(5):
                val = q.sync_q.get()
                assert val == i
                q.sync_q.task_done()

        async def go():
            f = loop.run_in_executor(None, threaded)
            for i in range(5):
                await q.async_q.put(i)

            await q.async_q.join()

            await f
            assert q.async_q.empty()

        for i in range(3):
            await go()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_wait_without_closing(self):
        q = janus.Queue()

        with pytest.raises(RuntimeError, match="Waiting for non-closed queue"):
            await q.wait_closed()

        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_modifying_forbidden_after_closing(self):
        q = janus.Queue()
        q.close()

        with pytest.raises(
            janus.SyncQueueShutDown
        ):
            q.sync_q.put(5)

        with pytest.raises(
            janus.SyncQueueShutDown
        ):
            q.sync_q.get()

        with pytest.raises(
            janus.AsyncQueueShutDown
        ):
            await q.async_q.put(5)

        with pytest.raises(
            janus.AsyncQueueShutDown
        ):
            q.async_q.put_nowait(5)

        with pytest.raises(
            janus.AsyncQueueShutDown
        ):
            q.async_q.get_nowait()

        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_double_closing(self):
        q = janus.Queue()
        q.close()
        q.close()
        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_closed(self):
        q = janus.Queue()
        assert not q.closed
        assert not q.async_q.closed
        assert not q.sync_q.closed
        q.close()
        await q.wait_closed()
        assert q.closed
        assert q.async_q.closed
        assert q.sync_q.closed

    @pytest.mark.asyncio
    async def test_async_join_after_closing(self):
        q = janus.Queue()
        q.close()
        await asyncio.wait_for(q.async_q.join(), timeout=0.1)

        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_close_after_async_join(self):
        q = janus.Queue()
        q.sync_q.put(1)

        task = asyncio.create_task(q.async_q.join())
        await asyncio.sleep(0.01)  # ensure tasks are blocking

        q.close()
        await asyncio.wait_for(task, timeout=0.1)

        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_sync_join_after_closing(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()
        q.sync_q.put(1)

        q.close()
        await asyncio.wait_for(loop.run_in_executor(None, q.sync_q.join), timeout=0.1)

        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_close_after_sync_join(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()
        q.sync_q.put(1)

        fut = loop.run_in_executor(None, q.sync_q.join)
        await asyncio.sleep(0.1)  # ensure tasks are blocking

        q.close()

        await asyncio.wait_for(fut, timeout=0.1)

        await q.wait_closed()

    @pytest.mark.asyncio
    async def test_put_notifies_sync_not_empty(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        with ThreadPoolExecutor(4) as executor:
            for _ in range(4):
                executor.submit(q.sync_q.get)

            while q._sync_not_empty_waiting != 4:
                await asyncio.sleep(0.001)

            q.sync_q.put_nowait(1)
            q.async_q.put_nowait(2)
            await loop.run_in_executor(executor, q.sync_q.put, 3)
            await q.async_q.put(4)

        assert q.sync_q.empty()
        await q.aclose()

    @pytest.mark.asyncio
    async def test_put_notifies_async_not_empty(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue()

        tasks = [loop.create_task(q.async_q.get()) for _ in range(4)]

        while q._async_not_empty_waiting != 4:
            await asyncio.sleep(0)

        q.sync_q.put_nowait(1)
        q.async_q.put_nowait(2)
        await loop.run_in_executor(None, q.sync_q.put, 3)
        await q.async_q.put(4)

        await asyncio.gather(*tasks)
        assert q.sync_q.empty()
        await q.aclose()

    @pytest.mark.asyncio
    async def test_get_notifies_sync_not_full(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue(2)
        q.sync_q.put_nowait(1)
        q.sync_q.put_nowait(2)

        with ThreadPoolExecutor(4) as executor:
            for _ in range(4):
                executor.submit(q.sync_q.put, object())

            while q._sync_not_full_waiting != 4:
                await asyncio.sleep(0.001)

            q.sync_q.get_nowait()
            q.async_q.get_nowait()
            await loop.run_in_executor(executor, q.sync_q.get)
            await q.async_q.get()

        assert q.sync_q.qsize() == 2
        await q.aclose()

    @pytest.mark.asyncio
    async def test_get_notifies_async_not_full(self):
        loop = asyncio.get_running_loop()
        q = janus.Queue(2)
        q.sync_q.put_nowait(1)
        q.sync_q.put_nowait(2)

        tasks = [loop.create_task(q.async_q.put(object())) for _ in range(4)]

        while q._async_not_full_waiting != 4:
            await asyncio.sleep(0)

        q.sync_q.get_nowait()
        q.async_q.get_nowait()
        await loop.run_in_executor(None, q.sync_q.get)
        await q.async_q.get()

        await asyncio.gather(*tasks)
        assert q.sync_q.qsize() == 2
        await q.aclose()

    @pytest.mark.asyncio
    async def test_wait_closed_with_pending_tasks(self):
        q = janus.Queue()

        async def getter():
            await q.async_q.get()

        task = asyncio.create_task(getter())
        await asyncio.sleep(0.01)
        q.shutdown()
        # q._pending is not empty now
        await q.wait_closed()

        with pytest.raises(janus.AsyncQueueShutDown):
            await task
