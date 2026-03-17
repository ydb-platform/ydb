import asyncio
import sys
import janus

if sys.version_info >= (3, 11):
    from asyncio import Runner
else:
    from backports.asyncio.runner import Runner


def test_bench_sync_put_async_get(benchmark):
    q: janus.Queue

    async def init():
        nonlocal q
        q = janus.Queue()

    def threaded():
        for i in range(5):
            q.sync_q.put(i)

    async def go():
        for i in range(100):
            f = asyncio.get_running_loop().run_in_executor(None, threaded)
            for i in range(5):
                val = await q.async_q.get()
                assert val == i

            await f
            assert q.async_q.empty()

    async def finish():
        q.close()
        await q.wait_closed()

    with Runner(debug=True) as runner:
        runner.run(init())

        @benchmark
        def _run():
            runner.run(go())

        runner.run(finish())


def test_bench_sync_put_async_join(benchmark):
    q: janus.Queue

    async def init():
        nonlocal q
        q = janus.Queue()

    async def go():
        for i in range(100):
            for i in range(5):
                q.sync_q.put(i)

            async def do_work():
                await asyncio.sleep(0.01)
                while not q.async_q.empty():
                    await q.async_q.get()
                    q.async_q.task_done()

            task = asyncio.create_task(do_work())

            await q.async_q.join()
            await task

    async def finish():
        q.close()
        await q.wait_closed()

    with Runner(debug=True) as runner:
        runner.run(init())

        @benchmark
        def _run():
            runner.run(go())

        runner.run(finish())


def test_bench_async_put_sync_get(benchmark):
    q: janus.Queue

    async def init():
        nonlocal q
        q = janus.Queue()

    def threaded():
        for i in range(5):
            val = q.sync_q.get()
            assert val == i

    async def go():
        for i in range(100):
            f = asyncio.get_running_loop().run_in_executor(None, threaded)
            for i in range(5):
                await q.async_q.put(i)

            await f
            assert q.async_q.empty()

    async def finish():
        q.close()
        await q.wait_closed()

    with Runner(debug=True) as runner:
        runner.run(init())

        @benchmark
        def _run():
            runner.run(go())

        runner.run(finish())


def test_sync_join_async_done(benchmark):
    q: janus.Queue

    async def init():
        nonlocal q
        q = janus.Queue()

    def threaded():
        for i in range(5):
            q.sync_q.put(i)
        q.sync_q.join()

    async def go():
        for i in range(100):
            f = asyncio.get_running_loop().run_in_executor(None, threaded)
            for i in range(5):
                val = await q.async_q.get()
                assert val == i
                q.async_q.task_done()

            await f
            assert q.async_q.empty()

    async def finish():
        q.close()
        await q.wait_closed()

    with Runner(debug=True) as runner:
        runner.run(init())

        @benchmark
        def _run():
            runner.run(go())

        runner.run(finish())
