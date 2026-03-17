import asyncio

import pytest

from aiopg.utils import ClosableQueue


async def test_closable_queue_noclose(loop):
    the_queue = asyncio.Queue()
    queue = ClosableQueue(the_queue, loop)
    assert queue.empty()
    assert queue.qsize() == 0

    await the_queue.put(1)
    assert not queue.empty()
    assert queue.qsize() == 1
    v = await queue.get()
    assert v == 1

    await the_queue.put(2)
    v = queue.get_nowait()
    assert v == 2


async def test_closable_queue_close(loop):
    the_queue = asyncio.Queue()
    queue = ClosableQueue(the_queue, loop)
    v1 = None

    async def read():
        nonlocal v1
        v1 = await queue.get()
        await queue.get()

    reader = loop.create_task(read())
    await the_queue.put(1)
    await asyncio.sleep(0.1)
    assert v1 == 1

    queue.close(RuntimeError("connection closed"))
    with pytest.raises(RuntimeError) as excinfo:
        await reader
    assert excinfo.value.args == ("connection closed",)


async def test_closable_queue_close_get_nowait(loop):
    the_queue = asyncio.Queue()
    queue = ClosableQueue(the_queue, loop)

    await the_queue.put(1)
    queue.close(RuntimeError("connection closed"))

    # even when the queue is closed, while there are items in the queu, we
    # allow reading them.
    assert queue.get_nowait() == 1

    # when there are no more items in the queue, if there is a close exception
    # then it will get raises here
    with pytest.raises(RuntimeError) as excinfo:
        queue.get_nowait()
    assert excinfo.value.args == ("connection closed",)


async def test_closable_queue_get_nowait_noclose(loop):
    the_queue = asyncio.Queue()
    queue = ClosableQueue(the_queue, loop)
    await the_queue.put(1)
    assert queue.get_nowait() == 1
    with pytest.raises(asyncio.QueueEmpty):
        queue.get_nowait()


async def test_closable_queue_get_cancellation(loop):
    queue = ClosableQueue(asyncio.Queue(), loop)
    get_task = loop.create_task(queue.get())
    await asyncio.sleep(0.1)
    get_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await get_task
