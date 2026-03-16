import aiounittest
import asyncio
import time
import sys
from unittest import expectedFailure


async def async_add(x, y, delay=0.1):
    await asyncio.sleep(delay)
    return x + y


def sync_add(x, y, delay=0.1):
    time.sleep(delay)
    return x + y

async def async_one():
    await async_nested_exc()

async def async_nested_exc():
    await asyncio.sleep(0.1)
    raise Exception('Test')


class TestAsyncCase(aiounittest.AsyncTestCase):

    def test_sync_add(self):
        ret = sync_add(1, 5)
        self.assertEqual(ret, 6)

    def test_sync_async_add(self):
        loop = asyncio.get_event_loop()
        ret = loop.run_until_complete(async_add(1,5))
        loop.close()
        self.assertEqual(ret, 6)

        # Set a new event loop, since we closed the old one
        asyncio.set_event_loop(asyncio.new_event_loop())

    async def test_await_async_add(self):
        ret = await async_add(1, 5)
        self.assertEqual(ret, 6)

    if sys.version_info < (3, 11):
        @asyncio.coroutine
        def test_yield_async_add(self):
            ret = yield from async_add(1, 5)
            self.assertEqual(ret, 6)

    async def test_await_async_fail(self):
        with self.assertRaises(Exception) as e:
            await async_one()

    @expectedFailure
    async def test_failure_await_async_add(self):
        ret = await async_add(1, 5)
        self.assertEqual(ret, -1)

    if sys.version_info < (3, 11):
        @expectedFailure
        @asyncio.coroutine
        def test_yield_async_add(self):
            ret = yield from async_add(1, 5)
            self.assertEqual(ret, -1)
