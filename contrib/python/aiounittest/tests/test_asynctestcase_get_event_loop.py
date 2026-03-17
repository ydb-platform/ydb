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


class TestAsyncCaseWithCustomLoop(aiounittest.AsyncTestCase):

    def get_event_loop(self):
        self.my_loop = asyncio.get_event_loop()
        return self.my_loop

    async def test_await_async_add(self):
        ret = await async_add(1, 5)
        self.assertEqual(ret, 6)
        self.assertFalse(self.my_loop.is_closed())

    if sys.version_info < (3, 11):
        @asyncio.coroutine
        def test_yield_async_add(self):
            ret = yield from async_add(1, 5)
            self.assertEqual(ret, 6)
            self.assertFalse(self.my_loop.is_closed())
