import asyncio
import unittest
from aiounittest import async_test


async def add(x, y):
    await asyncio.sleep(0.1)
    return x + y

class MyAsyncTestDecorator(unittest.TestCase):

    @async_test
    async def test_async_add(self):
        ret = await add(5, 6)
        self.assertEqual(ret, 11)

    @unittest.expectedFailure
    @async_test
    async def test_async_add(self):
        ret = await add(5, 6)
        self.assertEqual(ret, 44)

    def test_async_test_with_custom_loop(self):

        async def some_func():
            ret = await add(5, 6)
            assert ret == 11
            return 'OK'

        loop = asyncio.get_event_loop()
        wrapped = async_test(some_func, loop=loop)

        res = wrapped()

        self.assertEqual(res, 'OK')

        # decorator should not change the state (eg close) of the loop if it's custom
        self.assertFalse(loop.is_closed())
