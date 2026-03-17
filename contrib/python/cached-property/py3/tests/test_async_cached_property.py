import asyncio
import time
import unittest
from threading import Lock, Thread
from freezegun import freeze_time
import cached_property


def CheckFactory(cached_property_decorator, threadsafe=False):
    """
    Create dynamically a Check class whose add_cached method is decorated by
    the cached_property_decorator.
    """

    class Check:
        def __init__(self):
            self.control_total = 0
            self.cached_total = 0
            self.lock = Lock()

        async def add_control(self):  # noqa
            self.control_total += 1
            return self.control_total

        @cached_property_decorator
        async def add_cached(self):
            if threadsafe:
                time.sleep(1)
                # Need to guard this since += isn't atomic.
                with self.lock:
                    self.cached_total += 1
            else:
                self.cached_total += 1

            return self.cached_total

        def run_threads(self, num_threads):
            threads = []
            for _ in range(num_threads):

                def call_add_cached():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self.add_cached)

                thread = Thread(target=call_add_cached)
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()

    return Check


class TestCachedProperty(unittest.IsolatedAsyncioTestCase):
    """Tests for cached_property"""

    cached_property_factory = cached_property.cached_property

    async def assert_control(self, check, expected):
        """
        Assert that both `add_control` and 'control_total` equal `expected`
        """
        self.assertEqual(expected, await check.add_control())
        self.assertEqual(expected, check.control_total)

    async def assert_cached(self, check, expected):
        """
        Assert that both `add_cached` and 'cached_total` equal `expected`
        """
        print("assert_cached", check.add_cached)
        self.assertEqual(expected, await check.add_cached)
        self.assertEqual(expected, check.cached_total)

    async def test_cached_property(self):
        Check = CheckFactory(self.cached_property_factory)
        check = Check()

        # The control shows that we can continue to add 1
        await self.assert_control(check, 1)
        await self.assert_control(check, 2)

        # The cached version demonstrates how nothing is added after the first
        await self.assert_cached(check, 1)
        await self.assert_cached(check, 1)

        # The cache does not expire
        with freeze_time("9999-01-01"):
            await self.assert_cached(check, 1)

        # Typically descriptors return themselves if accessed though the class
        # rather than through an instance.
        self.assertTrue(isinstance(Check.add_cached, self.cached_property_factory))

    async def test_reset_cached_property(self):
        Check = CheckFactory(self.cached_property_factory)
        check = Check()

        # Run standard cache assertion
        await self.assert_cached(check, 1)
        await self.assert_cached(check, 1)

        # Clear the cache
        del check.add_cached

        # Value is cached again after the next access
        await self.assert_cached(check, 2)
        await self.assert_cached(check, 2)

    async def test_none_cached_property(self):
        class Check:
            def __init__(self):
                self.cached_total = None

            @self.cached_property_factory
            async def add_cached(self):
                return self.cached_total

        await self.assert_cached(Check(), None)
