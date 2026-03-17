# -*- coding: utf-8 -*-
"""
The same tests as in :mod:`.test_async_cached_property`, but with the old
yield from instead of the new async/await syntax. Used to test Python 3.4
compatibility which has asyncio but doesn't have async/await yet.
"""

import unittest
import asyncio
from freezegun import freeze_time

import cached_property


def unittest_run_loop(f):
    def wrapper(*args, **kwargs):
        coro = asyncio.coroutine(f)
        future = coro(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)

    return wrapper


def CheckFactory(cached_property_decorator):
    """
    Create dynamically a Check class whose add_cached method is decorated by
    the cached_property_decorator.
    """

    class Check(object):
        def __init__(self):
            self.control_total = 0
            self.cached_total = 0

        @asyncio.coroutine
        def add_control(self):
            self.control_total += 1
            return self.control_total

        @cached_property_decorator
        @asyncio.coroutine
        def add_cached(self):
            self.cached_total += 1
            return self.cached_total

    return Check


class TestCachedProperty(unittest.TestCase):
    """Tests for cached_property"""

    cached_property_factory = cached_property.cached_property

    @asyncio.coroutine
    def assert_control(self, check, expected):
        """
        Assert that both `add_control` and 'control_total` equal `expected`
        """
        value = yield from check.add_control()  # noqa
        self.assertEqual(value, expected)
        self.assertEqual(check.control_total, expected)

    @asyncio.coroutine
    def assert_cached(self, check, expected):
        """
        Assert that both `add_cached` and 'cached_total` equal `expected`
        """
        print("assert_cached", check.add_cached)
        value = yield from check.add_cached
        self.assertEqual(value, expected)
        self.assertEqual(check.cached_total, expected)

    @unittest_run_loop
    @asyncio.coroutine
    def test_cached_property(self):
        Check = CheckFactory(self.cached_property_factory)
        check = Check()

        # The control shows that we can continue to add 1
        yield from self.assert_control(check, 1)
        yield from self.assert_control(check, 2)

        # The cached version demonstrates how nothing is added after the first
        yield from self.assert_cached(check, 1)
        yield from self.assert_cached(check, 1)

        # The cache does not expire
        with freeze_time("9999-01-01"):
            yield from self.assert_cached(check, 1)

        # Typically descriptors return themselves if accessed though the class
        # rather than through an instance.
        self.assertTrue(isinstance(Check.add_cached, self.cached_property_factory))

    @unittest_run_loop
    @asyncio.coroutine
    def test_reset_cached_property(self):
        Check = CheckFactory(self.cached_property_factory)
        check = Check()

        # Run standard cache assertion
        yield from self.assert_cached(check, 1)
        yield from self.assert_cached(check, 1)

        # Clear the cache
        del check.add_cached

        # Value is cached again after the next access
        yield from self.assert_cached(check, 2)
        yield from self.assert_cached(check, 2)

    @unittest_run_loop
    @asyncio.coroutine
    def test_none_cached_property(self):
        class Check(object):
            def __init__(self):
                self.cached_total = None

            @self.cached_property_factory
            @asyncio.coroutine
            def add_cached(self):
                return self.cached_total

        yield from self.assert_cached(Check(), None)
