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

        @property
        def add_control(self):
            self.control_total += 1
            return self.control_total

        @cached_property_decorator
        def add_cached(self):
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
                thread = Thread(target=self.add_cached)
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()

    return Check


class TestCachedProperty(unittest.TestCase):
    """Tests for cached_property"""

    cached_property_factory = cached_property.cached_property

    def assert_control(self, check, expected):
        """
        Assert that both `add_control` and 'control_total` equal `expected`
        """
        self.assertEqual(expected, check.add_control)
        self.assertEqual(expected, check.control_total)

    def assert_cached(self, check, expected):
        """
        Assert that both `add_cached` and 'cached_total` equal `expected`
        """
        self.assertEqual(expected, check.add_cached)
        self.assertEqual(expected, check.cached_total)

    def test_cached_property(self):
        Check = CheckFactory(self.cached_property_factory)
        check = Check()

        # The control shows that we can continue to add 1
        self.assert_control(check, 1)
        self.assert_control(check, 2)

        # The cached version demonstrates how nothing is added after the first
        self.assert_cached(check, 1)
        self.assert_cached(check, 1)

        # The cache does not expire
        with freeze_time("9999-01-01"):
            self.assert_cached(check, 1)

        # Typically descriptors return themselves if accessed though the class
        # rather than through an instance.
        self.assertTrue(isinstance(Check.add_cached, self.cached_property_factory))

    def test_reset_cached_property(self):
        Check = CheckFactory(self.cached_property_factory)
        check = Check()

        # Run standard cache assertion
        self.assert_cached(check, 1)
        self.assert_cached(check, 1)

        # Clear the cache
        del check.add_cached

        # Value is cached again after the next access
        self.assert_cached(check, 2)
        self.assert_cached(check, 2)

    def test_none_cached_property(self):
        class Check:
            def __init__(self):
                self.cached_total = None

            @self.cached_property_factory
            def add_cached(self):
                return self.cached_total

        self.assert_cached(Check(), None)

    def test_set_cached_property(self):
        Check = CheckFactory(self.cached_property_factory)
        check = Check()
        check.add_cached = "foo"
        self.assertEqual("foo", check.add_cached)
        self.assertEqual(0, check.cached_total)


class TestThreadedCachedProperty(TestCachedProperty):
    """Tests for threaded_cached_property"""

    cached_property_factory = cached_property.threaded_cached_property

    def test_threads(self):
        Check = CheckFactory(self.cached_property_factory, threadsafe=True)
        check = Check()
        num_threads = 5

        # threaded_cached_property_with_ttl is thread-safe
        check.run_threads(num_threads)
        self.assert_cached(check, 1)
        self.assert_cached(check, 1)

        # The cache does not expire
        with freeze_time("9999-01-01"):
            check.run_threads(num_threads)
            self.assert_cached(check, 1)
            self.assert_cached(check, 1)


class TestCachedPropertyWithTTL(TestCachedProperty):
    """Tests for cached_property_with_ttl"""

    cached_property_factory = cached_property.cached_property_with_ttl

    def test_ttl_expiry(self):
        Check = CheckFactory(self.cached_property_factory(ttl=100000))
        check = Check()

        # Run standard cache assertion
        self.assert_cached(check, 1)
        self.assert_cached(check, 1)

        # The cache expires in the future
        with freeze_time("9999-01-01"):
            self.assert_cached(check, 2)
            self.assert_cached(check, 2)

        # Things are not reverted when we are back to the present
        self.assert_cached(check, 2)
        self.assert_cached(check, 2)


class TestThreadedCachedPropertyWithTTL(
    TestThreadedCachedProperty, TestCachedPropertyWithTTL
):
    """Tests for threaded_cached_property_with_ttl"""

    cached_property_factory = cached_property.threaded_cached_property_with_ttl

    def test_threads_ttl_expiry(self):
        Check = CheckFactory(self.cached_property_factory(ttl=100000), threadsafe=True)
        check = Check()
        num_threads = 5

        # Same as in test_threads
        check.run_threads(num_threads)
        self.assert_cached(check, 1)
        self.assert_cached(check, 1)

        # The cache expires in the future
        with freeze_time("9999-01-01"):
            check.run_threads(num_threads)
            self.assert_cached(check, 2)
            self.assert_cached(check, 2)

        # Things are not reverted when we are back to the present
        self.assert_cached(check, 2)
        self.assert_cached(check, 2)
