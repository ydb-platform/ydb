#-*- coding:utf-8 -*-

import unittest
from contextlib import contextmanager
from datetime import datetime
from time import sleep

import mock
from tornado import gen
from tornado import testing

from pybreaker import *


class CircuitBreakerStorageBasedTestCase(object):
    """
    Mix in to test against different storage backings. Depends on
    `self.breaker` and `self.breaker_kwargs`.
    """

    def test_successful_call(self):
        """CircuitBreaker: it should keep the circuit closed after a successful
        call.
        """
        def func(): return True
        self.assertTrue(self.breaker.call(func))
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual('closed', self.breaker.current_state)

    def test_one_failed_call(self):
        """CircuitBreaker: it should keep the circuit closed after a few
        failures.
        """
        def func(): raise NotImplementedError()
        self.assertRaises(NotImplementedError, self.breaker.call, func)
        self.assertEqual(1, self.breaker.fail_counter)
        self.assertEqual('closed', self.breaker.current_state)

    def test_one_successful_call_after_failed_call(self):
        """CircuitBreaker: it should keep the circuit closed after few mixed
        outcomes.
        """
        def suc(): return True
        def err(): raise NotImplementedError()

        self.assertRaises(NotImplementedError, self.breaker.call, err)
        self.assertEqual(1, self.breaker.fail_counter)

        self.assertTrue(self.breaker.call(suc))
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual('closed', self.breaker.current_state)

    def test_several_failed_calls(self):
        """CircuitBreaker: it should open the circuit after many failures.
        """
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)
        def func(): raise NotImplementedError()

        self.assertRaises(NotImplementedError, self.breaker.call, func)
        self.assertRaises(NotImplementedError, self.breaker.call, func)

        # Circuit should open
        self.assertRaises(CircuitBreakerError, self.breaker.call, func)
        self.assertEqual(3, self.breaker.fail_counter)
        self.assertEqual('open', self.breaker.current_state)

    def test_traceback_in_circuitbreaker_error(self):
        """CircuitBreaker: it should open the circuit after many failures.
        """
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)
        def func(): raise NotImplementedError()

        self.assertRaises(NotImplementedError, self.breaker.call, func)
        self.assertRaises(NotImplementedError, self.breaker.call, func)

        # Circuit should open
        try:
            self.breaker.call(func)
            self.fail('CircuitBreakerError should throw')
        except CircuitBreakerError as e:
            import traceback
            self.assertIn('NotImplementedError', traceback.format_exc())
        self.assertEqual(3, self.breaker.fail_counter)
        self.assertEqual('open', self.breaker.current_state)

    def test_failed_call_after_timeout(self):
        """CircuitBreaker: it should half-open the circuit after timeout.
        """
        self.breaker = CircuitBreaker(fail_max=3, reset_timeout=0.5, **self.breaker_kwargs)
        def func(): raise NotImplementedError()

        self.assertRaises(NotImplementedError, self.breaker.call, func)
        self.assertRaises(NotImplementedError, self.breaker.call, func)
        self.assertEqual('closed', self.breaker.current_state)

        # Circuit should open
        self.assertRaises(CircuitBreakerError, self.breaker.call, func)
        self.assertEqual(3, self.breaker.fail_counter)

        # Wait for timeout
        sleep(0.6)

        # Circuit should open again
        self.assertRaises(CircuitBreakerError, self.breaker.call, func)
        self.assertEqual(4, self.breaker.fail_counter)
        self.assertEqual('open', self.breaker.current_state)

    def test_successful_after_timeout(self):
        """CircuitBreaker: it should close the circuit when a call succeeds
        after timeout. The successful function should only be called once.
        """
        self.breaker = CircuitBreaker(fail_max=3, reset_timeout=1, **self.breaker_kwargs)

        suc = mock.MagicMock(return_value=True)
        def err(): raise NotImplementedError()

        self.assertRaises(NotImplementedError, self.breaker.call, err)
        self.assertRaises(NotImplementedError, self.breaker.call, err)
        self.assertEqual('closed', self.breaker.current_state)

        # Circuit should open
        self.assertRaises(CircuitBreakerError, self.breaker.call, err)
        self.assertRaises(CircuitBreakerError, self.breaker.call, suc)
        self.assertEqual(3, self.breaker.fail_counter)

        # Wait for timeout, at least a second since redis rounds to a second
        sleep(2)

        # Circuit should close again
        self.assertTrue(self.breaker.call(suc))
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual('closed', self.breaker.current_state)
        self.assertEqual(1, suc.call_count)

    def test_failed_call_when_halfopen(self):
        """CircuitBreaker: it should open the circuit when a call fails in
        half-open state.
        """
        def fun(): raise NotImplementedError()

        self.breaker.half_open()
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual('half-open', self.breaker.current_state)

        # Circuit should open
        self.assertRaises(CircuitBreakerError, self.breaker.call, fun)
        self.assertEqual(1, self.breaker.fail_counter)
        self.assertEqual('open', self.breaker.current_state)

    def test_successful_call_when_halfopen(self):
        """CircuitBreaker: it should close the circuit when a call succeeds in
        half-open state.
        """
        def fun(): return True

        self.breaker.half_open()
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual('half-open', self.breaker.current_state)

        # Circuit should open
        self.assertTrue(self.breaker.call(fun))
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual('closed', self.breaker.current_state)

    def test_close(self):
        """CircuitBreaker: it should allow the circuit to be closed manually.
        """
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)
        def func(): raise NotImplementedError()

        self.assertRaises(NotImplementedError, self.breaker.call, func)
        self.assertRaises(NotImplementedError, self.breaker.call, func)

        # Circuit should open
        self.assertRaises(CircuitBreakerError, self.breaker.call, func)
        self.assertRaises(CircuitBreakerError, self.breaker.call, func)
        self.assertEqual(3, self.breaker.fail_counter)
        self.assertEqual('open', self.breaker.current_state)

        # Circuit should close again
        self.breaker.close()
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual('closed', self.breaker.current_state)

    def test_transition_events(self):
        """CircuitBreaker: it should call the appropriate functions on every
        state transition.
        """
        class Listener(CircuitBreakerListener):
            def __init__(self):
                self.out = ''

            def state_change(self, cb, old_state, new_state):
                assert cb
                if old_state: self.out += old_state.name
                if new_state: self.out += '->' + new_state.name
                self.out += ','

        listener = Listener()
        self.breaker = CircuitBreaker(listeners=(listener,), **self.breaker_kwargs)
        self.assertEqual('closed', self.breaker.current_state)

        self.breaker.open()
        self.assertEqual('open', self.breaker.current_state)

        self.breaker.half_open()
        self.assertEqual('half-open', self.breaker.current_state)

        self.breaker.close()
        self.assertEqual('closed', self.breaker.current_state)

        self.assertEqual('closed->open,open->half-open,half-open->closed,', \
                         listener.out)

    def test_call_events(self):
        """CircuitBreaker: it should call the appropriate functions on every
        successful/failed call.
        """
        self.out = ''

        def suc(): return True
        def err(): raise NotImplementedError()

        class Listener(CircuitBreakerListener):
            def __init__(self):
                self.out = ''
            def before_call(self, cb, func, *args, **kwargs):
                assert cb
                self.out += '-'
            def success(self, cb):
                assert cb
                self.out += 'success'
            def failure(self, cb, exc):
                assert cb; assert exc
                self.out += 'failure'

        listener = Listener()
        self.breaker = CircuitBreaker(listeners=(listener,), **self.breaker_kwargs)

        self.assertTrue(self.breaker.call(suc))
        self.assertRaises(NotImplementedError, self.breaker.call, err)
        self.assertEqual('-success-failure', listener.out)

    def test_generator(self):
        """CircuitBreaker: it should inspect generator values.
        """
        @self.breaker
        def suc(value):
            "Docstring"
            yield value

        @self.breaker
        def err(value):
            "Docstring"
            x = yield value
            raise NotImplementedError(x)

        s = suc(True)
        e = err(True)
        next(e)

        self.assertRaises(NotImplementedError, e.send, True)
        self.assertEqual(1, self.breaker.fail_counter)
        self.assertTrue(next(s))
        self.assertRaises((StopIteration, RuntimeError), lambda: next(s))
        self.assertEqual(0, self.breaker.fail_counter)

    def test_contextmanager(self):
        """CircuitBreaker: it should catch in a with statement
        """
        class Foo:
            @contextmanager
            @self.breaker
            def wrapper(self):
                try:
                    yield
                except NotImplementedError as e:
                    raise ValueError()

            def foo(self):
                with self.wrapper():
                    raise NotImplementedError()


        try:
            Foo().foo()
        except ValueError as e:
            self.assertTrue(isinstance(e, ValueError))


class CircuitBreakerConfigurationTestCase(object):
    """
    Tests for the CircuitBreaker class.
    """

    def test_default_state(self):
        """CircuitBreaker: it should get initial state from state_storage.
        """
        for state in (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN):
            storage = CircuitMemoryStorage(state)
            breaker = CircuitBreaker(state_storage=storage)
            self.assertEqual(breaker.state.name, state)

    def test_default_params(self):
        """CircuitBreaker: it should define smart defaults.
        """
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual(60, self.breaker.reset_timeout)
        self.assertEqual(5, self.breaker.fail_max)
        self.assertEqual('closed', self.breaker.current_state)
        self.assertEqual((), self.breaker.excluded_exceptions)
        self.assertEqual((), self.breaker.listeners)
        self.assertEqual('memory', self.breaker._state_storage.name)

    def test_new_with_custom_reset_timeout(self):
        """CircuitBreaker: it should support a custom reset timeout value.
        """
        self.breaker = CircuitBreaker(reset_timeout=30)
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual(30, self.breaker.reset_timeout)
        self.assertEqual(5, self.breaker.fail_max)
        self.assertEqual((), self.breaker.excluded_exceptions)
        self.assertEqual((), self.breaker.listeners)
        self.assertEqual('memory', self.breaker._state_storage.name)

    def test_new_with_custom_fail_max(self):
        """CircuitBreaker: it should support a custom maximum number of
        failures.
        """
        self.breaker = CircuitBreaker(fail_max=10)
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual(60, self.breaker.reset_timeout)
        self.assertEqual(10, self.breaker.fail_max)
        self.assertEqual((), self.breaker.excluded_exceptions)
        self.assertEqual((), self.breaker.listeners)
        self.assertEqual('memory', self.breaker._state_storage.name)

    def test_new_with_custom_excluded_exceptions(self):
        """CircuitBreaker: it should support a custom list of excluded
        exceptions.
        """
        self.breaker = CircuitBreaker(exclude=[Exception])
        self.assertEqual(0, self.breaker.fail_counter)
        self.assertEqual(60, self.breaker.reset_timeout)
        self.assertEqual(5, self.breaker.fail_max)
        self.assertEqual((Exception,), self.breaker.excluded_exceptions)
        self.assertEqual((), self.breaker.listeners)
        self.assertEqual('memory', self.breaker._state_storage.name)

    def test_fail_max_setter(self):
        """CircuitBreaker: it should allow the user to set a new value for
        'fail_max'.
        """
        self.assertEqual(5, self.breaker.fail_max)
        self.breaker.fail_max = 10
        self.assertEqual(10, self.breaker.fail_max)

    def test_reset_timeout_setter(self):
        """CircuitBreaker: it should allow the user to set a new value for
        'reset_timeout'.
        """
        self.assertEqual(60, self.breaker.reset_timeout)
        self.breaker.reset_timeout = 30
        self.assertEqual(30, self.breaker.reset_timeout)

    def test_call_with_no_args(self):
        """CircuitBreaker: it should be able to invoke functions with no-args.
        """
        def func(): return True
        self.assertTrue(self.breaker.call(func))

    def test_call_with_args(self):
        """CircuitBreaker: it should be able to invoke functions with args.
        """
        def func(arg1, arg2): return [arg1, arg2]
        self.assertEqual([42, 'abc'], self.breaker.call(func, 42, 'abc'))

    def test_call_with_kwargs(self):
        """CircuitBreaker: it should be able to invoke functions with kwargs.
        """
        def func(**kwargs): return kwargs
        self.assertEqual({'a':1, 'b':2}, self.breaker.call(func, a=1, b=2))

    @testing.gen_test
    def test_call_async_with_no_args(self):
        """CircuitBreaker: it should be able to invoke async functions with no-args.
        """
        @gen.coroutine
        def func(): return True

        ret = yield self.breaker.call(func)
        self.assertTrue(ret)

    @testing.gen_test
    def test_call_async_with_args(self):
        """CircuitBreaker: it should be able to invoke async functions with args.
        """
        @gen.coroutine
        def func(arg1, arg2): return [arg1, arg2]

        ret = yield self.breaker.call(func, 42, 'abc')
        self.assertEqual([42, 'abc'], ret)

    @testing.gen_test
    def test_call_async_with_kwargs(self):
        """CircuitBreaker: it should be able to invoke async functions with kwargs.
        """
        @gen.coroutine
        def func(**kwargs): return kwargs
        ret = yield self.breaker.call(func, a=1, b=2)
        self.assertEqual({'a':1, 'b':2}, ret)

    def test_add_listener(self):
        """CircuitBreaker: it should allow the user to add a listener at a
        later time.
        """
        self.assertEqual((), self.breaker.listeners)

        first = CircuitBreakerListener()
        self.breaker.add_listener(first)
        self.assertEqual((first,), self.breaker.listeners)

        second = CircuitBreakerListener()
        self.breaker.add_listener(second)
        self.assertEqual((first, second), self.breaker.listeners)

    def test_add_listeners(self):
        """CircuitBreaker: it should allow the user to add listeners at a
        later time.
        """
        first, second = CircuitBreakerListener(), CircuitBreakerListener()
        self.breaker.add_listeners(first, second)
        self.assertEqual((first, second), self.breaker.listeners)

    def test_remove_listener(self):
        """CircuitBreaker: it should allow the user to remove a listener.
        """
        first = CircuitBreakerListener()
        self.breaker.add_listener(first)
        self.assertEqual((first,), self.breaker.listeners)

        self.breaker.remove_listener(first)
        self.assertEqual((), self.breaker.listeners)

    def test_excluded_exceptions(self):
        """CircuitBreaker: it should ignore specific exceptions.
        """
        self.breaker = CircuitBreaker(exclude=[LookupError])

        def err_1(): raise NotImplementedError()
        def err_2(): raise LookupError()
        def err_3(): raise KeyError()

        self.assertRaises(NotImplementedError, self.breaker.call, err_1)
        self.assertEqual(1, self.breaker.fail_counter)

        # LookupError is not considered a system error
        self.assertRaises(LookupError, self.breaker.call, err_2)
        self.assertEqual(0, self.breaker.fail_counter)

        self.assertRaises(NotImplementedError, self.breaker.call, err_1)
        self.assertEqual(1, self.breaker.fail_counter)

        # Should consider subclasses as well (KeyError is a subclass of
        # LookupError)
        self.assertRaises(KeyError, self.breaker.call, err_3)
        self.assertEqual(0, self.breaker.fail_counter)

    def test_excluded_callable_exceptions(self):
        """CircuitBreaker: it should ignore specific exceptions that return true from a filtering callable.
        """
        class TestException(Exception):
            def __init__(self, value):
                self.value = value

        filter_function = lambda e: type(e) == TestException and e.value == 'good'
        self.breaker = CircuitBreaker(exclude=[filter_function])

        def err_1(): raise TestException("bad")
        def err_2(): raise TestException("good")
        def err_3(): raise NotImplementedError()

        self.assertRaises(TestException, self.breaker.call, err_1)
        self.assertEqual(1, self.breaker.fail_counter)

        self.assertRaises(TestException, self.breaker.call, err_2)
        self.assertEqual(0, self.breaker.fail_counter)

        self.assertRaises(NotImplementedError, self.breaker.call, err_3)
        self.assertEqual(1, self.breaker.fail_counter)

    def test_excluded_callable_and_types_exceptions(self):
        """CircuitBreaker: it should allow a mix of exclusions that includes both filter functions and types.
        """
        class TestException(Exception):
            def __init__(self, value):
                self.value = value

        filter_function = lambda e: type(e) == TestException and e.value == 'good'
        self.breaker = CircuitBreaker(exclude=[filter_function, LookupError])

        def err_1(): raise TestException("bad")
        def err_2(): raise TestException("good")
        def err_3(): raise NotImplementedError()
        def err_4(): raise LookupError()

        self.assertRaises(TestException, self.breaker.call, err_1)
        self.assertEqual(1, self.breaker.fail_counter)

        self.assertRaises(TestException, self.breaker.call, err_2)
        self.assertEqual(0, self.breaker.fail_counter)

        self.assertRaises(NotImplementedError, self.breaker.call, err_3)
        self.assertEqual(1, self.breaker.fail_counter)

        self.assertRaises(LookupError, self.breaker.call, err_4)
        self.assertEqual(0, self.breaker.fail_counter)

    def test_add_excluded_exception(self):
        """CircuitBreaker: it should allow the user to exclude an exception at a
        later time.
        """
        self.assertEqual((), self.breaker.excluded_exceptions)

        self.breaker.add_excluded_exception(NotImplementedError)
        self.assertEqual((NotImplementedError,), \
                         self.breaker.excluded_exceptions)

        self.breaker.add_excluded_exception(Exception)
        self.assertEqual((NotImplementedError, Exception), \
                         self.breaker.excluded_exceptions)

    def test_add_excluded_exceptions(self):
        """CircuitBreaker: it should allow the user to exclude exceptions at a
        later time.
        """
        self.breaker.add_excluded_exceptions(NotImplementedError, Exception)
        self.assertEqual((NotImplementedError, Exception), \
                         self.breaker.excluded_exceptions)

    def test_remove_excluded_exception(self):
        """CircuitBreaker: it should allow the user to remove an excluded
        exception.
        """
        self.breaker.add_excluded_exception(NotImplementedError)
        self.assertEqual((NotImplementedError,), \
                         self.breaker.excluded_exceptions)

        self.breaker.remove_excluded_exception(NotImplementedError)
        self.assertEqual((), self.breaker.excluded_exceptions)

    def test_decorator(self):
        """CircuitBreaker: it should be a decorator.
        """
        @self.breaker
        def suc(value):
            "Docstring"
            return value

        @self.breaker
        def err(value):
            "Docstring"
            raise NotImplementedError()

        self.assertEqual('Docstring', suc.__doc__)
        self.assertEqual('Docstring', err.__doc__)
        self.assertEqual('suc', suc.__name__)
        self.assertEqual('err', err.__name__)

        self.assertRaises(NotImplementedError, err, True)
        self.assertEqual(1, self.breaker.fail_counter)

        self.assertTrue(suc(True))
        self.assertEqual(0, self.breaker.fail_counter)

    @testing.gen_test
    def test_decorator_call_future(self):
        """CircuitBreaker: it should be a decorator.
        """
        @self.breaker(__pybreaker_call_async=True)
        @gen.coroutine
        def suc(value):
            "Docstring"
            raise gen.Return(value)

        @self.breaker(__pybreaker_call_async=True)
        @gen.coroutine
        def err(value):
            "Docstring"
            raise NotImplementedError()

        self.assertEqual('Docstring', suc.__doc__)
        self.assertEqual('Docstring', err.__doc__)
        self.assertEqual('suc', suc.__name__)
        self.assertEqual('err', err.__name__)

        with self.assertRaises(NotImplementedError):
            yield err(True)

        self.assertEqual(1, self.breaker.fail_counter)

        ret = yield suc(True)
        self.assertTrue(ret)
        self.assertEqual(0, self.breaker.fail_counter)

    @mock.patch('pybreaker.HAS_TORNADO_SUPPORT', False)
    def test_no_tornado_raises(self):
        with self.assertRaises(ImportError):
            def func(): return True
            self.breaker(func, __pybreaker_call_async=True)

    def test_name(self):
        """CircuitBreaker: it should allow an optional name to be set and
           retrieved.
        """
        name = "test_breaker"
        self.breaker = CircuitBreaker(name=name)
        self.assertEqual(self.breaker.name, name)

        name = "breaker_test"
        self.breaker.name = name
        self.assertEqual(self.breaker.name, name)


class CircuitBreakerTestCase(testing.AsyncTestCase, CircuitBreakerStorageBasedTestCase, CircuitBreakerConfigurationTestCase):
    """
    Tests for the CircuitBreaker class.
    """

    def setUp(self):
        super(CircuitBreakerTestCase, self).setUp()
        self.breaker_kwargs = {}
        self.breaker = CircuitBreaker()

    def test_create_new_state__bad_state(self):
        with self.assertRaises(ValueError):
            self.breaker._create_new_state('foo')

    @mock.patch('pybreaker.CircuitOpenState')
    def test_notify_not_called_on_init(self, open_state):
        storage = CircuitMemoryStorage('open')
        breaker = CircuitBreaker(state_storage=storage)
        open_state.assert_called_once_with(breaker, prev_state=None, notify=False)

    @mock.patch('pybreaker.CircuitOpenState')
    def test_notify_called_on_state_change(self, open_state):
        storage = CircuitMemoryStorage('closed')
        breaker = CircuitBreaker(state_storage=storage)
        prev_state = breaker.state
        breaker.state = 'open'
        open_state.assert_called_once_with(breaker, prev_state=prev_state, notify=True)

    def test_failure_count_not_reset_during_creation(self):
        for state in (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN):
            storage = CircuitMemoryStorage(state)
            storage.increment_counter()

            breaker = CircuitBreaker(state_storage=storage)
            self.assertEqual(breaker.state.name, state)
            self.assertEqual(breaker.fail_counter, 1)

    def test_state_opened_at_not_reset_during_creation(self):
        for state in (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN):
            storage = CircuitMemoryStorage(state)
            now = datetime.now()
            storage.opened_at = now

            breaker = CircuitBreaker(state_storage=storage)
            self.assertEqual(breaker.state.name, state)
            self.assertEqual(storage.opened_at, now)



import fakeredis
import logging
from redis.exceptions import RedisError

class CircuitBreakerRedisTestCase(unittest.TestCase, CircuitBreakerStorageBasedTestCase):
    """
    Tests for the CircuitBreaker class.
    """

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self.breaker_kwargs = {'state_storage': CircuitRedisStorage('closed', self.redis)}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

    def tearDown(self):
        self.redis.flushall()

    def test_namespace(self):
        self.redis.flushall()
        self.breaker_kwargs = {'state_storage': CircuitRedisStorage('closed', self.redis, namespace='my_app')}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

        def func(): raise NotImplementedError()
        self.assertRaises(NotImplementedError, self.breaker.call, func)
        keys = self.redis.keys()
        self.assertEqual(2, len(keys))
        self.assertTrue(keys[0].decode('utf-8').startswith('my_app'))
        self.assertTrue(keys[1].decode('utf-8').startswith('my_app'))

    def test_fallback_state(self):
        logger = logging.getLogger('pybreaker')
        logger.setLevel(logging.FATAL)
        self.breaker_kwargs = {'state_storage': CircuitRedisStorage('closed', self.redis, fallback_circuit_state='open')}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)
        def func(k): raise RedisError()
        with mock.patch.object(self.redis, 'get', new=func):
            state = self.breaker.state
            self.assertEqual('open', state.name)

    def test_missing_state(self):
        """CircuitBreakerRedis: If state on Redis is missing, it should set the
        fallback circuit state and reset the fail counter to 0.
        """
        self.breaker_kwargs = {'state_storage': CircuitRedisStorage('closed', self.redis, fallback_circuit_state='open')}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

        def func(): raise NotImplementedError()
        self.assertRaises(NotImplementedError, self.breaker.call, func)
        self.assertEqual(1, self.breaker.fail_counter)

        with mock.patch.object(self.redis, 'get', new=lambda k: None):
            state = self.breaker.state
            self.assertEqual('open', state.name)
            self.assertEqual(0, self.breaker.fail_counter)



import threading
from types import MethodType

class CircuitBreakerThreadsTestCase(unittest.TestCase):
    """
    Tests to reproduce common synchronization errors on CircuitBreaker class.
    """

    def setUp(self):
        self.breaker = CircuitBreaker(fail_max=3000, reset_timeout=1)

    def _start_threads(self, target, n):
        """
        Starts `n` threads that calls `target` and waits for them to finish.
        """
        threads = [threading.Thread(target=target) for i in range(n)]
        [t.start() for t in threads]
        [t.join() for t in threads]

    def _mock_function(self, obj, func):
        """
        Replaces a bounded function in `self.breaker` by another.
        """
        setattr(obj, func.__name__, MethodType(func, self.breaker))

    def test_fail_thread_safety(self):
        """CircuitBreaker: it should compute a failed call atomically to
        avoid race conditions.
        """
        # Create a specific exception to avoid masking other errors
        class SpecificException(Exception):
            pass

        @self.breaker
        def err(): raise SpecificException()

        def trigger_error():
            for n in range(500):
                try: err()
                except SpecificException: pass

        def _inc_counter(self):
            c = self._state_storage._fail_counter
            sleep(0.00005)
            self._state_storage._fail_counter = c + 1

        self._mock_function(self.breaker, _inc_counter)
        self._start_threads(trigger_error, 3)
        self.assertEqual(1500, self.breaker.fail_counter)

    def test_success_thread_safety(self):
        """CircuitBreaker: it should compute a successful call atomically
        to avoid race conditions.
        """
        @self.breaker
        def suc(): return True

        def trigger_success():
            for n in range(500):
                suc()

        class SuccessListener(CircuitBreakerListener):
            def success(self, cb):
                c = 0
                if hasattr(cb, '_success_counter'):
                    c = cb._success_counter
                sleep(0.00005)
                cb._success_counter = c + 1

        self.breaker.add_listener(SuccessListener())
        self._start_threads(trigger_success, 3)
        self.assertEqual(1500, self.breaker._success_counter)

    def test_half_open_thread_safety(self):
        """CircuitBreaker: it should allow only one trial call when the
        circuit is half-open.
        """
        self.breaker = CircuitBreaker(fail_max=1, reset_timeout=0.01)

        self.breaker.open()
        sleep(0.01)

        @self.breaker
        def err(): raise Exception()

        def trigger_failure():
            try: err()
            except: pass

        class StateListener(CircuitBreakerListener):
            def __init__(self):
                self._count = 0

            def before_call(self, cb, fun, *args, **kwargs):
                sleep(0.00005)

            def state_change(self, cb, old_state, new_state):
                if new_state.name == 'half-open':
                    self._count += 1

        state_listener = StateListener()
        self.breaker.add_listener(state_listener)

        self._start_threads(trigger_failure, 5)
        self.assertEqual(1, state_listener._count)


    def test_fail_max_thread_safety(self):
        """CircuitBreaker: it should not allow more failed calls than
        'fail_max' setting.
        """
        @self.breaker
        def err(): raise Exception()

        def trigger_error():
            for i in range(2000):
                try: err()
                except: pass

        class SleepListener(CircuitBreakerListener):
            def before_call(self, cb, func, *args, **kwargs):
                sleep(0.00005)

        self.breaker.add_listener(SleepListener())
        self._start_threads(trigger_error, 3)
        self.assertEqual(self.breaker.fail_max, self.breaker.fail_counter)


class CircuitBreakerRedisConcurrencyTestCase(unittest.TestCase):
    """
    Tests to reproduce common concurrency between different machines
    connecting to redis. This is simulated locally using threads.
    """

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self.breaker_kwargs = {'fail_max': 3000, 'reset_timeout': 1,'state_storage': CircuitRedisStorage('closed', self.redis)}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

    def tearDown(self):
        self.redis.flushall()

    def _start_threads(self, target, n):
        """
        Starts `n` threads that calls `target` and waits for them to finish.
        """
        threads = [threading.Thread(target=target) for i in range(n)]
        [t.start() for t in threads]
        [t.join() for t in threads]

    def _mock_function(self, obj, func):
        """
        Replaces a bounded function in `self.breaker` by another.
        """
        setattr(obj, func.__name__, MethodType(func, self.breaker))

    def test_fail_thread_safety(self):
        """CircuitBreaker: it should compute a failed call atomically to
        avoid race conditions.
        """
        # Create a specific exception to avoid masking other errors
        class SpecificException(Exception):
            pass

        @self.breaker
        def err(): raise SpecificException()

        def trigger_error():
            for n in range(500):
                try: err()
                except SpecificException: pass

        def _inc_counter(self):
            sleep(0.00005)
            self._state_storage.increment_counter()

        self._mock_function(self.breaker, _inc_counter)
        self._start_threads(trigger_error, 3)
        self.assertEqual(1500, self.breaker.fail_counter)

    def test_success_thread_safety(self):
        """CircuitBreaker: it should compute a successful call atomically
        to avoid race conditions.
        """
        @self.breaker
        def suc(): return True

        def trigger_success():
            for n in range(500):
                suc()

        class SuccessListener(CircuitBreakerListener):
            def success(self, cb):
                c = 0
                if hasattr(cb, '_success_counter'):
                    c = cb._success_counter
                sleep(0.00005)
                cb._success_counter = c + 1

        self.breaker.add_listener(SuccessListener())
        self._start_threads(trigger_success, 3)
        self.assertEqual(1500, self.breaker._success_counter)

    def test_half_open_thread_safety(self):
        """CircuitBreaker: it should allow only one trial call when the
        circuit is half-open.
        """
        self.breaker = CircuitBreaker(fail_max=1, reset_timeout=0.01)

        self.breaker.open()
        sleep(0.01)

        @self.breaker
        def err(): raise Exception()

        def trigger_failure():
            try: err()
            except: pass

        class StateListener(CircuitBreakerListener):
            def __init__(self):
                self._count = 0

            def before_call(self, cb, fun, *args, **kwargs):
                sleep(0.00005)

            def state_change(self, cb, old_state, new_state):
                if new_state.name == 'half-open':
                    self._count += 1

        state_listener = StateListener()
        self.breaker.add_listener(state_listener)

        self._start_threads(trigger_failure, 5)
        self.assertEqual(1, state_listener._count)


    def test_fail_max_thread_safety(self):
        """CircuitBreaker: it should not allow more failed calls than 'fail_max'
        setting. Note that with Redis, where we have separate systems
        incrementing the counter, we can get concurrent updates such that the
        counter is greater than the 'fail_max' by the number of systems. To
        prevent this, we'd need to take out a lock amongst all systems before
        trying the call.
        """
        @self.breaker
        def err(): raise Exception()

        def trigger_error():
            for i in range(2000):
                try: err()
                except: pass

        class SleepListener(CircuitBreakerListener):
            def before_call(self, cb, func, *args, **kwargs):
                sleep(0.00005)

        self.breaker.add_listener(SleepListener())
        num_threads = 3
        self._start_threads(trigger_error, num_threads)
        self.assertTrue(self.breaker.fail_counter < self.breaker.fail_max + num_threads)


if __name__ == "__main__":
    unittest.main()
