import unittest
from contextlib import contextmanager
from datetime import datetime
from time import sleep
from unittest import mock

import pytest
from pybreaker import *
from tornado import gen, testing


class CircuitBreakerStorageBasedTestCase:
    """Mix in to test against different storage backings. Depends on
    `self.breaker` and `self.breaker_kwargs`.
    """

    def __init__(self):
        self.breaker_kwargs = None

    def test_successful_call(self):
        """CircuitBreaker: it should keep the circuit closed after a successful
        call.
        """

        def func():
            return True

        assert self.breaker.call(func)
        assert self.breaker.fail_counter == 0
        assert self.breaker.current_state == "closed"

    def test_one_failed_call(self):
        """CircuitBreaker: it should keep the circuit closed after a few
        failures.
        """

        def func():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        assert self.breaker.fail_counter == 1
        assert self.breaker.current_state == "closed"

    def test_one_successful_call_after_failed_call(self):
        """CircuitBreaker: it should keep the circuit closed after few mixed
        outcomes.
        """

        def suc():
            return True

        def err():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(err)
        assert self.breaker.fail_counter == 1

        assert self.breaker.call(suc)
        assert self.breaker.fail_counter == 0
        assert self.breaker.current_state == "closed"

    def test_several_failed_calls_setting_absent(self):
        """CircuitBreaker: it should open the circuit after many failures."""
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)

        def func():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)
        assert self.breaker.fail_counter == 3
        assert self.breaker.current_state == "open"

    def test_throw_new_error_on_trip_false(self):
        """CircuitBreaker: it should throw the original exception"""
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs, throw_new_error_on_trip=False)

        def func():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)

        # Circuit should be open
        assert self.breaker.fail_counter == 3
        assert self.breaker.current_state == "open"

        # Circuit should still be open and break
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)
        assert self.breaker.fail_counter == 3
        assert self.breaker.current_state == "open"

    def test_throw_new_error_on_trip_true(self):
        """CircuitBreaker: it should throw a CircuitBreakerError exception"""
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs, throw_new_error_on_trip=True)

        def func():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)
        assert self.breaker.fail_counter == 3
        assert self.breaker.current_state == "open"

    def test_traceback_in_circuitbreaker_error(self):
        """CircuitBreaker: it should open the circuit after many failures."""
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)

        def func():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)

        # Circuit should open
        try:
            self.breaker.call(func)
            pytest.fail("CircuitBreakerError should throw")
        except CircuitBreakerError:
            import traceback

            assert "NotImplementedError" in traceback.format_exc()
        assert self.breaker.fail_counter == 3
        assert self.breaker.current_state == "open"

    def test_failed_call_after_timeout(self):
        """CircuitBreaker: it should half-open the circuit after timeout."""
        self.breaker = CircuitBreaker(fail_max=3, reset_timeout=0.5, **self.breaker_kwargs)

        def func():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        assert self.breaker.current_state == "closed"

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)
        assert self.breaker.fail_counter == 3

        # Wait for timeout
        sleep(0.6)

        # Circuit should open again
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)
        assert self.breaker.fail_counter == 4
        assert self.breaker.current_state == "open"

    def test_successful_after_timeout(self):
        """CircuitBreaker: it should close the circuit when a call succeeds
        after timeout. The successful function should only be called once.
        """
        self.breaker = CircuitBreaker(fail_max=3, reset_timeout=1, **self.breaker_kwargs)

        suc = mock.MagicMock(return_value=True)

        def err():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(err)
        with pytest.raises(NotImplementedError):
            self.breaker.call(err)
        assert self.breaker.current_state == "closed"

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(err)
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(suc)
        assert self.breaker.fail_counter == 3

        # Wait for timeout, at least a second since redis rounds to a second
        sleep(2)

        # Circuit should close again
        assert self.breaker.call(suc)
        assert self.breaker.fail_counter == 0
        assert self.breaker.current_state == "closed"
        assert suc.call_count == 1

    def test_failed_call_when_halfopen(self):
        """CircuitBreaker: it should open the circuit when a call fails in
        half-open state.
        """

        def fun():
            raise NotImplementedError

        self.breaker.half_open()
        assert self.breaker.fail_counter == 0
        assert self.breaker.current_state == "half-open"

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(fun)
        assert self.breaker.fail_counter == 1
        assert self.breaker.current_state == "open"

    def test_successful_call_when_halfopen(self):
        """CircuitBreaker: it should close the circuit when a call succeeds in
        half-open state.
        """

        def fun():
            return True

        self.breaker.half_open()
        assert self.breaker.fail_counter == 0
        assert self.breaker.current_state == "half-open"

        # Circuit should open
        assert self.breaker.call(fun)
        assert self.breaker.fail_counter == 0
        assert self.breaker.current_state == "closed"

    def test_close(self):
        """CircuitBreaker: it should allow the circuit to be closed manually."""
        self.breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)

        def func():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        with pytest.raises(NotImplementedError):
            self.breaker.call(func)

        # Circuit should open
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(func)
        assert self.breaker.fail_counter == 3
        assert self.breaker.current_state == "open"

        # Circuit should close again
        self.breaker.close()
        assert self.breaker.fail_counter == 0
        assert self.breaker.current_state == "closed"

    def test_transition_events(self):
        """CircuitBreaker: it should call the appropriate functions on every
        state transition.
        """

        class Listener(CircuitBreakerListener):
            def __init__(self):
                self.out = ""

            def state_change(self, cb, old_state, new_state):
                assert cb
                if old_state:
                    self.out += old_state.name
                if new_state:
                    self.out += "->" + new_state.name
                self.out += ","

        listener = Listener()
        self.breaker = CircuitBreaker(listeners=(listener,), **self.breaker_kwargs)
        assert self.breaker.current_state == "closed"

        self.breaker.open()
        assert self.breaker.current_state == "open"

        self.breaker.half_open()
        assert self.breaker.current_state == "half-open"

        self.breaker.close()
        assert self.breaker.current_state == "closed"

        assert listener.out == "closed->open,open->half-open,half-open->closed,"

    def test_call_events(self):
        """CircuitBreaker: it should call the appropriate functions on every
        successful/failed call.
        """
        self.out = ""

        def suc():
            return True

        def err():
            raise NotImplementedError

        class Listener(CircuitBreakerListener):
            def __init__(self):
                self.out = ""

            def before_call(self, cb, func, *args, **kwargs):
                assert cb
                self.out += "-"

            def success(self, cb):
                assert cb
                self.out += "success"

            def failure(self, cb, exc):
                assert cb
                assert exc
                self.out += "failure"

        listener = Listener()
        self.breaker = CircuitBreaker(listeners=(listener,), **self.breaker_kwargs)

        assert self.breaker.call(suc)
        with pytest.raises(NotImplementedError):
            self.breaker.call(err)
        assert listener.out == "-success-failure"

    def test_generator(self):
        """CircuitBreaker: it should inspect generator values."""

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

        with pytest.raises(NotImplementedError):
            e.send(True)
        assert self.breaker.fail_counter == 1
        assert next(s)
        with pytest.raises((StopIteration, RuntimeError)):
            next(s)
        assert self.breaker.fail_counter == 0

    def test_contextmanager(self):
        """CircuitBreaker: it should catch in a with statement"""

        class Foo:
            @contextmanager
            @self.breaker
            def wrapper(self):
                try:
                    yield
                except NotImplementedError:
                    raise ValueError

            def foo(self):
                with self.wrapper():
                    raise NotImplementedError

        try:
            Foo().foo()
        except ValueError as e:
            assert isinstance(e, ValueError)


class CircuitBreakerConfigurationTestCase:
    """Tests for the CircuitBreaker class."""

    def test_default_state(self):
        """CircuitBreaker: it should get initial state from state_storage."""
        for state in (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN):
            storage = CircuitMemoryStorage(state)
            breaker = CircuitBreaker(state_storage=storage)
            assert breaker.state.name == state

    def test_default_params(self):
        """CircuitBreaker: it should define smart defaults."""
        assert self.breaker.fail_counter == 0
        assert self.breaker.reset_timeout == 60
        assert self.breaker.fail_max == 5
        assert self.breaker.current_state == "closed"
        assert self.breaker.excluded_exceptions == ()
        assert self.breaker.listeners == ()
        assert self.breaker._state_storage.name == "memory"

    def test_new_with_custom_reset_timeout(self):
        """CircuitBreaker: it should support a custom reset timeout value."""
        self.breaker = CircuitBreaker(reset_timeout=30)
        assert self.breaker.fail_counter == 0
        assert self.breaker.reset_timeout == 30
        assert self.breaker.fail_max == 5
        assert self.breaker.excluded_exceptions == ()
        assert self.breaker.listeners == ()
        assert self.breaker._state_storage.name == "memory"

    def test_new_with_custom_fail_max(self):
        """CircuitBreaker: it should support a custom maximum number of
        failures.
        """
        self.breaker = CircuitBreaker(fail_max=10)
        assert self.breaker.fail_counter == 0
        assert self.breaker.reset_timeout == 60
        assert self.breaker.fail_max == 10
        assert self.breaker.excluded_exceptions == ()
        assert self.breaker.listeners == ()
        assert self.breaker._state_storage.name == "memory"

    def test_new_with_custom_excluded_exceptions(self):
        """CircuitBreaker: it should support a custom list of excluded
        exceptions.
        """
        self.breaker = CircuitBreaker(exclude=[Exception])
        assert self.breaker.fail_counter == 0
        assert self.breaker.reset_timeout == 60
        assert self.breaker.fail_max == 5
        assert (Exception,) == self.breaker.excluded_exceptions
        assert self.breaker.listeners == ()
        assert self.breaker._state_storage.name == "memory"

    def test_fail_max_setter(self):
        """CircuitBreaker: it should allow the user to set a new value for
        'fail_max'.
        """
        assert self.breaker.fail_max == 5
        self.breaker.fail_max = 10
        assert self.breaker.fail_max == 10

    def test_reset_timeout_setter(self):
        """CircuitBreaker: it should allow the user to set a new value for
        'reset_timeout'.
        """
        assert self.breaker.reset_timeout == 60
        self.breaker.reset_timeout = 30
        assert self.breaker.reset_timeout == 30

    def test_call_with_no_args(self):
        """CircuitBreaker: it should be able to invoke functions with no-args."""

        def func():
            return True

        assert self.breaker.call(func)

    def test_call_with_args(self):
        """CircuitBreaker: it should be able to invoke functions with args."""

        def func(arg1, arg2):
            return [arg1, arg2]

        assert [42, "abc"], self.breaker.call(func, 42 == "abc")

    def test_call_with_kwargs(self):
        """CircuitBreaker: it should be able to invoke functions with kwargs."""

        def func(**kwargs):
            return kwargs

        assert {"a": 1, "b": 2}, self.breaker.call(func, a=1, b=2)

    @testing.gen_test
    def test_call_async_with_no_args(self):
        """CircuitBreaker: it should be able to invoke async functions with no-args."""

        @gen.coroutine
        def func():
            return True

        ret = yield self.breaker.call(func)
        assert ret

    @testing.gen_test
    def test_call_async_with_args(self):
        """CircuitBreaker: it should be able to invoke async functions with args."""

        @gen.coroutine
        def func(arg1, arg2):
            return [arg1, arg2]

        ret = yield self.breaker.call(func, 42, "abc")
        assert [42, "abc"] == ret

    @testing.gen_test
    def test_call_async_with_kwargs(self):
        """CircuitBreaker: it should be able to invoke async functions with kwargs."""

        @gen.coroutine
        def func(**kwargs):
            return kwargs

        ret = yield self.breaker.call(func, a=1, b=2)
        assert {"a": 1, "b": 2} == ret

    def test_add_listener(self):
        """CircuitBreaker: it should allow the user to add a listener at a
        later time.
        """
        assert self.breaker.listeners == ()

        first = CircuitBreakerListener()
        self.breaker.add_listener(first)
        assert (first,) == self.breaker.listeners

        second = CircuitBreakerListener()
        self.breaker.add_listener(second)
        assert (first, second) == self.breaker.listeners

    def test_add_listeners(self):
        """CircuitBreaker: it should allow the user to add listeners at a
        later time.
        """
        first, second = CircuitBreakerListener(), CircuitBreakerListener()
        self.breaker.add_listeners(first, second)
        assert (first, second) == self.breaker.listeners

    def test_remove_listener(self):
        """CircuitBreaker: it should allow the user to remove a listener."""
        first = CircuitBreakerListener()
        self.breaker.add_listener(first)
        assert (first,) == self.breaker.listeners

        self.breaker.remove_listener(first)
        assert self.breaker.listeners == ()

    def test_excluded_exceptions(self):
        """CircuitBreaker: it should ignore specific exceptions."""
        self.breaker = CircuitBreaker(exclude=[LookupError])

        def err_1():
            raise NotImplementedError

        def err_2():
            raise LookupError

        def err_3():
            raise KeyError

        with pytest.raises(NotImplementedError):
            self.breaker.call(err_1)
        assert self.breaker.fail_counter == 1

        # LookupError is not considered a system error
        with pytest.raises(LookupError):
            self.breaker.call(err_2)
        assert self.breaker.fail_counter == 0

        with pytest.raises(NotImplementedError):
            self.breaker.call(err_1)
        assert self.breaker.fail_counter == 1

        # Should consider subclasses as well (KeyError is a subclass of
        # LookupError)
        with pytest.raises(KeyError):
            self.breaker.call(err_3)
        assert self.breaker.fail_counter == 0

    def test_excluded_callable_exceptions(self):
        """CircuitBreaker: it should ignore specific exceptions that return true from a filtering callable."""

        class TestException(Exception):
            def __init__(self, value):
                self.value = value

        filter_function = lambda e: type(e) == TestException and e.value == "good"
        self.breaker = CircuitBreaker(exclude=[filter_function])

        def err_1():
            raise TestException("bad")

        def err_2():
            raise TestException("good")

        def err_3():
            raise NotImplementedError

        with pytest.raises(TestException):
            self.breaker.call(err_1)
        assert self.breaker.fail_counter == 1

        with pytest.raises(TestException):
            self.breaker.call(err_2)
        assert self.breaker.fail_counter == 0

        with pytest.raises(NotImplementedError):
            self.breaker.call(err_3)
        assert self.breaker.fail_counter == 1

    def test_excluded_callable_and_types_exceptions(self):
        """CircuitBreaker: it should allow a mix of exclusions that includes both filter functions and types."""

        class TestException(Exception):
            def __init__(self, value):
                self.value = value

        filter_function = lambda e: type(e) == TestException and e.value == "good"
        self.breaker = CircuitBreaker(exclude=[filter_function, LookupError])

        def err_1():
            raise TestException("bad")

        def err_2():
            raise TestException("good")

        def err_3():
            raise NotImplementedError

        def err_4():
            raise LookupError

        with pytest.raises(TestException):
            self.breaker.call(err_1)
        assert self.breaker.fail_counter == 1

        with pytest.raises(TestException):
            self.breaker.call(err_2)
        assert self.breaker.fail_counter == 0

        with pytest.raises(NotImplementedError):
            self.breaker.call(err_3)
        assert self.breaker.fail_counter == 1

        with pytest.raises(LookupError):
            self.breaker.call(err_4)
        assert self.breaker.fail_counter == 0

    def test_add_excluded_exception(self):
        """CircuitBreaker: it should allow the user to exclude an exception at a
        later time.
        """
        assert self.breaker.excluded_exceptions == ()

        self.breaker.add_excluded_exception(NotImplementedError)
        assert (NotImplementedError,) == self.breaker.excluded_exceptions

        self.breaker.add_excluded_exception(Exception)
        assert (NotImplementedError, Exception) == self.breaker.excluded_exceptions

    def test_add_excluded_exceptions(self):
        """CircuitBreaker: it should allow the user to exclude exceptions at a
        later time.
        """
        self.breaker.add_excluded_exceptions(NotImplementedError, Exception)
        assert (NotImplementedError, Exception) == self.breaker.excluded_exceptions

    def test_remove_excluded_exception(self):
        """CircuitBreaker: it should allow the user to remove an excluded
        exception.
        """
        self.breaker.add_excluded_exception(NotImplementedError)
        assert (NotImplementedError,) == self.breaker.excluded_exceptions

        self.breaker.remove_excluded_exception(NotImplementedError)
        assert self.breaker.excluded_exceptions == ()

    def test_decorator(self):
        """CircuitBreaker: it should be a decorator."""

        @self.breaker
        def suc(value):
            "Docstring"
            return value

        @self.breaker
        def err(value):
            "Docstring"
            raise NotImplementedError

        assert suc.__doc__ == "Docstring"
        assert err.__doc__ == "Docstring"
        assert suc.__name__ == "suc"
        assert err.__name__ == "err"

        with pytest.raises(NotImplementedError):
            err(True)
        assert self.breaker.fail_counter == 1

        assert suc(True)
        assert self.breaker.fail_counter == 0

    @testing.gen_test
    def test_decorator_call_future(self):
        """CircuitBreaker: it should be a decorator."""

        @self.breaker(__pybreaker_call_async=True)
        @gen.coroutine
        def suc(value):
            "Docstring"
            raise gen.Return(value)

        @self.breaker(__pybreaker_call_async=True)
        @gen.coroutine
        def err(value):
            "Docstring"
            raise NotImplementedError

        assert suc.__doc__ == "Docstring"
        assert err.__doc__ == "Docstring"
        assert suc.__name__ == "suc"
        assert err.__name__ == "err"

        with pytest.raises(NotImplementedError):
            yield err(True)

        assert self.breaker.fail_counter == 1

        ret = yield suc(True)
        assert ret
        assert self.breaker.fail_counter == 0

    @mock.patch("pybreaker.HAS_TORNADO_SUPPORT", False)
    def test_no_tornado_raises(self):
        with pytest.raises(ImportError):

            def func():
                return True

            self.breaker(func, __pybreaker_call_async=True)

    def test_name(self):
        """CircuitBreaker: it should allow an optional name to be set and
        retrieved.
        """
        name = "test_breaker"
        self.breaker = CircuitBreaker(name=name)
        assert self.breaker.name == name

        name = "breaker_test"
        self.breaker.name = name
        assert self.breaker.name == name

    def test_success_threshold_default_behavior(self):
        """CircuitBreaker: it should maintain backward compatibility with default success_threshold=1."""
        self.breaker = CircuitBreaker(fail_max=3, reset_timeout=0.1, **self.breaker_kwargs)

        def fun():
            return True

        def err():
            raise NotImplementedError

        # Open the circuit
        for i in range(3):
            if i < 2:
                with pytest.raises(NotImplementedError):
                    self.breaker.call(err)
            else:
                with pytest.raises(CircuitBreakerError):
                    self.breaker.call(err)

        # Wait for timeout to enter half-open state
        sleep(0.2)

        # Single successful call should close the circuit (default behavior)
        assert self.breaker.call(fun)
        assert self.breaker.current_state == "closed"
        assert self.breaker.success_counter == 0  # Should be reset when closed

    def test_success_threshold_multiple_successes_required(self):
        """CircuitBreaker: it should require multiple successful calls before closing when success_threshold > 1."""
        self.breaker = CircuitBreaker(fail_max=3, reset_timeout=0.1, success_threshold=3, **self.breaker_kwargs)

        def fun():
            return True

        def err():
            raise NotImplementedError

        # Open the circuit
        for i in range(3):
            if i < 2:
                with pytest.raises(NotImplementedError):
                    self.breaker.call(err)
            else:
                with pytest.raises(CircuitBreakerError):
                    self.breaker.call(err)

        # Wait for timeout to enter half-open state
        sleep(0.2)

        # First successful call should not close the circuit
        assert self.breaker.call(fun)
        assert self.breaker.current_state == "half-open"
        assert self.breaker.success_counter == 1

        # Second successful call should not close the circuit
        assert self.breaker.call(fun)
        assert self.breaker.current_state == "half-open"
        assert self.breaker.success_counter == 2

        # Third successful call should close the circuit
        assert self.breaker.call(fun)
        assert self.breaker.current_state == "closed"
        assert self.breaker.success_counter == 0  # Should be reset when closed

    def test_success_threshold_failure_resets_counter(self):
        """CircuitBreaker: it should reset success counter when a failure occurs in half-open state."""
        self.breaker = CircuitBreaker(fail_max=3, reset_timeout=0.1, success_threshold=3, **self.breaker_kwargs)

        def fun():
            return True

        def err():
            raise NotImplementedError

        # Open the circuit
        for i in range(3):
            if i < 2:
                with pytest.raises(NotImplementedError):
                    self.breaker.call(err)
            else:
                with pytest.raises(CircuitBreakerError):
                    self.breaker.call(err)

        # Wait for timeout to enter half-open state
        sleep(0.2)

        # First successful call
        assert self.breaker.call(fun)
        assert self.breaker.current_state == "half-open"
        assert self.breaker.success_counter == 1

        # Second successful call
        assert self.breaker.call(fun)
        assert self.breaker.current_state == "half-open"
        assert self.breaker.success_counter == 2

        # Failure should reset success counter and open circuit
        with pytest.raises(CircuitBreakerError):
            self.breaker.call(err)
        assert self.breaker.current_state == "open"
        assert self.breaker.success_counter == 0  # Should be reset when opened

    def test_success_threshold_property(self):
        """CircuitBreaker: it should support getting and setting success_threshold."""
        self.breaker = CircuitBreaker(success_threshold=5)
        assert self.breaker.success_threshold == 5

        self.breaker.success_threshold = 10
        assert self.breaker.success_threshold == 10

    def test_success_threshold_redis_storage(self):
        """CircuitBreaker: it should work correctly with Redis storage."""
        # This test requires Redis to be running
        try:
            import redis

            redis_client = redis.Redis()
            redis_client.ping()
        except (ImportError, redis.ConnectionError):
            pytest.skip("Redis not available")

        storage = CircuitRedisStorage(STATE_CLOSED, redis_client)
        self.breaker = CircuitBreaker(fail_max=3, reset_timeout=0.1, success_threshold=2, state_storage=storage)

        def fun():
            return True

        def err():
            raise NotImplementedError

        # Open the circuit
        for i in range(3):
            if i < 2:
                with pytest.raises(NotImplementedError):
                    self.breaker.call(err)
            else:
                with pytest.raises(CircuitBreakerError):
                    self.breaker.call(err)

        # Wait for timeout to enter half-open state
        sleep(0.2)

        # First successful call should not close the circuit
        assert self.breaker.call(fun)
        assert self.breaker.current_state == "half-open"
        assert self.breaker.success_counter == 1

        # Second successful call should close the circuit
        assert self.breaker.call(fun)
        assert self.breaker.current_state == "closed"
        assert self.breaker.success_counter == 0


class CircuitBreakerTestCase(
    testing.AsyncTestCase,
    CircuitBreakerStorageBasedTestCase,
    CircuitBreakerConfigurationTestCase,
):
    """Tests for the CircuitBreaker class."""

    def setUp(self):
        super(CircuitBreakerTestCase, self).setUp()
        self.breaker_kwargs = {}
        self.breaker = CircuitBreaker()

    def test_create_new_state__bad_state(self):
        with pytest.raises(ValueError):
            self.breaker._create_new_state("foo")

    @mock.patch("pybreaker.CircuitOpenState")
    def test_notify_not_called_on_init(self, open_state):
        storage = CircuitMemoryStorage("open")
        breaker = CircuitBreaker(state_storage=storage)
        open_state.assert_called_once_with(breaker, prev_state=None, notify=False)

    @mock.patch("pybreaker.CircuitOpenState")
    def test_notify_called_on_state_change(self, open_state):
        storage = CircuitMemoryStorage("closed")
        breaker = CircuitBreaker(state_storage=storage)
        prev_state = breaker.state
        breaker.state = "open"
        open_state.assert_called_once_with(breaker, prev_state=prev_state, notify=True)

    def test_failure_count_not_reset_during_creation(self):
        for state in (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN):
            storage = CircuitMemoryStorage(state)
            storage.increment_counter()

            breaker = CircuitBreaker(state_storage=storage)
            assert breaker.state.name == state
            assert breaker.fail_counter == 1

    def test_state_opened_at_not_reset_during_creation(self):
        for state in (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN):
            storage = CircuitMemoryStorage(state)
            now = datetime.now()
            storage.opened_at = now

            breaker = CircuitBreaker(state_storage=storage)
            assert breaker.state.name == state
            assert storage.opened_at == now


import logging

import fakeredis
from redis.exceptions import RedisError


class CircuitBreakerRedisTestCase(unittest.TestCase, CircuitBreakerStorageBasedTestCase):
    """Tests for the CircuitBreaker class."""

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self.breaker_kwargs = {"state_storage": CircuitRedisStorage("closed", self.redis)}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

    def tearDown(self):
        self.redis.flushall()

    def test_namespace(self):
        self.redis.flushall()
        self.breaker_kwargs = {"state_storage": CircuitRedisStorage("closed", self.redis, namespace="my_app")}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

        def func():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        keys = self.redis.keys()
        assert len(keys) == 3  # fail_counter, success_counter, state
        assert keys[0].decode("utf-8").startswith("my_app")
        assert keys[1].decode("utf-8").startswith("my_app")
        assert keys[2].decode("utf-8").startswith("my_app")

    def test_fallback_state(self):
        logger = logging.getLogger("pybreaker")
        logger.setLevel(logging.FATAL)
        self.breaker_kwargs = {
            "state_storage": CircuitRedisStorage("closed", self.redis, fallback_circuit_state="open")
        }
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

        def func(k):
            raise RedisError()

        with mock.patch.object(self.redis, "get", new=func):
            state = self.breaker.state
            assert state.name == "open"

    def test_missing_state(self):
        """CircuitBreakerRedis: If state on Redis is missing, it should set the
        fallback circuit state and reset the fail counter to 0.
        """
        self.breaker_kwargs = {
            "state_storage": CircuitRedisStorage("closed", self.redis, fallback_circuit_state="open")
        }
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

        def func():
            raise NotImplementedError

        with pytest.raises(NotImplementedError):
            self.breaker.call(func)
        assert self.breaker.fail_counter == 1

        with mock.patch.object(self.redis, "get", new=lambda k: None):
            state = self.breaker.state
            assert state.name == "open"
            assert self.breaker.fail_counter == 0

    def test_cluster_mode(self):
        self.redis.flushall()

        storage = CircuitRedisStorage(STATE_OPEN, self.redis, namespace="my_app", cluster_mode=True)
        breaker_kwargs = {"state_storage": storage}

        now = datetime.now()
        storage.opened_at = now

        now_str = now.strftime("%Y-%m-%d-%H:%M:%S")
        opened_at = storage.opened_at.strftime("%Y-%m-%d-%H:%M:%S")

        breaker = CircuitBreaker(**breaker_kwargs)
        assert breaker.state.name == STATE_OPEN
        assert opened_at == now_str


import threading
from types import MethodType


class CircuitBreakerThreadsTestCase(unittest.TestCase):
    """Tests to reproduce common synchronization errors on CircuitBreaker class."""

    def setUp(self):
        self.breaker = CircuitBreaker(fail_max=3000, reset_timeout=1)

    def _start_threads(self, target, n):
        """Starts `n` threads that calls `target` and waits for them to finish."""
        threads = [threading.Thread(target=target) for i in range(n)]
        [t.start() for t in threads]
        [t.join() for t in threads]

    def _mock_function(self, obj, func):
        """Replaces a bounded function in `self.breaker` by another."""
        setattr(obj, func.__name__, MethodType(func, self.breaker))

    def test_fail_thread_safety(self):
        """CircuitBreaker: it should compute a failed call atomically to
        avoid race conditions.
        """

        # Create a specific exception to avoid masking other errors
        class SpecificException(Exception):
            pass

        @self.breaker
        def err():
            raise SpecificException

        def trigger_error():
            for n in range(500):
                try:
                    err()
                except SpecificException:
                    pass

        def _inc_counter(self):
            c = self._state_storage._fail_counter
            sleep(0.00005)
            self._state_storage._fail_counter = c + 1

        self._mock_function(self.breaker, _inc_counter)
        self._start_threads(trigger_error, 3)
        assert self.breaker.fail_counter == 1500

    def test_success_thread_safety(self):
        """CircuitBreaker: it should compute a successful call atomically
        to avoid race conditions.
        """

        @self.breaker
        def suc():
            return True

        def trigger_success():
            for n in range(500):
                suc()

        class SuccessListener(CircuitBreakerListener):
            def success(self, cb):
                c = 0
                if hasattr(cb, "_success_counter"):
                    c = cb._success_counter
                sleep(0.00005)
                cb._success_counter = c + 1

        self.breaker.add_listener(SuccessListener())
        self._start_threads(trigger_success, 3)
        assert self.breaker._success_counter == 1500

    def test_half_open_thread_safety(self):
        """CircuitBreaker: it should allow only one trial call when the
        circuit is half-open.
        """
        self.breaker = CircuitBreaker(fail_max=1, reset_timeout=0.01)

        self.breaker.open()
        sleep(0.01)

        @self.breaker
        def err():
            raise Exception

        def trigger_failure():
            try:
                err()
            except:
                pass

        class StateListener(CircuitBreakerListener):
            def __init__(self):
                self._count = 0

            def before_call(self, cb, fun, *args, **kwargs):
                sleep(0.00005)

            def state_change(self, cb, old_state, new_state):
                if new_state.name == "half-open":
                    self._count += 1

        state_listener = StateListener()
        self.breaker.add_listener(state_listener)

        self._start_threads(trigger_failure, 5)
        assert state_listener._count == 1

    def test_fail_max_thread_safety(self):
        """CircuitBreaker: it should not allow more failed calls than
        'fail_max' setting.
        """

        @self.breaker
        def err():
            raise Exception

        def trigger_error():
            for i in range(2000):
                try:
                    err()
                except:
                    pass

        class SleepListener(CircuitBreakerListener):
            def before_call(self, cb, func, *args, **kwargs):
                sleep(0.00005)

        self.breaker.add_listener(SleepListener())
        self._start_threads(trigger_error, 3)
        assert self.breaker.fail_max == self.breaker.fail_counter


class CircuitBreakerRedisConcurrencyTestCase(unittest.TestCase):
    """Tests to reproduce common concurrency between different machines
    connecting to redis. This is simulated locally using threads.
    """

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self.breaker_kwargs = {
            "fail_max": 3000,
            "reset_timeout": 1,
            "state_storage": CircuitRedisStorage("closed", self.redis),
        }
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

    def tearDown(self):
        self.redis.flushall()

    def _start_threads(self, target, n):
        """Starts `n` threads that calls `target` and waits for them to finish."""
        threads = [threading.Thread(target=target) for i in range(n)]
        [t.start() for t in threads]
        [t.join() for t in threads]

    def _mock_function(self, obj, func):
        """Replaces a bounded function in `self.breaker` by another."""
        setattr(obj, func.__name__, MethodType(func, self.breaker))

    def test_fail_thread_safety(self):
        """CircuitBreaker: it should compute a failed call atomically to
        avoid race conditions.
        """

        # Create a specific exception to avoid masking other errors
        class SpecificException(Exception):
            pass

        @self.breaker
        def err():
            raise SpecificException

        def trigger_error():
            for n in range(500):
                try:
                    err()
                except SpecificException:
                    pass

        def _inc_counter(self):
            sleep(0.00005)
            self._state_storage.increment_counter()

        self._mock_function(self.breaker, _inc_counter)
        self._start_threads(trigger_error, 3)
        assert self.breaker.fail_counter == 1500

    def test_success_thread_safety(self):
        """CircuitBreaker: it should compute a successful call atomically
        to avoid race conditions.
        """

        @self.breaker
        def suc():
            return True

        def trigger_success():
            for n in range(500):
                suc()

        class SuccessListener(CircuitBreakerListener):
            def success(self, cb):
                c = 0
                if hasattr(cb, "_success_counter"):
                    c = cb._success_counter
                sleep(0.00005)
                cb._success_counter = c + 1

        self.breaker.add_listener(SuccessListener())
        self._start_threads(trigger_success, 3)
        assert self.breaker._success_counter == 1500

    def test_half_open_thread_safety(self):
        """CircuitBreaker: it should allow only one trial call when the
        circuit is half-open.
        """
        self.breaker = CircuitBreaker(fail_max=1, reset_timeout=0.01)

        self.breaker.open()
        sleep(0.01)

        @self.breaker
        def err():
            raise Exception

        def trigger_failure():
            try:
                err()
            except:
                pass

        class StateListener(CircuitBreakerListener):
            def __init__(self):
                self._count = 0

            def before_call(self, cb, fun, *args, **kwargs):
                sleep(0.00005)

            def state_change(self, cb, old_state, new_state):
                if new_state.name == "half-open":
                    self._count += 1

        state_listener = StateListener()
        self.breaker.add_listener(state_listener)

        self._start_threads(trigger_failure, 5)
        assert state_listener._count == 1

    def test_fail_max_thread_safety(self):
        """CircuitBreaker: it should not allow more failed calls than 'fail_max'
        setting. Note that with Redis, where we have separate systems
        incrementing the counter, we can get concurrent updates such that the
        counter is greater than the 'fail_max' by the number of systems. To
        prevent this, we'd need to take out a lock amongst all systems before
        trying the call.
        """

        @self.breaker
        def err():
            raise Exception

        def trigger_error():
            for i in range(2000):
                try:
                    err()
                except:
                    pass

        class SleepListener(CircuitBreakerListener):
            def before_call(self, cb, func, *args, **kwargs):
                sleep(0.00005)

        self.breaker.add_listener(SleepListener())
        num_threads = 3
        self._start_threads(trigger_error, num_threads)
        assert self.breaker.fail_counter < self.breaker.fail_max + num_threads


class CircuitBreakerContextManagerTestCase(unittest.TestCase):
    """Tests for the CircuitBreaker class, when used as a context manager."""

    def test_calling(self):
        """Test that the CircuitBreaker calling() API returns a context manager and works as expected."""

        class TestError(Exception):
            pass

        breaker = CircuitBreaker(fail_max=2, reset_timeout=0.01)
        mock_fn = mock.MagicMock()

        def _do_raise():
            with breaker.calling():
                raise TestError

        def _do_succeed():
            with breaker.calling():
                mock_fn()

        self.assertRaises(TestError, _do_raise)
        self.assertRaises(CircuitBreakerError, _do_raise)
        assert breaker.fail_counter == 2
        assert breaker.current_state == "open"

        # Still fails while circuit breaker is open:
        self.assertRaises(CircuitBreakerError, _do_succeed)
        mock_fn.assert_not_called()

        sleep(0.01)

        _do_succeed()
        mock_fn.assert_called_once()
        assert breaker.fail_counter == 0
        assert breaker.current_state == "closed"


if __name__ == "__main__":
    unittest.main()
