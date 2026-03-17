# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2016 Hynek Schlawack
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools

import pytest

from twisted.internet.defer import Deferred, Failure, fail, succeed

from prometheus_async import tx


def _from_async_fn(async_fn):
    # this code is based on
    # https://docs.twisted.org/en/twisted-22.8.0/api/twisted.trial._synctest._Assertions.html#successResultOf
    # except it takes a coroutine function and wraps it in a Deferred first
    @functools.wraps(async_fn)
    def wrapper(*args, **kwargs):
        results = []
        d = Deferred.fromCoroutine(async_fn(*args, **kwargs)).addBoth(
            results.append
        )
        try:
            if not results:
                # none of the tests here use the reactor and so
                # the Deferred should always immediately complete
                # and so this branch will never execute
                raise RuntimeError(
                    f"Success result expected on {d!r}, "
                    "found no result instead"
                )

            if isinstance(results[0], Failure):
                results[0].raiseException()

            return results[0]
        finally:
            # remove a reference cycle via the Deferred[Failure[E]]
            # or list[Failure[E]]
            del d
            del results

    return wrapper


class TestFromAsyncFn:
    def test_no_result(self):
        """
        Missing results are caught.
        """

        @_from_async_fn
        async def demo():
            return await Deferred()

        with pytest.raises(
            RuntimeError,
            match=r"Success result expected on <Deferred at 0x.*>, "
            "found no result instead",
        ):
            demo()

    def test_failure_result(self):
        """
        If an async function fails, the error is propagated.
        """

        class SentinelError(Exception):
            pass

        sentinel_exception = SentinelError("sentinel exception")

        @_from_async_fn
        async def demo():
            return await fail(sentinel_exception)

        with pytest.raises(
            SentinelError, match=r"sentinel exception"
        ) as exc_info:
            demo()

        assert exc_info.value is sentinel_exception

    def test_success_result(self):
        """
        If an async function succeeds, the success and result are propagated.
        """
        sentinel = object()

        @_from_async_fn
        async def demo():
            return await succeed(sentinel)

        assert demo() is sentinel


class TestTime:
    @pytest.mark.usefixtures("patch_timer")
    def test_decorator_sync(self, fake_observer):
        """
        time works with sync results functions.
        """

        @tx.time(fake_observer)
        def func():
            return 42

        assert 42 == func()
        assert [1] == fake_observer._observed

    @pytest.mark.usefixtures("patch_timer")
    @_from_async_fn
    async def test_decorator(self, fake_observer):
        """
        time works with functions returning Deferreds.
        """

        @tx.time(fake_observer)
        def func():
            return succeed(42)

        rv = func()

        # Twisted runs fires callbacks immediately.
        assert [1] == fake_observer._observed
        assert 42 == (await rv)
        assert [1] == fake_observer._observed

    @pytest.mark.usefixtures("patch_timer")
    @_from_async_fn
    async def test_decorator_exc(self, fake_observer):
        """
        Does not swallow exceptions.
        """
        v = ValueError("foo")

        @tx.time(fake_observer)
        def func():
            return fail(v)

        with pytest.raises(ValueError) as e:
            await func()

        assert v is e.value

    @pytest.mark.usefixtures("patch_timer")
    @_from_async_fn
    async def test_deferred(self, fake_observer):
        """
        time works with Deferreds.
        """
        d = tx.time(fake_observer, Deferred())

        assert [] == fake_observer._observed

        d.callback(42)

        assert 42 == (await d)
        assert [1] == fake_observer._observed


class TestCountExceptions:
    @_from_async_fn
    async def test_decorator_no_exc(self, fake_counter):
        """
        If no exception is raised, the counter does not change.
        """

        @tx.count_exceptions(fake_counter)
        def func():
            return succeed(42)

        assert 42 == (await func())
        assert 0 == fake_counter._val

    def test_decorator_no_exc_sync(self, fake_counter):
        """
        If no exception is raised, the counter does not change.
        """

        @tx.count_exceptions(fake_counter)
        def func():
            return 42

        assert 42 == func()
        assert 0 == fake_counter._val

    @_from_async_fn
    async def test_decorator_wrong_exc(self, fake_counter):
        """
        If a wrong exception is raised, the counter does not change.
        """

        @tx.count_exceptions(fake_counter, exc=ValueError)
        def func():
            return fail(TypeError())

        with pytest.raises(TypeError):
            await func()

        assert 0 == fake_counter._val

    @_from_async_fn
    async def test_decorator_exc(self, fake_counter):
        """
        If the correct exception is raised, count it.
        """

        @tx.count_exceptions(fake_counter, exc=TypeError)
        def func():
            return fail(TypeError())

        with pytest.raises(TypeError):
            await func()

        assert 1 == fake_counter._val

    def test_decorator_exc_sync(self, fake_counter):
        """
        If the correct synchronous exception is raised, count it.
        """

        @tx.count_exceptions(fake_counter)
        def func():
            if True:
                raise TypeError("foo")
            return succeed(42)

        with pytest.raises(TypeError):
            func()

        assert 1 == fake_counter._val

    @_from_async_fn
    async def test_deferred_no_exc(self, fake_counter):
        """
        If no exception is raised, the counter does not change.
        """
        d = succeed(42)

        assert 42 == (await tx.count_exceptions(fake_counter, d))
        assert 0 == fake_counter._val


class TestTrackInprogress:
    @_from_async_fn
    async def test_deferred(self, fake_gauge):
        """
        Incs and decs if its passed a Deferred.
        """
        d = tx.track_inprogress(fake_gauge, Deferred())

        assert 1 == fake_gauge._val

        d.callback(42)
        rv = await d

        assert 42 == rv
        assert 0 == fake_gauge._val

    @_from_async_fn
    async def test_decorator_deferred(self, fake_gauge):
        """
        Incs and decs if the decorated function returns a Deferred.
        """
        d = Deferred()

        @tx.track_inprogress(fake_gauge)
        def func():
            return d

        rv = func()

        assert 1 == fake_gauge._val

        d.callback(42)
        rv = await rv

        assert 42 == rv
        assert 0 == fake_gauge._val

    def test_decorator_value(self, fake_gauge):
        """
        Incs and decs if the decorated function returns a value.
        """

        @tx.track_inprogress(fake_gauge)
        def func():
            return 42

        rv = func()

        assert 42 == rv
        assert 0 == fake_gauge._val
        assert 2 == fake_gauge._calls
