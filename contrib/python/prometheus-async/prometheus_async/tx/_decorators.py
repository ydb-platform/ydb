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

"""
Decorators for Twisted.
"""

from __future__ import annotations

from time import perf_counter
from typing import TYPE_CHECKING, Callable, overload

from twisted.internet.defer import Deferred
from wrapt import decorator


if TYPE_CHECKING:
    from typing import Any

    from prometheus_client import Gauge

    from ..types import C, D, F, Incrementer, Observer, P, T


@overload
def time(metric: Observer) -> Callable[[Callable[P, D]], Callable[P, D]]: ...


@overload
def time(metric: Observer, deferred: D) -> D: ...


def time(metric: Observer, deferred: D | None = None) -> D | C:
    r"""
    Call ``metric.observe(time)`` with runtime in seconds.

    Can be used as a decorator as well as on ``Deferred``\ s.

    Works with both sync and async results.

    :returns: function or ``Deferred``.
    """
    if deferred is None:

        @decorator
        def time_decorator(f: C, _: Any, args: Any, kw: Any) -> C | D:
            def observe(value: T) -> T:
                metric.observe(perf_counter() - start_time)
                return value

            start_time = perf_counter()
            rv = f(*args, **kw)
            if isinstance(rv, Deferred):
                return rv.addBoth(observe)  # type: ignore[return-value]

            return observe(rv)

        return time_decorator

    def observe(value: T) -> T:
        metric.observe(perf_counter() - start_time)
        return value

    start_time = perf_counter()
    return deferred.addBoth(observe)  # type: ignore[return-value]


@overload
def count_exceptions(
    metric: Incrementer, *, exc: type[BaseException] = ...
) -> Callable[P, C]: ...


@overload
def count_exceptions(
    metric: Incrementer,
    deferred: D,
    *,
    exc: type[BaseException] = ...,
) -> D: ...


def count_exceptions(
    metric: Incrementer,
    deferred: D | None = None,
    *,
    exc: type[BaseException] = BaseException,
) -> D | Callable[P, C]:
    """
    Call ``metric.inc()`` whenever *exc* is caught.

    Can be used as a decorator or on a ``Deferred``.

    :returns: function (if decorator) or ``Deferred``.
    """

    def inc(fail: F) -> F:
        fail.trap(exc)  # type: ignore[no-untyped-call]
        metric.inc()
        return fail

    if deferred is None:

        @decorator
        def count_exceptions_decorator(
            f: C, _: Any, args: Any, kw: Any
        ) -> C | D:
            try:
                rv = f(*args, **kw)
            except exc:
                metric.inc()
                raise

            if isinstance(rv, Deferred):
                return rv.addErrback(inc)  # type: ignore[return-value]

            return rv

        return count_exceptions_decorator

    return deferred.addErrback(inc)  # type: ignore[return-value]


@overload
def track_inprogress(metric: Gauge) -> Callable[P, C]: ...


@overload
def track_inprogress(metric: Gauge, deferred: D) -> D: ...


def track_inprogress(
    metric: Gauge, deferred: D | None = None
) -> D | Callable[P, C]:
    """
    Call ``metrics.inc()`` on entry and ``metric.dec()`` on exit.

    Can be used as a decorator or on a ``Deferred``.

    :returns: function (if decorator) or ``Deferred``.
    """

    def dec(rv: T) -> T:
        metric.dec()
        return rv

    if deferred is None:

        @decorator
        def track_inprogress_decorator(
            f: C, _: Any, args: Any, kw: Any
        ) -> C | D:
            metric.inc()
            try:
                rv = f(*args, **kw)
            finally:
                if isinstance(rv, Deferred):
                    return rv.addBoth(dec)  # type: ignore[return-value]  # noqa: B012

                metric.dec()
                return rv  # noqa: B012

        return track_inprogress_decorator

    metric.inc()
    return deferred.addBoth(dec)  # type: ignore[return-value]
