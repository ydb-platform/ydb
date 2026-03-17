# Copyright The OpenTelemetry Authors
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
.. asyncio: https://github.com/python/asyncio

The opentelemetry-instrumentation-asyncio package allows tracing asyncio applications.
The metric for coroutine, future, is generated even if there is no setting to generate a span.

Run instrumented application
------------------------------
1. coroutine
----------------
.. code:: python

    # export OTEL_PYTHON_ASYNCIO_COROUTINE_NAMES_TO_TRACE=sleep

    import asyncio
    from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor

    AsyncioInstrumentor().instrument()

    async def main():
        await asyncio.create_task(asyncio.sleep(0.1))

    asyncio.run(main())

2. future
------------
.. code:: python

    # export OTEL_PYTHON_ASYNCIO_FUTURE_TRACE_ENABLED=true

    import asyncio
    from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor

    AsyncioInstrumentor().instrument()

    loop = asyncio.get_event_loop()

    future = asyncio.Future()
    future.set_result(1)
    task = asyncio.ensure_future(future)
    loop.run_until_complete(task)

3. to_thread
-------------
.. code:: python

    # export OTEL_PYTHON_ASYNCIO_TO_THREAD_FUNCTION_NAMES_TO_TRACE=func

    import asyncio
    from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor

    AsyncioInstrumentor().instrument()

    async def main():
        await asyncio.to_thread(func)

    def func():
        pass

    asyncio.run(main())


asyncio metric types
---------------------

* asyncio.process.duration (seconds) - Duration of asyncio process
* asyncio.process.count (count) - Number of asyncio process


API
---
"""

import asyncio
import functools
import sys
from asyncio import futures
from timeit import default_timer
from typing import Collection

from wrapt import wrap_function_wrapper as _wrap

from opentelemetry.instrumentation.asyncio.instrumentation_state import (
    _is_instrumented,
)
from opentelemetry.instrumentation.asyncio.package import _instruments
from opentelemetry.instrumentation.asyncio.utils import (
    get_coros_to_trace,
    get_future_trace_enabled,
    get_to_thread_to_trace,
)
from opentelemetry.instrumentation.asyncio.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.metrics import get_meter
from opentelemetry.trace import get_tracer
from opentelemetry.trace.status import Status, StatusCode

ASYNCIO_PREFIX = "asyncio"


class AsyncioInstrumentor(BaseInstrumentor):
    """
    An instrumentor for asyncio

    See `BaseInstrumentor`
    """

    methods_with_coroutine = [
        "create_task",
        "ensure_future",
        "wait_for",
        "wait",
        "as_completed",
        "run_coroutine_threadsafe",
    ]

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        # pylint: disable=attribute-defined-outside-init
        self._tracer = get_tracer(
            __name__, __version__, kwargs.get("tracer_provider")
        )
        self._meter = get_meter(
            __name__, __version__, kwargs.get("meter_provider")
        )

        self._coros_name_to_trace = get_coros_to_trace()
        self._future_active_enabled = get_future_trace_enabled()
        self._to_thread_name_to_trace = get_to_thread_to_trace()

        self.process_duration_histogram = self._meter.create_histogram(
            name="asyncio.process.duration",
            description="Duration of asyncio process",
            unit="s",
        )
        self.process_created_counter = self._meter.create_counter(
            name="asyncio.process.created",
            description="Number of asyncio process",
            unit="{process}",
        )

        for method in self.methods_with_coroutine:
            self.instrument_method_with_coroutine(method)

        self.instrument_gather()
        self.instrument_to_thread()
        self.instrument_taskgroup_create_task()

    def _uninstrument(self, **kwargs):
        for method in self.methods_with_coroutine:
            uninstrument_method_with_coroutine(method)
        uninstrument_gather()
        uninstrument_to_thread()
        uninstrument_taskgroup_create_task()

    def instrument_method_with_coroutine(self, method_name: str):
        """
        Instruments specified asyncio method.
        """

        def wrap_coro_or_future(method, instance, args, kwargs):
            # If the first argument is a coroutine or future,
            # we decorate it with a span and return the task.
            if args and len(args) > 0:
                first_arg = args[0]
                # Check if it's a coroutine or future and wrap it
                if asyncio.iscoroutine(first_arg) or futures.isfuture(
                    first_arg
                ):
                    args = (self.trace_item(first_arg),) + args[1:]
                # Check if it's a list and wrap each item
                elif isinstance(first_arg, list):
                    args = (
                        [self.trace_item(item) for item in first_arg],
                    ) + args[1:]
            return method(*args, **kwargs)

        _wrap(asyncio, method_name, wrap_coro_or_future)

    def instrument_gather(self):
        def wrap_coros_or_futures(method, instance, args, kwargs):
            if args and len(args) > 0:
                # Check if it's a coroutine or future and wrap it
                wrapped_args = tuple(self.trace_item(item) for item in args)
                return method(*wrapped_args, **kwargs)
            return method(*args, **kwargs)

        _wrap(asyncio, "gather", wrap_coros_or_futures)

    def instrument_to_thread(self) -> None:
        def wrap_to_thread(method, instance, args, kwargs) -> None:
            if args:
                first_arg = args[0]
                # Wrap the first argument
                wrapped_first_arg = self.trace_to_thread(first_arg)
                wrapped_args = (wrapped_first_arg,) + args[1:]

                return method(*wrapped_args, **kwargs)
            return method(*args, **kwargs)

        _wrap(asyncio, "to_thread", wrap_to_thread)

    def instrument_taskgroup_create_task(self) -> None:
        # TaskGroup.create_task was added in Python 3.11
        if sys.version_info < (3, 11):
            return

        def wrap_taskgroup_create_task(method, instance, args, kwargs) -> None:
            if args:
                coro = args[0]
                wrapped_coro = self.trace_coroutine(coro)
                wrapped_args = (wrapped_coro,) + args[1:]
                return method(*wrapped_args, **kwargs)
            return method(*args, **kwargs)

        _wrap(
            asyncio.TaskGroup,  # pylint: disable=no-member
            "create_task",
            wrap_taskgroup_create_task,
        )

    def trace_to_thread(self, func: callable):
        """
        Trace a function, but if already instrumented, skip double-wrapping.
        """
        if _is_instrumented(func):
            return func

        start = default_timer()
        func_name = getattr(func, "__name__", None)
        if func_name is None and isinstance(func, functools.partial):
            func_name = func.func.__name__
        span = (
            self._tracer.start_span(f"{ASYNCIO_PREFIX} to_thread-" + func_name)
            if func_name in self._to_thread_name_to_trace
            else None
        )
        attr = {"type": "to_thread", "name": func_name}
        exception = None
        try:
            attr["state"] = "finished"
            return func
        except Exception:
            attr["state"] = "exception"
            raise
        finally:
            self.record_process(start, attr, span, exception)

    def trace_item(self, coro_or_future):
        """Trace a coroutine or future item."""
        # Task is already traced, return it
        if isinstance(coro_or_future, asyncio.Task):
            return coro_or_future
        if asyncio.iscoroutine(coro_or_future):
            return self.trace_coroutine(coro_or_future)
        if futures.isfuture(coro_or_future):
            return self.trace_future(coro_or_future)
        return coro_or_future

    async def trace_coroutine(self, coro):
        """
        Wrap a coroutine so that we measure its duration, metrics, etc.
        If already instrumented, simply 'await coro' to preserve call behavior.
        """
        if _is_instrumented(coro):
            return await coro

        if not hasattr(coro, "__name__"):
            return await coro
        start = default_timer()
        attr = {
            "type": "coroutine",
            "name": coro.__name__,
        }
        span = (
            self._tracer.start_span(f"{ASYNCIO_PREFIX} coro-" + coro.__name__)
            if coro.__name__ in self._coros_name_to_trace
            else None
        )
        exception = None
        try:
            attr["state"] = "finished"
            return await coro
        # CancelledError is raised when a coroutine is cancelled
        # before it has a chance to run. We don't want to record
        # this as an error.
        # Still it needs to be raised in order for `asyncio.wait_for`
        # to properly work with timeout and raise accordingly `asyncio.TimeoutError`
        except asyncio.CancelledError:
            attr["state"] = "cancelled"
            raise
        except Exception as exc:
            exception = exc
            state = determine_state(exception)
            attr["state"] = state
            raise
        finally:
            self.record_process(start, attr, span, exception)

    def trace_future(self, future):
        """
        Wrap a Future's done callback. If already instrumented, skip re-wrapping.
        """
        if _is_instrumented(future):
            return future

        start = default_timer()
        span = (
            self._tracer.start_span(f"{ASYNCIO_PREFIX} future")
            if self._future_active_enabled
            else None
        )

        def callback(f):
            attr = {
                "type": "future",
                "state": (
                    "cancelled"
                    if f.cancelled()
                    else determine_state(f.exception())
                ),
            }
            self.record_process(
                start, attr, span, None if f.cancelled() else f.exception()
            )

        future.add_done_callback(callback)
        return future

    def record_process(
        self, start: float, attr: dict, span=None, exception=None
    ) -> None:
        """
        Record the processing time, update histogram and counter, and handle span.

        :param start: Start time of the process.
        :param attr: Attributes for the histogram and counter.
        :param span: Optional span for tracing.
        :param exception: Optional exception occurred during the process.
        """
        duration = max(default_timer() - start, 0)
        self.process_duration_histogram.record(duration, attr)
        self.process_created_counter.add(1, attr)

        if span:
            if span.is_recording() and exception:
                span.set_status(Status(StatusCode.ERROR))
                span.record_exception(exception)
            span.end()


def determine_state(exception: Exception) -> str:
    if isinstance(exception, asyncio.CancelledError):
        return "cancelled"
    if isinstance(exception, asyncio.TimeoutError):
        return "timeout"
    if exception:
        return "exception"
    return "finished"


def uninstrument_taskgroup_create_task() -> None:
    # TaskGroup.create_task was added in Python 3.11
    if sys.version_info < (3, 11):
        return
    unwrap(asyncio.TaskGroup, "create_task")  # pylint: disable=no-member


def uninstrument_to_thread() -> None:
    unwrap(asyncio, "to_thread")


def uninstrument_gather() -> None:
    unwrap(asyncio, "gather")


def uninstrument_method_with_coroutine(method_name: str) -> None:
    """
    Uninstrument specified asyncio method.
    """
    unwrap(asyncio, method_name)
