# Copyright 2014-2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tornado compatibility layer for Motor, an asynchronous MongoDB driver.

See "Frameworks" in the Developer Guide.
"""

import functools
import os
import tornado.process
import warnings

from concurrent.futures import ThreadPoolExecutor
from tornado import concurrent, gen, ioloop, version as tornado_version
from tornado.gen import chain_future, coroutine  # For framework interface.


try:
    import contextvars
except ImportError:
    contextvars = None


CLASS_PREFIX = ''


def get_event_loop():
    return ioloop.IOLoop.current()


def is_event_loop(loop):
    return isinstance(loop, ioloop.IOLoop)


def check_event_loop(loop):
    if not is_event_loop(loop):
        raise TypeError(
            "io_loop must be instance of IOLoop, not %r" % loop)


def get_future(loop):
    return concurrent.Future()


if 'MOTOR_MAX_WORKERS' in os.environ:
    max_workers = int(os.environ['MOTOR_MAX_WORKERS'])
else:
    max_workers = tornado.process.cpu_count() * 5

_EXECUTOR = ThreadPoolExecutor(max_workers=max_workers)


def run_on_executor(loop, fn, *args, **kwargs):
    if contextvars:
        context = contextvars.copy_context()
        fn = functools.partial(context.run, fn)

    return loop.run_in_executor(
        _EXECUTOR, functools.partial(fn, *args, **kwargs))


def chain_return_value(future, loop, return_value):
    """Compatible way to return a value in all Pythons.

    PEP 479, raise StopIteration(value) from a coroutine won't work forever,
    but "return value" doesn't work in Python 2. Instead, Motor methods that
    return values resolve a Future with it, and are implemented with callbacks
    rather than a coroutine internally.
    """
    chained = concurrent.Future()

    def copy(_future):
        # Return early if the task was cancelled.
        if chained.done():
            return
        if _future.exception() is not None:
            chained.set_exception(_future.exception())
        else:
            chained.set_result(return_value)

    future.add_done_callback(functools.partial(loop.add_callback, copy))
    return chained


def is_future(f):
    return isinstance(f, concurrent.Future)


def call_soon(loop, callback, *args, **kwargs):
    if args or kwargs:
        loop.add_callback(functools.partial(callback, *args, **kwargs))
    else:
        loop.add_callback(callback)


def add_future(loop, future, callback, *args):
    loop.add_future(future, functools.partial(callback, *args))


def pymongo_class_wrapper(f, pymongo_class):
    """Executes the coroutine f and wraps its result in a Motor class.

    See WrapAsync.
    """
    @functools.wraps(f)
    async def _wrapper(self, *args, **kwargs):
        result = await f(self, *args, **kwargs)

        # Don't call isinstance(), not checking subclasses.
        if result.__class__ == pymongo_class:
            # Delegate to the current object to wrap the result.
            return self.wrap(result)
        else:
            return result

    return _wrapper


def yieldable(future):
    warnings.warn(
        "The yieldable function is deprecated and will be removed in "
        "Motor 3.0", DeprecationWarning, stacklevel=2)
    return future


def platform_info():
    return 'Tornado %s' % (tornado_version,)
