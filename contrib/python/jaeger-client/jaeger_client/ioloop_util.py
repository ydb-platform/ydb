# Copyright (c) 2016 Uber Technologies, Inc.
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


import sys
from tornado import gen
from tornado.concurrent import Future


def submit(fn, io_loop, *args, **kwargs):
    """Submit Tornado Coroutine to IOLoop.current().

    :param fn: Tornado Coroutine to execute
    :param io_loop: Tornado IOLoop where to schedule the coroutine
    :param args: Args to pass to coroutine
    :param kwargs: Kwargs to pass to coroutine
    :returns tornado.concurrent.Future: future result of coroutine
    """
    future = Future()

    def execute():
        """Execute fn on the IOLoop."""
        try:
            result = gen.maybe_future(fn(*args, **kwargs))
        except Exception:
            # The function we ran didn't return a future and instead raised
            # an exception. Let's pretend that it returned this dummy
            # future with our stack trace.
            f = gen.Future()
            f.set_exc_info(sys.exc_info())
            on_done(f)
        else:
            result.add_done_callback(on_done)

    def on_done(tornado_future):
        """
        Set tornado.Future results to the concurrent.Future.
        :param tornado_future:
        """
        exception = tornado_future.exception()
        if not exception:
            future.set_result(tornado_future.result())
        else:
            future.set_exception(exception)

    io_loop.add_callback(execute)

    return future


def future_result(result):
    future = Future()
    future.set_result(result)
    return future


def future_exception(exception):
    future = Future()
    future.set_exception(exception)
    return future
