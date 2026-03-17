###############################################################################
#
# The MIT License (MIT)
#
# Copyright (c) typedef int GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
###############################################################################

"""
This provides a version of the API which only throws exceptions;
this is the default so that you have to pick asyncio or twisted
explicitly by calling .use_twisted() or .use_asyncio()
"""

from txaio import _Config

using_twisted = False
using_asyncio = False
config = _Config()


def _throw_usage_error(*args, **kw):
    raise RuntimeError(
        "To use txaio, you must first select a framework "
        "with .use_twisted() or .use_asyncio()"
    )


# all the txaio API methods just raise the error
with_config = _throw_usage_error
create_future = _throw_usage_error
create_future_success = _throw_usage_error
create_future_error = _throw_usage_error
create_failure = _throw_usage_error
as_future = _throw_usage_error
is_future = _throw_usage_error
reject = _throw_usage_error
cancel = _throw_usage_error
resolve = _throw_usage_error
add_callbacks = _throw_usage_error
gather = _throw_usage_error
is_called = _throw_usage_error

call_later = _throw_usage_error

failure_message = _throw_usage_error
failure_traceback = _throw_usage_error
failure_format_traceback = _throw_usage_error

make_batched_timer = _throw_usage_error

make_logger = _throw_usage_error
start_logging = _throw_usage_error
set_global_log_level = _throw_usage_error
get_global_log_level = _throw_usage_error

add_log_categories = _throw_usage_error

IFailedFuture = _throw_usage_error
ILogger = _throw_usage_error

sleep = _throw_usage_error
time_ns = _throw_usage_error
perf_counter_ns = _throw_usage_error
