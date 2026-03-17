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


from txaio._version import __version__
from txaio.interfaces import IFailedFuture, ILogger

version = __version__

# This is the API
# see tx.py for Twisted implementation
# see aio.py for asyncio implementation


class _Config(object):
    """
    This holds all valid configuration options, accessed as
    class-level variables. For example, if you were using asyncio:

    .. sourcecode:: python

        txaio.config.loop = asyncio.get_event_loop()

    ``loop`` is populated automatically (while importing one of the
    framework-specific libraries) but can be changed before any call
    into this library. Currently, it's only used by :meth:`call_later`
    If using asyncio, you must set this to an event-loop (by default,
    we use asyncio.get_event_loop). If using Twisted, set this to a
    reactor instance (by default we "from twisted.internet import
    reactor" on the first call to call_later)
    """

    #: the event-loop object to use
    loop = None


__all__ = (
    "with_config",  # allow mutliple custom configurations at once
    "using_twisted",  # True if we're using Twisted
    "using_asyncio",  # True if we're using asyncio
    "use_twisted",  # sets the library to use Twisted, or exception
    "use_asyncio",  # sets the library to use asyncio, or exception
    "config",  # the config instance, access via attributes
    "create_future",  # create a Future (can be already resolved/errored)
    "create_future_success",
    "create_future_error",
    "create_failure",  # return an object implementing IFailedFuture
    "as_future",  # call a method, and always return a Future
    "is_future",  # True for Deferreds in tx and Futures, @coroutines in asyncio
    "reject",  # errback a Future
    "resolve",  # callback a Future
    "cancel",  # cancel a Future
    "add_callbacks",  # add callback and/or errback
    "gather",  # return a Future waiting for several other Futures
    "is_called",  # True if the Future has a result
    "call_later",  # call the callback after the given delay seconds
    "failure_message",  # a printable error-message from a IFailedFuture
    "failure_traceback",  # returns a traceback instance from an IFailedFuture
    "failure_format_traceback",  # a string, the formatted traceback
    "make_batched_timer",  # create BatchedTimer/IBatchedTimer instances
    "make_logger",  # creates an object implementing ILogger
    "start_logging",  # initializes logging (may grab stdin at this point)
    "set_global_log_level",  # Set the global log level
    "get_global_log_level",  # Get the global log level
    "add_log_categories",
    "IFailedFuture",  # describes API for arg to errback()s
    "ILogger",  # API for logging
    "sleep",  # little helper for inline sleeping
    "time_ns",  # helper: current time (UTC) in ns
    "perf_counter_ns",  # helper: current performance counter in ns
)


_explicit_framework = None


def use_twisted():
    global _explicit_framework
    if _explicit_framework is not None and _explicit_framework != "twisted":
        raise RuntimeError("Explicitly using '{}' already".format(_explicit_framework))
    _explicit_framework = "twisted"
    from txaio import tx

    _use_framework(tx)
    import txaio

    txaio.using_twisted = True
    txaio.using_asyncio = False


def use_asyncio():
    global _explicit_framework
    if _explicit_framework is not None and _explicit_framework != "asyncio":
        raise RuntimeError("Explicitly using '{}' already".format(_explicit_framework))
    _explicit_framework = "asyncio"
    from txaio import aio

    _use_framework(aio)
    import txaio

    txaio.using_twisted = False
    txaio.using_asyncio = True


def _use_framework(module):
    """
    Internal helper, to set this modules methods to a specified
    framework helper-methods.
    """
    import txaio

    for method_name in __all__:
        if method_name in ["use_twisted", "use_asyncio"]:
            continue
        setattr(txaio, method_name, getattr(module, method_name))


# use the "un-framework", which is neither asyncio nor twisted and
# just throws an exception -- this forces you to call .use_twisted()
# or .use_asyncio() to use the library.
from txaio import _unframework  # noqa

_use_framework(_unframework)
