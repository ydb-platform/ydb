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

import asyncio
from asyncio import iscoroutine
from asyncio import Future

try:
    from types import AsyncGeneratorType
except ImportError:

    class AsyncGeneratorType:
        pass


import io
import os
import sys
import time
import weakref
import functools
import traceback
import logging
import inspect

from datetime import datetime

from txaio.interfaces import IFailedFuture, ILogger, log_levels
from txaio._iotype import guess_stream_needs_encoding
from txaio._common import _BatchedTimer
from txaio import _util
from txaio import _Config


config = _Config()


def with_config(loop=None):
    """
    :return: an instance of the txaio API with the given
        configuration. This won't affect anything using the 'gloabl'
        config nor other instances created using this function.

    If you need to customize txaio configuration separately (e.g. to
    use multiple event-loops in asyncio), you can take code like this:

        import txaio


        class FunTimes(object):

            def something_async(self):
                return txaio.call_later(1, lambda: 'some result')

    and instead do this:

        import txaio


        class FunTimes(object):
            txaio = txaio

            def something_async(self):
                # this will run in the local/new event loop created in the constructor
                return self.txaio.call_later(1, lambda: 'some result')

        fun0 = FunTimes()
        fun1 = FunTimes()
        fun1.txaio = txaio.with_config(loop=asyncio.new_event_loop())

    So `fun1` will run its futures on the newly-created event loop,
    while `fun0` will work just as it did before this `with_config`
    method was introduced (after 2.6.2).
    """
    cfg = _Config()
    if loop is not None:
        cfg.loop = loop
    return _AsyncioApi(cfg)


# logging should probably all be folded into _AsyncioApi as well
_stderr, _stdout = sys.stderr, sys.stdout
_loggers = (
    weakref.WeakSet()
)  # weak-ref's of each logger we've created before start_logging()
_log_level = "info"  # re-set by start_logging
_started_logging = False
_categories = {}


def add_log_categories(categories):
    _categories.update(categories)


class FailedFuture(IFailedFuture):
    """
    This provides an object with any features from Twisted's Failure
    that we might need in Autobahn classes that use FutureMixin.

    We need to encapsulate information from exceptions so that
    errbacks still have access to the traceback (in case they want to
    print it out) outside of "except" blocks.
    """

    def __init__(self, type_, value, traceback):
        """
        These are the same parameters as returned from ``sys.exc_info()``

        :param type_: exception type
        :param value: the Exception instance
        :param traceback: a traceback object
        """
        self._type = type_
        self._value = value
        self._traceback = traceback

    @property
    def value(self):
        return self._value

    def __str__(self):
        return str(self.value)


# logging API methods


def _log(logger, level, format="", **kwargs):
    # Look for a log_category, switch it in if we have it
    if "log_category" in kwargs and kwargs["log_category"] in _categories:
        format = _categories.get(kwargs["log_category"])

    kwargs["log_time"] = time.time()
    kwargs["log_level"] = level
    kwargs["log_format"] = format
    # NOTE: turning kwargs into a single "argument which
    # is a dict" on purpose, since a LogRecord only keeps
    # args, not kwargs.
    if level == "trace":
        level = "debug"
        kwargs["txaio_trace"] = True

    msg = format.format(**kwargs)
    getattr(logger._logger, level)(msg)


def _no_op(*args, **kw):
    pass


class _TxaioLogWrapper(ILogger):
    def __init__(self, logger):
        self._logger = logger
        self._set_log_level(_log_level)

    def emit(self, level, *args, **kwargs):
        func = getattr(self, level)
        return func(*args, **kwargs)

    def _set_log_level(self, level):
        target_level = log_levels.index(level)
        # this binds either _log or _no_op above to this instance,
        # depending on the desired level.
        for idx, name in enumerate(log_levels):
            if idx <= target_level:
                log_method = functools.partial(_log, self, name)
            else:
                log_method = _no_op
            setattr(self, name, log_method)
        self._log_level = level


class _TxaioFileHandler(logging.Handler, object):
    def __init__(self, fileobj, **kw):
        super(_TxaioFileHandler, self).__init__(**kw)
        self._file = fileobj
        self._encode = guess_stream_needs_encoding(fileobj)

    def emit(self, record):
        if isinstance(record.args, dict):
            fmt = record.args.get("log_format", record.args.get("log_message", ""))
            message = fmt.format(**record.args)
            dt = datetime.fromtimestamp(record.args.get("log_time", 0))
        else:
            message = record.getMessage()
            if record.levelno == logging.ERROR and record.exc_info:
                message += "\n"
                for line in traceback.format_exception(*record.exc_info):
                    message = message + line
            dt = datetime.fromtimestamp(record.created)
        msg = "{0} {1}{2}".format(
            dt.strftime("%Y-%m-%dT%H:%M:%S%z"), message, os.linesep
        )
        if self._encode:
            msg = msg.encode("utf8")
        self._file.write(msg)


def make_logger():
    # we want the namespace to be the calling context of "make_logger"
    # otherwise the root logger will be returned
    cf = inspect.currentframe().f_back
    if "self" in cf.f_locals:
        # We're probably in a class init or method
        cls = cf.f_locals["self"].__class__
        namespace = "{0}.{1}".format(cls.__module__, cls.__name__)
    else:
        namespace = cf.f_globals["__name__"]
        if cf.f_code.co_name != "<module>":
            # If it's not the module, and not in a class instance, add the code
            # object's name.
            namespace = namespace + "." + cf.f_code.co_name

    logger = _TxaioLogWrapper(logging.getLogger(name=namespace))
    # remember this so we can set their levels properly once
    # start_logging is actually called
    _loggers.add(logger)
    return logger


def start_logging(out=_stdout, level="info"):
    """
    Begin logging.

    :param out: if provided, a file-like object to log to. By default, this is
                stdout.
    :param level: the maximum log-level to emit (a string)
    """
    global _log_level, _started_logging
    if level not in log_levels:
        raise RuntimeError(
            "Invalid log level '{0}'; valid are: {1}".format(
                level, ", ".join(log_levels)
            )
        )

    if _started_logging:
        return

    _started_logging = True
    _log_level = level

    handler = _TxaioFileHandler(out)
    logging.getLogger().addHandler(handler)
    # note: Don't need to call basicConfig() or similar, because we've
    # now added at least one handler to the root logger
    logging.raiseExceptions = True  # FIXME
    level_to_stdlib = {
        "critical": logging.CRITICAL,
        "error": logging.ERROR,
        "warn": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "trace": logging.DEBUG,
    }
    logging.getLogger().setLevel(level_to_stdlib[level])
    # make sure any loggers we created before now have their log-level
    # set (any created after now will get it from _log_level
    for logger in _loggers:
        logger._set_log_level(level)


def set_global_log_level(level):
    """
    Set the global log level on all loggers instantiated by txaio.
    """
    for logger in _loggers:
        logger._set_log_level(level)
    global _log_level
    _log_level = level


def get_global_log_level():
    return _log_level


# asyncio API methods; the module-level functions are (now, for
# backwards-compat) exported from a default instance of this class


_unspecified = object()


class _AsyncioApi(object):
    using_twisted = False
    using_asyncio = True

    def __init__(self, config):
        self._config = config

    @property
    def _loop(self):
        # if configured explicetly, then use this loop
        if self._config.loop:
            return self._config.loop

        # otherwise give out the event loop of the thread this is called in
        # rather fetching the loop once in __init__, which may not neccessarily
        # be called from the thread we now run the event loop in.
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def failure_message(self, fail):
        """
        :param fail: must be an IFailedFuture
        returns a unicode error-message
        """
        try:
            return "{0}: {1}".format(
                fail._value.__class__.__name__,
                str(fail._value),
            )
        except Exception:
            return 'Failed to produce failure message for "{0}"'.format(fail)

    def failure_traceback(self, fail):
        """
        :param fail: must be an IFailedFuture
        returns a traceback instance
        """
        return fail._traceback

    def failure_format_traceback(self, fail):
        """
        :param fail: must be an IFailedFuture
        returns a string
        """
        try:
            f = io.StringIO()
            traceback.print_exception(
                fail._type,
                fail.value,
                fail._traceback,
                file=f,
            )
            return f.getvalue()
        except Exception:
            return "Failed to format failure traceback for '{0}'".format(fail)

    def create_future(
        self, result=_unspecified, error=_unspecified, canceller=_unspecified
    ):
        if result is not _unspecified and error is not _unspecified:
            raise ValueError("Cannot have both result and error.")

        f = self._loop.create_future()
        if result is not _unspecified:
            resolve(f, result)
        elif error is not _unspecified:
            reject(f, error)

        # Twisted's only API for cancelling is to pass a
        # single-argument callable to the Deferred constructor, so
        # txaio apes that here for asyncio. The argument is the Future
        # that has been cancelled.
        if canceller is not _unspecified:

            def done(f):
                try:
                    f.exception()
                except asyncio.CancelledError:
                    canceller(f)

            f.add_done_callback(done)

        return f

    def create_future_success(self, result):
        return self.create_future(result=result)

    def create_future_error(self, error=None):
        f = self.create_future()
        reject(f, error)
        return f

    def as_future(self, fun, *args, **kwargs):
        try:
            res = fun(*args, **kwargs)
        except Exception:
            return create_future_error(create_failure())
        else:
            if isinstance(res, Future):
                return res
            elif iscoroutine(res):
                return self._loop.create_task(res)
            elif isinstance(res, AsyncGeneratorType):
                raise RuntimeError(
                    "as_future() received an async generator function; does "
                    "'{}' use 'yield' when you meant 'await'?".format(str(fun))
                )
            else:
                return create_future_success(res)

    def is_future(self, obj):
        return iscoroutine(obj) or isinstance(obj, Future)

    def call_later(self, delay, fun, *args, **kwargs):
        # loop.call_later doesn't support kwargs
        real_call = functools.partial(fun, *args, **kwargs)
        return self._loop.call_later(delay, real_call)

    def make_batched_timer(self, bucket_seconds, chunk_size=100):
        """
        Creates and returns an object implementing
        :class:`txaio.IBatchedTimer`.

        :param bucket_seconds: the number of seconds in each bucket. That
            is, a value of 5 means that any timeout within a 5 second
            window will be in the same bucket, and get notified at the
            same time. This is only accurate to "milliseconds".

        :param chunk_size: when "doing" the callbacks in a particular
            bucket, this controls how many we do at once before yielding to
            the reactor.
        """

        def get_seconds():
            return self._loop.time()

        return _BatchedTimer(
            bucket_seconds * 1000.0,
            chunk_size,
            seconds_provider=get_seconds,
            delayed_call_creator=self.call_later,
        )

    def is_called(self, future):
        return future.done()

    def resolve(self, future, result=None):
        future.set_result(result)

    def reject(self, future, error=None):
        if error is None:
            error = create_failure()  # will be error if we're not in an "except"
        elif isinstance(error, Exception):
            error = FailedFuture(type(error), error, None)
        else:
            if not isinstance(error, IFailedFuture):
                raise RuntimeError("reject requires an IFailedFuture or Exception")
        future.set_exception(error.value)

    def cancel(self, future, msg=None):
        if sys.version_info >= (3, 9):
            future.cancel(msg)
        else:
            future.cancel()

    def create_failure(self, exception=None):
        """
        This returns an object implementing IFailedFuture.

        If exception is None (the default) we MUST be called within an
        "except" block (such that sys.exc_info() returns useful
        information).
        """
        if exception:
            return FailedFuture(type(exception), exception, None)
        return FailedFuture(*sys.exc_info())

    def add_callbacks(self, future, callback, errback):
        """
        callback or errback may be None, but at least one must be
        non-None.
        """

        def done(f):
            try:
                res = f.result()
                if callback:
                    callback(res)
            except (Exception, asyncio.CancelledError):
                if errback:
                    errback(create_failure())

        return future.add_done_callback(done)

    def gather(self, futures, consume_exceptions=True):
        """
        This returns a Future that waits for all the Futures in the list
        ``futures``

        :param futures: a list of Futures (or coroutines?)

        :param consume_exceptions: if True, any errors are eaten and
        returned in the result list.
        """

        # from the asyncio docs: "If return_exceptions is True, exceptions
        # in the tasks are treated the same as successful results, and
        # gathered in the result list; otherwise, the first raised
        # exception will be immediately propagated to the returned
        # future."
        return asyncio.gather(*futures, return_exceptions=consume_exceptions)

    def sleep(self, delay):
        """
        Inline sleep for use in co-routines.

        :param delay: Time to sleep in seconds.
        :type delay: float
        """
        return asyncio.ensure_future(asyncio.sleep(delay))


_default_api = _AsyncioApi(config)


using_twisted = _default_api.using_twisted
using_asyncio = _default_api.using_asyncio
sleep = _default_api.sleep
failure_message = _default_api.failure_message
failure_traceback = _default_api.failure_traceback
failure_format_traceback = _default_api.failure_format_traceback
create_future = _default_api.create_future
create_future_success = _default_api.create_future_success
create_future_error = _default_api.create_future_error
as_future = _default_api.as_future
is_future = _default_api.is_future
call_later = _default_api.call_later
make_batched_timer = _default_api.make_batched_timer
is_called = _default_api.is_called
resolve = _default_api.resolve
reject = _default_api.reject
cancel = _default_api.cancel
create_failure = _default_api.create_failure
add_callbacks = _default_api.add_callbacks
gather = _default_api.gather
sleep = _default_api.sleep
time_ns = _util.time_ns
perf_counter_ns = _util.perf_counter_ns
