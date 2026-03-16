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

import io
import os
import sys
import weakref
import inspect

from functools import partial

from twisted.python.failure import Failure
from twisted.internet.defer import maybeDeferred, Deferred, DeferredList
from twisted.internet.defer import succeed, fail
from twisted.internet.interfaces import IReactorTime

from zope.interface import provider

from txaio.interfaces import IFailedFuture, ILogger, log_levels
from txaio._iotype import guess_stream_needs_encoding
from txaio import _Config
from txaio._common import _BatchedTimer
from txaio import _util
from twisted.logger import Logger as _Logger, formatEvent, ILogObserver
from twisted.logger import globalLogBeginner, formatTime, LogLevel

from twisted.internet.defer import ensureDeferred
from asyncio import iscoroutinefunction

using_twisted = True
using_asyncio = False

config = _Config()
_stderr, _stdout = sys.stderr, sys.stdout

# some book-keeping variables here. _observer is used as a global by
# the "backwards compatible" (Twisted < 15) loggers. The _loggers object
# is a weak-ref set; we add Logger instances to this *until* such
# time as start_logging is called (with the desired log-level) and
# then we call _set_log_level on each instance. After that,
# Logger's ctor uses _log_level directly.
_observer = None  # for Twisted legacy logging support; see below
_loggers = weakref.WeakSet()  # weak-references of each logger we've created
_log_level = "info"  # global log level; possibly changed in start_logging()
_started_logging = False

_categories = {}

IFailedFuture.register(Failure)
ILogger.register(_Logger)


def _no_op(*args, **kwargs):
    pass


def add_log_categories(categories):
    _categories.update(categories)


def with_config(loop=None):
    if loop is not None:
        if config.loop is not None and config.loop is not loop:
            raise RuntimeError(
                "Twisted has only a single, global reactor. You passed in "
                "a reactor different from the one already configured "
                "in txaio.config.loop"
            )
    return _TxApi(config)


# NOTE: beware that twisted.logger._logger.Logger copies itself via an
# overriden __get__ method when used as recommended as a class
# descriptor.  So, we override __get__ to just return ``self`` which
# means ``log_source`` will be wrong, but we don't document that as a
# key that you can depend on anyway :/
class Logger(object):
    def __init__(self, level=None, logger=None, namespace=None, observer=None):
        assert logger, "Should not be instantiated directly."

        self._logger = logger(observer=observer, namespace=namespace)
        self._log_level_set_explicitly = False

        if level:
            self.set_log_level(level)
        else:
            self._set_log_level(_log_level)

        _loggers.add(self)

    def __get__(self, oself, type=None):
        # this causes the Logger to lie about the "source=", but
        # otherwise we create a new Logger instance every time we do
        # "self.log.info()" if we use it like:
        # class Foo:
        #     log = make_logger
        return self

    def _log(self, level, *args, **kwargs):
        # Look for a log_category, switch it in if we have it
        if "log_category" in kwargs and kwargs["log_category"] in _categories:
            args = tuple()
            kwargs["format"] = _categories.get(kwargs["log_category"])

        self._logger.emit(level, *args, **kwargs)

    def emit(self, level, *args, **kwargs):
        if log_levels.index(self._log_level) < log_levels.index(level):
            return

        if level == "trace":
            return self._trace(*args, **kwargs)

        level = LogLevel.lookupByName(level)
        return self._log(level, *args, **kwargs)

    def set_log_level(self, level, keep=True):
        """
        Set the log level. If keep is True, then it will not change along with
        global log changes.
        """
        self._set_log_level(level)
        self._log_level_set_explicitly = keep

    def _set_log_level(self, level):
        # up to the desired level, we don't do anything, as we're a
        # "real" Twisted new-logger; for methods *after* the desired
        # level, we bind to the no_op method
        desired_index = log_levels.index(level)

        for idx, name in enumerate(log_levels):
            if name == "none":
                continue

            if idx > desired_index:
                current = getattr(self, name, None)
                if not current == _no_op or current is None:
                    setattr(self, name, _no_op)
                if name == "error":
                    setattr(self, "failure", _no_op)

            else:
                if getattr(self, name, None) in (_no_op, None):
                    if name == "trace":
                        setattr(self, "trace", self._trace)
                    else:
                        setattr(
                            self, name, partial(self._log, LogLevel.lookupByName(name))
                        )

                    if name == "error":
                        setattr(self, "failure", self._failure)

        self._log_level = level

    def _failure(self, format=None, *args, **kw):
        return self._logger.failure(format, *args, **kw)

    def _trace(self, *args, **kw):
        # there is no "trace" level in Twisted -- but this whole
        # method will be no-op'd unless we are at the 'trace' level.
        self.debug(*args, txaio_trace=True, **kw)


def make_logger(level=None, logger=_Logger, observer=None):
    # we want the namespace to be the calling context of "make_logger"
    # -- so we *have* to pass namespace kwarg to Logger (or else it
    # will always say the context is "make_logger")
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
    logger = Logger(level=level, namespace=namespace, logger=logger, observer=observer)
    return logger


@provider(ILogObserver)
class _LogObserver(object):
    """
    Internal helper.

    An observer which formats events to a given file.
    """

    to_tx = {
        "critical": LogLevel.critical,
        "error": LogLevel.error,
        "warn": LogLevel.warn,
        "info": LogLevel.info,
        "debug": LogLevel.debug,
        "trace": LogLevel.debug,
    }

    def __init__(self, out):
        self._file = out
        self._encode = guess_stream_needs_encoding(out)

        self._levels = None

    def _acceptable_level(self, level):
        if self._levels is None:
            target_level = log_levels.index(_log_level)
            self._levels = [
                self.to_tx[lvl]
                for lvl in log_levels
                if log_levels.index(lvl) <= target_level and lvl != "none"
            ]
        return level in self._levels

    def __call__(self, event):
        # it seems if a twisted.logger.Logger() has .failure() called
        # on it, the log_format will be None for the traceback after
        # "Unhandled error in Deferred" -- perhaps this is a Twisted
        # bug?
        if event["log_format"] is None:
            msg = "{0} {1}{2}".format(
                formatTime(event["log_time"]),
                failure_format_traceback(event["log_failure"]),
                os.linesep,
            )
            if self._encode:
                msg = msg.encode("utf8")
            self._file.write(msg)
        else:
            # although Logger will already have filtered out unwanted
            # levels, bare Logger instances from Twisted code won't have.
            if "log_level" in event and self._acceptable_level(event["log_level"]):
                msg = "{0} {1}{2}".format(
                    formatTime(event["log_time"]),
                    formatEvent(event),
                    os.linesep,
                )
                if self._encode:
                    msg = msg.encode("utf8")

                self._file.write(msg)


def start_logging(out=_stdout, level="info"):
    """
    Start logging to the file-like object in ``out``. By default, this
    is stdout.
    """
    global _observer, _log_level, _started_logging

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
    set_global_log_level(_log_level)

    if out:
        _observer = _LogObserver(out)

    _observers = []
    if _observer:
        _observers.append(_observer)
    globalLogBeginner.beginLoggingTo(_observers)


_unspecified = object()


class _TxApi(object):
    def __init__(self, config):
        self._config = config

    def failure_message(self, fail):
        """
        :param fail: must be an IFailedFuture
        returns a unicode error-message
        """
        try:
            return "{0}: {1}".format(
                fail.value.__class__.__name__,
                fail.getErrorMessage(),
            )
        except Exception:
            return 'Failed to produce failure message for "{0}"'.format(fail)

    def failure_traceback(self, fail):
        """
        :param fail: must be an IFailedFuture
        returns a traceback instance
        """
        return fail.tb

    def failure_format_traceback(self, fail):
        """
        :param fail: must be an IFailedFuture
        returns a string
        """
        try:
            f = io.StringIO()
            fail.printTraceback(file=f)
            return f.getvalue()
        except Exception:
            return "Failed to format failure traceback for '{0}'".format(fail)

    def create_future(self, result=_unspecified, error=_unspecified, canceller=None):
        if result is not _unspecified and error is not _unspecified:
            raise ValueError("Cannot have both result and error.")

        f: Deferred = Deferred(canceller=canceller)
        if result is not _unspecified:
            resolve(f, result)
        elif error is not _unspecified:
            reject(f, error)
        return f

    def create_future_success(self, result):
        return succeed(result)

    def create_future_error(self, error=None):
        return fail(create_failure(error))

    def as_future(self, fun, *args, **kwargs):
        # Twisted doesn't automagically deal with coroutines on Py3
        if iscoroutinefunction(fun):
            try:
                return ensureDeferred(fun(*args, **kwargs))
            except TypeError as e:
                return create_future_error(e)
        return maybeDeferred(fun, *args, **kwargs)

    def is_future(self, obj):
        return isinstance(obj, Deferred)

    def call_later(self, delay, fun, *args, **kwargs):
        return IReactorTime(self._get_loop()).callLater(delay, fun, *args, **kwargs)

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
            return self._get_loop().seconds()

        def create_delayed_call(delay, fun, *args, **kwargs):
            return self._get_loop().callLater(delay, fun, *args, **kwargs)

        return _BatchedTimer(
            bucket_seconds * 1000.0,
            chunk_size,
            seconds_provider=get_seconds,
            delayed_call_creator=create_delayed_call,
        )

    def is_called(self, future):
        return future.called

    def resolve(self, future, result=None):
        future.callback(result)

    def reject(self, future, error=None):
        if error is None:
            error = create_failure()
        elif isinstance(error, Exception):
            error = Failure(error)
        else:
            if not isinstance(error, Failure):
                raise RuntimeError("reject requires a Failure or Exception")
        future.errback(error)

    def cancel(self, future, msg=None):
        future.cancel()

    def create_failure(self, exception=None):
        """
        Create a Failure instance.

        if ``exception`` is None (the default), we MUST be inside an
        "except" block. This encapsulates the exception into an object
        that implements IFailedFuture
        """
        if exception:
            return Failure(exception)
        return Failure()

    def add_callbacks(self, future, callback, errback):
        """
        callback or errback may be None, but at least one must be
        non-None.
        """
        assert future is not None
        if callback is None:
            assert errback is not None
            future.addErrback(errback)
        else:
            # Twisted allows errback to be None here
            future.addCallbacks(callback, errback)
        return future

    def gather(self, futures, consume_exceptions=True):
        def completed(res):
            rtn = []
            for ok, value in res:
                rtn.append(value)
                if not ok and not consume_exceptions:
                    value.raiseException()
            return rtn

        # XXX if consume_exceptions is False in asyncio.gather(), it will
        # abort on the first raised exception -- should we set
        # fireOnOneErrback=True (if consume_exceptions=False?) -- but then
        # we'll have to wrap the errback() to extract the "real" failure
        # from the FirstError that gets thrown if you set that ...

        dl = DeferredList(list(futures), consumeErrors=consume_exceptions)
        # we unpack the (ok, value) tuples into just a list of values, so
        # that the callback() gets the same value in asyncio and Twisted.
        add_callbacks(dl, completed, None)
        return dl

    def sleep(self, delay):
        """
        Inline sleep for use in co-routines.

        :param delay: Time to sleep in seconds.
        :type delay: float
        """
        d: Deferred = Deferred()
        self._get_loop().callLater(delay, d.callback, None)
        return d

    def _get_loop(self):
        """
        internal helper
        """
        # we import and assign the default here (and not, e.g., when
        # making Config) so as to delay importing reactor as long as
        # possible in case someone is installing a custom one.
        if self._config.loop is None:
            from twisted.internet import reactor

            self._config.loop = reactor
        return self._config.loop


def set_global_log_level(level):
    """
    Set the global log level on all loggers instantiated by txaio.
    """
    for item in _loggers:
        if not item._log_level_set_explicitly:
            item._set_log_level(level)
    global _log_level
    _log_level = level


def get_global_log_level():
    return _log_level


_default_api = _TxApi(config)


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
