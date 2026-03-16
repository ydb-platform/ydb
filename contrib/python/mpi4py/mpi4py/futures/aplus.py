# Author:  Lisandro Dalcin
# Contact: dalcinl@gmail.com
"""Support for Future chaining."""
# pylint: disable=broad-except

# This implementation is heavily inspired in code written by
# Daniel Dotsenko [@dvdotsenko] [dotsa at hotmail.com]
# https://github.com/dvdotsenko/python-future-then

import sys
import weakref
import threading
import functools

from ._core import Future


class ThenableFuture(Future):
    """*Thenable* `Future` subclass."""

    def then(self, on_success=None, on_failure=None):
        """Return ``then(self, on_success, on_failure)``."""
        return then(self, on_success, on_failure)

    def catch(self, on_failure=None):
        """Return ``catch(self, on_failure)``."""
        return catch(self, on_failure)


def then(future, on_success=None, on_failure=None):
    """JavaScript-like (`Promises/A+`_) support for Future chaining.

    Args:
        future: Input future instance.
        on_success (optional): Function to be called when the input future is
            successfully resolved. Once the input future is resolved, its
            result value is the input for `on_success`. If `on_success` is
            ``None``, the output future (i.e., the one returned by this
            function) will be resolved directly with the result of the input
            future. If `on_success` returns a future instance, the result
            future is chained to the output future. Otherwise, the result of
            `on_success` is used to resolve the output future.
        on_failure (optional): Function to be called when the input future is
            rejected. The Exception instance picked up from the rejected future
            is the input value for `on_failure`. If `on_failure` is ``None``,
            the output future (i.e., the one returned by this function) will be
            rejected directly with the exception of the input future.

    Returns:
        Output future to be resolved once in input future is resolved
        and either `on_success` or `on_failure` completes.

    .. _Promises/A+: https://promisesaplus.com/

    """
    new_future = future.__class__()
    done_cb = functools.partial(
        _done_cb, new_future,
        on_success=on_success,
        on_failure=on_failure)
    future.add_done_callback(done_cb)
    return new_future


def catch(future, on_failure=None):
    """Close equivalent to ``then(future, None, on_failure)``.

    Args:
        future: Input future instance.
        on_failure (optional): Function to be called when the input future is
            rejected. If `on_failure` is ``None``, the output future will be
            resolved with ``None`` thus ignoring the exception.

    """
    if on_failure is None:
        return then(future, None, lambda exc: None)
    return then(future, None, on_failure)


def _chain_log(new_future, future):
    with _chain_log.lock:
        registry = _chain_log.registry
        try:
            log = registry[new_future]
        except KeyError:
            log = weakref.WeakSet()
            registry[new_future] = log
        if future in log:
            raise RuntimeError(
                "Circular future chain detected: "
                "Future {0} is already in the resolved chain {1}"
                .format(future, set(log)))
        log.add(future)


_chain_log.lock = threading.Lock()  # type: ignore[attr-defined]
_chain_log.registry = weakref.WeakKeyDictionary()  # type: ignore[attr-defined]


def _chain_future(new_future, future):
    _chain_log(new_future, future)
    done_cb = functools.partial(_done_cb, new_future)
    future.add_done_callback(done_cb)


if sys.version_info[0] == 2:     # pragma: no cover
    def _sys_exception():
        exc = sys.exc_info()[1]
        return exc
else:                            # pragma: no cover
    def _sys_exception():
        exc = sys.exc_info()[1]
        exc.__traceback__ = None
        return exc


def _done_cb(new_future, future, on_success=None, on_failure=None):
    if not future.done():
        new_future.cancel()
        return
    if future.cancelled():
        new_future.cancel()
        return
    try:
        result = future.result()
        if on_success:
            result = on_success(result)
        if isinstance(result, Future):
            _chain_future(new_future, result)
        else:
            new_future.set_result(result)
    except BaseException:
        exception = _sys_exception()
        if not on_failure:
            new_future.set_exception(exception)
        else:
            try:
                result = on_failure(exception)
                if isinstance(result, BaseException):
                    new_future.set_exception(result)
                else:
                    new_future.set_result(result)
            except BaseException:
                exception = _sys_exception()
                new_future.set_exception(exception)
