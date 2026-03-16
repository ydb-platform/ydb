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

import abc

#: all the log-levels that txaio recognizes
log_levels = [
    "none",
    "critical",
    "error",
    "warn",
    "info",
    "debug",
    "trace",
]


class IBatchedTimer(abc.ABC):
    """
    Objects created by :met:`txaio.make_batched_timer` implement this
    interface.

    These APIs allow you to put call_later()'s into "buckets",
    reducing the number of actual underlying delayed calls that the
    event-loop (asyncio or Twisted) needs to deal with. Obviously, you
    lose some amount of precision in when the timers fire in exchange
    for less memory use, and fewer objects on the queues for the
    underlying event-loop/reactor.

    As a concrete example, in Autobahn we're using this to batch
    together timers for the "auto ping" feature. In this case, it is
    not vital when precisely the timers fire, but as the
    connection-count increases the number of outstanding timers
    becomes quite large.

    It is intended to be used like so:

    class Something(object):
        timers = txaio.make_batched_timer()

        def a_method(self):
            self.timers.call_later()  # drop-in API from txaio.call_later
    """

    def call_later(self, delay, func, *args, **kw):
        """
        This speaks the same API as :meth:`txaio.call_later` and also
        returns an object that has a ``.cancel`` method.

        You cannot rely on any other methods/attributes of the
        returned object. The timeout will actually fire at an
        aribtrary time "close" to the delay specified, depening upon
        the arguments this IBatchedTimer was created with.
        """


class ILogger(abc.ABC):
    """
    This defines the methods you can call on the object returned from
    :meth:`txaio.make_logger` -- although the actual object may have
    additional methods, you should *only* call the methods listed
    here.

    All the log methods have the same signature, they just differ in
    what "log level" they represent to the handlers/emitters. The
    ``message`` argument is a format string using `PEP3101
    <https://www.python.org/dev/peps/pep-3101/>`_-style references to
    things from the ``kwargs``. Note that there are also the following
    keys added to the ``kwargs``: ``log_time`` and ``log_level``.

    For example::

        class MyThing(object):
            log = txaio.make_logger()

            def something_interesting(self, things=dict(one=1, two=2)):
                try:
                    self.log.debug("Called with {things[one]}", things=things)
                    result = self._method_call()
                    self.log.info("Got '{result}'.", result=result)
                except Exception:
                    fail = txaio.create_failure()
                    self.log.critical(txaio.failure_format_traceback(fail))

    The philsophy behind txaio's interface is fairly similar to
    Twisted's logging APIs after version 15. See `Twisted's
    documentation
    <http://twistedmatrix.com/documents/current/core/howto/logger.html>`_
    for details.
    """

    # stdlib notes:
    # levels:
    #   CRITICAL 50
    #   ERROR 40
    #   WARNING 30
    #   INFO 20
    #   DEBUG 10
    #   NOTSET 0

    # NOTES
    # things in Twisted's event:
    # - log_level
    # - log_failure (sometimes?)
    # - log_format (can be None)
    # - log_source (sometimes? no, always, but sometimes None)
    # - log_namespace
    #
    # .warn not warning!

    def critical(self, message, **kwargs):
        "log a critical-level message"

    def error(self, message, **kwargs):
        "log a error-level message"

    def warn(self, message, **kwargs):
        "log a error-level message"

    def info(self, message, **kwargs):
        "log an info-level message"

    def debug(self, message, **kwargs):
        "log an debug-level message"

    def trace(self, message, **kwargs):
        "log a trace-level message"


class IFailedFuture(abc.ABC):
    """
    This defines the interface for a common object encapsulating a
    failure from either an asyncio task/coroutine or a Twisted
    Deferred.

    An instance implementing this interface is given to any
    ``errback`` callables you provide via :meth:`txaio.add_callbacks`

    In your errback you can extract information from an IFailedFuture
    with :meth:`txaio.failure_message` and
    :meth:`txaio.failure_traceback` or use ``.value`` to get the
    Exception instance.

    Depending on other details or methods will probably cause
    incompatibilities between asyncio and Twisted.
    """

    @abc.abstractproperty
    def value(self):
        """
        An actual Exception instance. Same as the second item returned from
        ``sys.exc_info()``
        """
