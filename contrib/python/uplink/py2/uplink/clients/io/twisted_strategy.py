# Standard library imports
import sys

# Third-party imports
from twisted.internet import reactor, defer, task

# Local imports
from uplink.clients.io import interfaces


__all__ = ["TwistedStrategy"]


class TwistedStrategy(interfaces.IOStrategy):
    """A non-blocking execution strategy using asyncio."""

    _deferred = None

    @defer.inlineCallbacks
    def invoke(self, func, args, kwargs, callback):
        try:
            response = yield func(*args, **kwargs)
        except Exception as error:
            tb = sys.exc_info()[2]
            response = yield callback.on_failure(type(error), error, tb)
        else:
            response = yield callback.on_success(response)
        defer.returnValue(response)

    @defer.inlineCallbacks
    def sleep(self, duration, callback):
        yield task.deferLater(reactor, duration, lambda: None)
        response = yield callback.on_success()
        defer.returnValue(response)

    @defer.inlineCallbacks
    def finish(self, response):
        yield
        defer.returnValue(response)

    @defer.inlineCallbacks
    def fail(self, exc_type, exc_val, exc_tb):
        yield
        super(TwistedStrategy, self).fail(exc_type, exc_val, exc_tb)

    @defer.inlineCallbacks
    def execute(self, executable):
        response = yield executable.execute()
        defer.returnValue(response)
