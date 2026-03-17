"""
This module provides a class for defining custom handling for specific
points of an HTTP transaction.
"""
# Local imports
from uplink import compat

__all__ = ["TransactionHook", "RequestAuditor", "ResponseHandler"]


def _wrap_if_necessary(hook, requires_consumer):
    if not requires_consumer:
        return _wrap_to_ignore_consumer(hook)
    return hook


def _wrap_to_ignore_consumer(hook):
    @compat.wraps(hook)
    def wrapper(_, *args, **kwargs):
        # Expects that consumer is the first argument
        return hook(*args, **kwargs)

    return wrapper


class TransactionHook(object):
    """
    A utility class providing methods that define hooks for specific
    points of an HTTP transaction.
    """

    def audit_request(self, consumer, request_builder):  # pragma: no cover
        """Inspects details of a request before it is sent."""
        pass

    handle_response = None
    """
    Handles a response object from the server.

    This method can be undefined (i.e., None), indicating that this hook
    does not handle responses.

    Args:
        response: The received HTTP response.
    """

    def handle_exception(
        self, consumer, exc_type, exc_val, exc_tb
    ):  # pragma: no cover
        """
        Handles an exception thrown while waiting for a response from
        the server.

        Args:
            consumer: The consumer that spawned the failing request.
            exc_type: The type of the exception.
            exc_val: The exception instance raised.
            exc_tb: A traceback instance.
        """
        pass


class TransactionHookChain(TransactionHook):
    """
    A chain that conjoins several transaction hooks into a single
    object.

    A method call on this composite object invokes the corresponding
    method on all hooks in the chain.
    """

    def __init__(self, *hooks):
        self._hooks = hooks
        self._response_handlers = []

        # TODO: If more than one callback exists on the chain, the chain
        # expects it can execute each synchronously. Instead, we should
        # be smart about this and produces a chained coroutine when all
        # callbacks are coroutines, so that the client can execute the
        # chain asynchronously. Further, when provided both synchronous
        # and asynchronous callbacks, we should raise an exception when
        # the order is mixed and split into two chains (one async and
        # the other sync) when the order permits separation.

        # Adding a synchronous callback to an async request forces the
        # request to execute synchronously while running this chain. To
        # avoid unnecessarily executing this chain when no callbacks
        # exists, we can set the `handle_response` method to null,
        # indicating that this hook doesn't handle responses.
        response_handlers = [h for h in hooks if h.handle_response is not None]
        if not response_handlers:
            self.handle_response = None
        elif len(response_handlers) == 1:
            self.handle_response = response_handlers[0].handle_response

        self._response_handlers = response_handlers

    def audit_request(self, consumer, request_handler):
        for hook in self._hooks:
            hook.audit_request(consumer, request_handler)

    def handle_response(self, consumer, response):
        for hook in self._response_handlers:
            response = hook.handle_response(consumer, response)
        return response

    def handle_exception(self, consumer, exc_type, exc_val, exc_tb):
        for hook in self._hooks:
            hook.handle_exception(consumer, exc_type, exc_val, exc_tb)
        compat.reraise(exc_type, exc_val, exc_tb)


class RequestAuditor(TransactionHook):
    """
    Transaction hook that inspects requests using a function provided at
    time of instantiation.
    """

    def __init__(self, auditor, requires_consumer=False):
        self.audit_request = _wrap_if_necessary(auditor, requires_consumer)


class ResponseHandler(TransactionHook):
    """
    Transaction hook that handles responses using a function provided at
    time of instantiation.
    """

    def __init__(self, handler, requires_consumer=False):
        self.handle_response = _wrap_if_necessary(handler, requires_consumer)


class ExceptionHandler(TransactionHook):
    """
    Transaction hook that handles an exception thrown while waiting for
    a response, using the provided function.
    """

    def __init__(self, exception_handler, requires_consumer=False):
        self.handle_exception = _wrap_if_necessary(
            exception_handler, requires_consumer
        )
