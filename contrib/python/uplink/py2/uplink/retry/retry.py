# Local imports
from uplink import decorators
from uplink.clients.io import RequestTemplate, transitions
from uplink.retry import (
    when as when_mod,
    stop as stop_mod,
    backoff as backoff_mod,
)
from uplink.retry._helpers import ClientExceptionProxy

__all__ = ["retry"]


# noinspection PyPep8Naming
class retry(decorators.MethodAnnotation):
    """
    A decorator that adds retry support to a consumer method or to an
    entire consumer.

    Unless you specify the ``when`` or ``on_exception`` argument, all
    failed requests that raise an exception are retried.

    Unless you specify the ``max_attempts`` or ``stop`` argument, this
    decorator continues retrying until the server returns a response.

    Unless you specify the ``backoff`` argument, this decorator uses
    `capped exponential backoff and jitter <https://amzn.to/2xc2nK2>`_,
    which should benefit performance with remote services under high
    contention.

    .. note::

        Response and error handlers (see :ref:`here <custom response
        handler>`) are invoked after the retry condition breaks or all
        retry attempts are exhausted, whatever comes first. These
        handlers will receive the first response/exception that triggers
        the retry's ``stop`` condition or doesn't match its ``when``
        filter.

        In other words, responses or exceptions that match
        the retry condition (e.g., retry when status code is 5xx) are
        not subject to response or error handlers as long as the request
        doesn't break the retry's stop condition (e.g., stop retrying
        after 5 attempts).

    Args:
        when (optional): A predicate that determines when a retry
            should be attempted.
        max_attempts (int, optional): The number of retries to attempt.
            If not specified, requests are retried continuously until
            a response is rendered.
        on_exception (:class:`Exception`, optional): The exception type
            that should prompt a retry attempt.
        stop (:obj:`callable`, optional): A function that creates
            predicates that decide when to stop retrying a request.
        backoff (:class:`RetryBackoff`, :obj:`callable`, optional): A
            backoff strategy or a function that creates an iterator
            over the ordered sequence of timeouts between retries. If
            not specified, exponential backoff is used.
    """

    _DEFAULT_PREDICATE = when_mod.raises(Exception)

    stop = stop_mod
    backoff = backoff_mod
    when = when_mod

    def __init__(
        self,
        when=None,
        max_attempts=None,
        on_exception=None,
        stop=None,
        backoff=None,
    ):
        if stop is None:
            if max_attempts is not None:
                stop = stop_mod.after_attempt(max_attempts)
            else:
                stop = stop_mod.NEVER

        if on_exception is not None:
            when = when_mod.raises(on_exception) | when

        if when is None:
            when = self._DEFAULT_PREDICATE

        if backoff is None:
            backoff = backoff_mod.jittered()

        if not isinstance(backoff, backoff_mod.RetryBackoff):
            backoff = backoff_mod.from_iterable_factory(backoff)

        self._when = when
        self._backoff = backoff
        self._stop = stop

    BASE_CLIENT_EXCEPTION = ClientExceptionProxy(
        lambda ex: ex.BaseClientException
    )
    CONNECTION_ERROR = ClientExceptionProxy(lambda ex: ex.ConnectionError)
    CONNECTION_TIMEOUT = ClientExceptionProxy(lambda ex: ex.ConnectionTimeout)
    SERVER_TIMEOUT = ClientExceptionProxy(lambda ex: ex.ServerTimeout)
    SSL_ERROR = ClientExceptionProxy(lambda ex: ex.SSLError)

    def modify_request(self, request_builder):
        request_builder.add_request_template(
            _RetryTemplate(
                self._when(request_builder),
                self._backoff,
                self._stop,
            )
        )


class _RetryTemplate(RequestTemplate):
    def __init__(self, condition, backoff, stop):
        self._condition = condition
        self._backoff = backoff
        self._stop = stop
        self._stop_iter = self._stop()

    def _process_timeout(self, timeout):
        next(self._stop_iter)
        if timeout is None or self._stop_iter.send(timeout):
            self._backoff.handle_after_final_retry()
            self._stop_iter = self._stop()
            return None
        return transitions.sleep(timeout)

    def after_response(self, request, response):
        if not self._condition.should_retry_after_response(response):
            return self._process_timeout(None)
        return self._process_timeout(
            self._backoff.get_timeout_after_response(request, response)
        )

    def after_exception(self, request, exc_type, exc_val, exc_tb):
        if not self._condition.should_retry_after_exception(
            exc_type, exc_val, exc_tb
        ):
            return self._process_timeout(None)
        return self._process_timeout(
            self._backoff.get_timeout_after_exception(
                request, exc_type, exc_val, exc_tb
            )
        )
