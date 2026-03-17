# Local imports
from uplink.retry._helpers import ClientExceptionProxy

__all__ = ["RetryPredicate", "raises", "status", "status_5xx"]


class RetryPredicate(object):
    def should_retry_after_response(self, response):
        return False

    def should_retry_after_exception(self, exc_type, exc_val, exc_tb):
        return False

    def __call__(self, request_builder):  # Pragma: no cover
        return self

    def __or__(self, other):
        if other is not None:
            assert isinstance(
                other, RetryPredicate
            ), "Both objects should be retry conditions."
            return _Or(self, other)
        return self


class _Or(RetryPredicate):
    def __init__(self, left, right):
        self._left = left
        self._right = right

    def should_retry_after_response(self, *args, **kwargs):
        return self._left.should_retry_after_response(
            *args, **kwargs
        ) or self._right.should_retry_after_response(*args, **kwargs)

    def should_retry_after_exception(self, *args, **kwargs):
        return self._left.should_retry_after_exception(
            *args, **kwargs
        ) or self._right.should_retry_after_exception(*args, **kwargs)

    def __call__(self, request_builder):
        left = self._left(request_builder)
        right = self._right(request_builder)
        return type(self)(left, right)


# noinspection PyPep8Naming
class raises(RetryPredicate):
    """Retry when a specific exception type is raised."""

    def __init__(self, expected_exception):
        self._expected_exception = expected_exception

    def __call__(self, request_builder):
        proxy = ClientExceptionProxy.wrap_proxy_if_necessary(
            self._expected_exception
        )
        type_ = proxy(request_builder.client.exceptions)
        return raises(type_)

    def should_retry_after_exception(self, exc_type, exc_val, exc_tb):
        return isinstance(exc_val, self._expected_exception)


# noinspection PyPep8Naming
class status(RetryPredicate):
    """Retry on specific HTTP response status codes."""

    def __init__(self, *status_codes):
        self._status_codes = status_codes

    def should_retry_after_response(self, response):
        return response.status_code in self._status_codes


# noinspection PyPep8Naming
class status_5xx(RetryPredicate):
    """Retry after receiving a 5xx (server error) response."""

    def should_retry_after_response(self, response):
        return 500 <= response.status_code < 600
