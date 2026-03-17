"""
Defines predicates for determining when to retry a request.

This module provides classes and functions to control when retry operations should be triggered.
"""

# Local imports
from uplink.retry._helpers import ClientExceptionProxy

__all__ = ["RetryPredicate", "raises", "status", "status_5xx"]


class RetryPredicate:
    """
    Base class for defining retry conditions.

    You can compose two `RetryPredicate` instances by using the `|` operator:

    ```python
    CustomPredicateA() | CustomPredicateB()
    ```

    The resulting predicate will retry if either of the composed predicates
    indicates to retry.
    """

    def should_retry_after_response(self, response):
        """
        Determines whether to retry after receiving a response.

        Args:
            response: The HTTP response.

        Returns:
            bool: True if the request should be retried, False otherwise.
        """
        return False

    def should_retry_after_exception(self, exc_type, exc_val, exc_tb):
        """
        Determines whether to retry after an exception occurs.

        Args:
            exc_type: The exception type.
            exc_val: The exception value.
            exc_tb: The exception traceback.

        Returns:
            bool: True if the request should be retried, False otherwise.
        """
        return False

    def __call__(self, request_builder):  # Pragma: no cover
        """
        Adapts the predicate to the request builder.

        Args:
            request_builder: The request builder.

        Returns:
            RetryPredicate: A retry predicate adapted to the request builder.
        """
        return self

    def __or__(self, other):
        """
        Composes the current predicate with another.

        Args:
            other: Another RetryPredicate instance.

        Returns:
            RetryPredicate: A composite predicate.
        """
        if other is not None:
            assert isinstance(other, RetryPredicate), (
                "Both objects should be retry conditions."
            )
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
    """
    Retry when a specific exception type is raised.

    Args:
        expected_exception: The exception type that should trigger a retry.
    """

    def __init__(self, expected_exception):
        self._expected_exception = expected_exception

    def __call__(self, request_builder):
        proxy = ClientExceptionProxy.wrap_proxy_if_necessary(self._expected_exception)
        type_ = proxy(request_builder.client.exceptions)
        return raises(type_)

    def should_retry_after_exception(self, exc_type, exc_val, exc_tb):
        return isinstance(exc_val, self._expected_exception)


# noinspection PyPep8Naming
class status(RetryPredicate):
    """
    Retry on specific HTTP response status codes.

    Args:
        *status_codes: The status codes that should trigger a retry.
    """

    def __init__(self, *status_codes):
        self._status_codes = status_codes

    def should_retry_after_response(self, response):
        return response.status_code in self._status_codes


# noinspection PyPep8Naming
class status_5xx(RetryPredicate):
    """Retry after receiving a 5xx (server error) response."""

    def should_retry_after_response(self, response):
        return 500 <= response.status_code < 600
