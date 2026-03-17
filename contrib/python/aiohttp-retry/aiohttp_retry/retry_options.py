from __future__ import annotations

import abc
import random
from typing import Any, Awaitable, Callable, Iterable
from warnings import warn

from aiohttp import ClientResponse

EvaluateResponseCallbackType = Callable[[ClientResponse], Awaitable[bool]]


class RetryOptionsBase:
    def __init__(
        self,
        attempts: int = 3,  # How many times we should retry
        statuses: Iterable[int] | None = None,  # On which statuses we should retry
        exceptions: Iterable[type[Exception]] | None = None,  # On which exceptions we should retry, by default on all
        methods: Iterable[str] | None = None,  # On which HTTP methods we should retry
        retry_all_server_errors: bool = True,  # If should retry all 500 errors or not
        # a callback that will run on response to decide if retry
        evaluate_response_callback: EvaluateResponseCallbackType | None = None,
    ) -> None:
        self.attempts: int = attempts
        if statuses is None:
            statuses = set()
        self.statuses: Iterable[int] = statuses

        if exceptions is None:
            exceptions = set()
        self.exceptions: Iterable[type[Exception]] = exceptions

        if methods is None:
            methods = {"HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST", "CONNECT", "PATCH"}
        self.methods: Iterable[str] = {method.upper() for method in methods}

        self.retry_all_server_errors = retry_all_server_errors
        self.evaluate_response_callback = evaluate_response_callback

    @abc.abstractmethod
    def get_timeout(self, attempt: int, response: ClientResponse | None = None) -> float:
        raise NotImplementedError


class ExponentialRetry(RetryOptionsBase):
    def __init__(
        self,
        attempts: int = 3,  # How many times we should retry
        start_timeout: float = 0.1,  # Base timeout time, then it exponentially grow
        max_timeout: float = 30.0,  # Max possible timeout between tries
        factor: float = 2.0,  # How much we increase timeout each time
        statuses: set[int] | None = None,  # On which statuses we should retry
        exceptions: set[type[Exception]] | None = None,  # On which exceptions we should retry
        methods: set[str] | None = None,  # On which HTTP methods we should retry
        retry_all_server_errors: bool = True,
        evaluate_response_callback: EvaluateResponseCallbackType | None = None,
    ) -> None:
        super().__init__(
            attempts=attempts,
            statuses=statuses,
            exceptions=exceptions,
            methods=methods,
            retry_all_server_errors=retry_all_server_errors,
            evaluate_response_callback=evaluate_response_callback,
        )

        self._start_timeout: float = start_timeout
        self._max_timeout: float = max_timeout
        self._factor: float = factor

    def get_timeout(
        self,
        attempt: int,
        response: ClientResponse | None = None,  # noqa: ARG002
    ) -> float:
        """Return timeout with exponential backoff."""
        timeout = self._start_timeout * (self._factor**attempt)
        return min(timeout, self._max_timeout)


def RetryOptions(*args: Any, **kwargs: Any) -> ExponentialRetry:  # noqa: N802
    warn("RetryOptions is deprecated, use ExponentialRetry", stacklevel=1)
    return ExponentialRetry(*args, **kwargs)


class RandomRetry(RetryOptionsBase):
    def __init__(
        self,
        attempts: int = 3,  # How many times we should retry
        statuses: Iterable[int] | None = None,  # On which statuses we should retry
        exceptions: Iterable[type[Exception]] | None = None,  # On which exceptions we should retry
        methods: Iterable[str] | None = None,  # On which HTTP methods we should retry
        min_timeout: float = 0.1,  # Minimum possible timeout
        max_timeout: float = 3.0,  # Maximum possible timeout between tries
        random_func: Callable[[], float] = random.random,  # Random number generator
        retry_all_server_errors: bool = True,
        evaluate_response_callback: EvaluateResponseCallbackType | None = None,
    ) -> None:
        super().__init__(
            attempts=attempts,
            statuses=statuses,
            exceptions=exceptions,
            methods=methods,
            retry_all_server_errors=retry_all_server_errors,
            evaluate_response_callback=evaluate_response_callback,
        )

        self.attempts: int = attempts
        self.min_timeout: float = min_timeout
        self.max_timeout: float = max_timeout
        self.random = random_func

    def get_timeout(
        self,
        attempt: int,  # noqa: ARG002
        response: ClientResponse | None = None,  # noqa: ARG002
    ) -> float:
        """Generate random timeouts."""
        return self.min_timeout + self.random() * (self.max_timeout - self.min_timeout)


class ListRetry(RetryOptionsBase):
    def __init__(
        self,
        timeouts: list[float],
        statuses: Iterable[int] | None = None,  # On which statuses we should retry
        exceptions: Iterable[type[Exception]] | None = None,  # On which exceptions we should retry
        methods: Iterable[str] | None = None,  # On which HTTP methods we should retry
        retry_all_server_errors: bool = True,
        evaluate_response_callback: EvaluateResponseCallbackType | None = None,
    ) -> None:
        super().__init__(
            attempts=len(timeouts),
            statuses=statuses,
            exceptions=exceptions,
            methods=methods,
            retry_all_server_errors=retry_all_server_errors,
            evaluate_response_callback=evaluate_response_callback,
        )
        self.timeouts = timeouts

    def get_timeout(
        self,
        attempt: int,
        response: ClientResponse | None = None,  # noqa: ARG002
    ) -> float:
        """Timeouts from a defined list."""
        return self.timeouts[attempt]


class FibonacciRetry(RetryOptionsBase):
    def __init__(
        self,
        attempts: int = 3,
        multiplier: float = 1.0,
        statuses: Iterable[int] | None = None,
        exceptions: Iterable[type[Exception]] | None = None,
        methods: Iterable[str] | None = None,
        max_timeout: float = 3.0,  # Maximum possible timeout between tries
        retry_all_server_errors: bool = True,
        evaluate_response_callback: EvaluateResponseCallbackType | None = None,
    ) -> None:
        super().__init__(
            attempts=attempts,
            statuses=statuses,
            exceptions=exceptions,
            methods=methods,
            retry_all_server_errors=retry_all_server_errors,
            evaluate_response_callback=evaluate_response_callback,
        )

        self.max_timeout = max_timeout
        self.multiplier = multiplier
        self.prev_step = 1.0
        self.current_step = 1.0

    def get_timeout(
        self,
        attempt: int,  # noqa: ARG002
        response: ClientResponse | None = None,  # noqa: ARG002
    ) -> float:
        new_current_step = self.prev_step + self.current_step
        self.prev_step = self.current_step
        self.current_step = new_current_step

        return min(self.multiplier * new_current_step, self.max_timeout)


class JitterRetry(ExponentialRetry):
    """https://github.com/inyutin/aiohttp_retry/issues/44."""

    def __init__(
        self,
        attempts: int = 3,  # How many times we should retry
        start_timeout: float = 0.1,  # Base timeout time, then it exponentially grow
        max_timeout: float = 30.0,  # Max possible timeout between tries
        factor: float = 2.0,  # How much we increase timeout each time
        statuses: set[int] | None = None,  # On which statuses we should retry
        exceptions: set[type[Exception]] | None = None,  # On which exceptions we should retry
        methods: set[str] | None = None,  # On which HTTP methods we should retry
        random_interval_size: float = 2.0,  # size of interval for random component
        retry_all_server_errors: bool = True,
        evaluate_response_callback: EvaluateResponseCallbackType | None = None,
    ) -> None:
        super().__init__(
            attempts=attempts,
            start_timeout=start_timeout,
            max_timeout=max_timeout,
            factor=factor,
            statuses=statuses,
            exceptions=exceptions,
            methods=methods,
            retry_all_server_errors=retry_all_server_errors,
            evaluate_response_callback=evaluate_response_callback,
        )

        self._start_timeout: float = start_timeout
        self._max_timeout: float = max_timeout
        self._factor: float = factor
        self._random_interval_size = random_interval_size

    def get_timeout(
        self,
        attempt: int,
        response: ClientResponse | None = None,  # noqa: ARG002
    ) -> float:
        timeout: float = super().get_timeout(attempt) + random.uniform(0, self._random_interval_size) ** self._factor
        return timeout
