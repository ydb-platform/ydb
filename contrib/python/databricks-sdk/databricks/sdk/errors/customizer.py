import abc
import logging

import requests


class _ErrorCustomizer(abc.ABC):
    """A customizer for errors from the Databricks REST API."""

    @abc.abstractmethod
    def customize_error(self, response: requests.Response, kwargs: dict):
        """Customize the error constructor parameters."""


class _RetryAfterCustomizer(_ErrorCustomizer):
    """An error customizer that sets the retry_after_secs parameter based on the Retry-After header."""

    _DEFAULT_RETRY_AFTER_SECONDS = 1
    """The default number of seconds to wait before retrying a request if the Retry-After header is missing or is not
    a valid integer."""

    @classmethod
    def _parse_retry_after(cls, response: requests.Response) -> int:
        retry_after = response.headers.get("Retry-After")
        if retry_after is None:
            logging.debug(
                f"No Retry-After header received in response with status code 429 or 503. Defaulting to {cls._DEFAULT_RETRY_AFTER_SECONDS}"
            )
            # 429 requests should include a `Retry-After` header, but if it's missing,
            # we default to 1 second.
            return cls._DEFAULT_RETRY_AFTER_SECONDS
        # If the request is throttled, try parse the `Retry-After` header and sleep
        # for the specified number of seconds. Note that this header can contain either
        # an integer or a RFC1123 datetime string.
        # See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
        #
        # For simplicity, we only try to parse it as an integer, as this is what Databricks
        # platform returns. Otherwise, we fall back and don't sleep.
        try:
            return int(retry_after)
        except ValueError:
            logging.debug(
                f"Invalid Retry-After header received: {retry_after}. Defaulting to {cls._DEFAULT_RETRY_AFTER_SECONDS}"
            )
            # defaulting to 1 sleep second to make self._is_retryable() simpler
            return cls._DEFAULT_RETRY_AFTER_SECONDS

    def customize_error(self, response: requests.Response, kwargs: dict):
        if response.status_code in (429, 503):
            kwargs["retry_after_secs"] = self._parse_retry_after(response)
