"""utilities for working with labels"""
from typing import Any, Callable, Iterable, Optional, Dict

from starlette.requests import Request


class ResponseHeaderLabel:
    """ResponseHeaderLabel is a special class that allows populating a label
    value based on response headers. starlette_exporter will recognize that this
    should not be called until response headers are written."""

    def __init__(
        self, key: str, allowed_values: Optional[Iterable] = None, default: str = ""
    ) -> None:
        self.key = key.lower()
        self.default = default
        self.allowed_values = allowed_values

    def __call__(self, headers: Dict) -> Any:
        v = headers.get(self.key, self.default)
        if self.allowed_values is not None and v not in self.allowed_values:
            return self.default
        return v


def from_header(key: str, allowed_values: Optional[Iterable] = None) -> Callable:
    """returns a function that retrieves a header value from a request.
    The returned function can be passed to the `labels` argument of PrometheusMiddleware
    to label metrics using a header value.

    `key`: header key
    `allowed_values`: an iterable (e.g. list or tuple) containing an allowlist of values. Any
    header value not in allowed_values will result in an empty string being returned.  Use
    this to constrain the potential label values.

    example:

    ```
        PrometheusMiddleware(
            labels={
                "host": from_header("X-User", allowed_values=("frank", "estelle"))
            }
        )
    ```
    """

    def inner(r: Request):
        v = r.headers.get(key, "")

        # if allowed_values was supplied, return a blank string if
        # the value of the header does match any of the values.
        if allowed_values is not None and v not in allowed_values:
            return ""

        return v

    return inner


def from_response_header(
    key: str, allowed_values: Optional[Iterable] = None, default: str = ""
):
    """returns a callable class that retrieves a header value from response headers.
    starlette_exporter will automatically populate this label value when response headers
    are written."""
    return ResponseHeaderLabel(key, allowed_values, default)
