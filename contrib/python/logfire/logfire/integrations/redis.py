from typing import Any, Protocol

from opentelemetry.trace import Span
from redis import Connection


class RequestHook(Protocol):
    """A hook that is called before the request is sent."""

    def __call__(self, span: Span, instance: Connection, *args: Any, **kwargs: Any) -> None:
        """Call the hook.

        Args:
            span: The span that is being created.
            instance: The connection instance.
            *args: The arguments that are passed to the command.
            **kwargs: The keyword arguments that are passed to the command.
        """


class ResponseHook(Protocol):
    """A hook that is called after the response is received."""

    def __call__(self, span: Span, instance: Connection, response: Any) -> None:
        """Call the hook.

        Args:
            span: The span that is being created.
            instance: The connection instance.
            response: The response that is received.
        """
