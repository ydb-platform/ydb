from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, NamedTuple

from typing_extensions import LiteralString


class StreamState(ABC):
    """Keeps track of the state of a streamed response."""

    @abstractmethod
    def record_chunk(self, chunk: Any) -> None:
        """Update the state based on a chunk from the streamed response."""

    @abstractmethod
    def get_response_data(self) -> Any:
        """Returns the response data for including in the log."""

    def get_attributes(self, span_data: dict[str, Any]) -> dict[str, Any]:
        """Attributes to include in the log."""
        return dict(**span_data, response_data=self.get_response_data())


class EndpointConfig(NamedTuple):
    """The configuration for the endpoint of a provider based on request url."""

    message_template: LiteralString
    span_data: dict[str, Any]
    stream_state_cls: type[StreamState] | None = None
