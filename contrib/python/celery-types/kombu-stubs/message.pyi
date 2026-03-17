from typing import Any

ACK_STATES: set[str]

class Message:
    errors: list[Any]
    body: str | None
    content_encoding: str | None
    headers: dict[str, str]
    properties: dict[str, str]
    delivery_tag: str | None
    delivery_info: dict[Any, Any]
    content_type: str | None
    accept: Any | None
    channel: Any | None
    def __init__(
        self,
        body: str | None = ...,
        delivery_tag: str | None = ...,
        content_type: str | None = ...,
        content_encoding: str | None = ...,
        delivery_info: Any | None = ...,
        properties: Any | None = ...,
        headers: dict[str, str] | None = ...,
        postencode: Any | None = ...,
        accept: Any | None = ...,
        channel: Any | None = ...,
        **kwargs: object,
    ) -> None: ...
    def ack(self, multiple: bool = ...) -> None: ...
    def ack_log_error(self, logger: Any, errors: Any, multiple: bool = ...) -> None: ...
    def reject_log_error(
        self, logger: Any, errors: Any, requeue: bool = ...
    ) -> None: ...
    def reject(self, requeue: bool = ...) -> None: ...
    def requeue(self) -> None: ...
    def decode(self) -> Any: ...
    @property
    def acknowledged(self) -> bool: ...
    @property
    def payload(self) -> Any: ...
