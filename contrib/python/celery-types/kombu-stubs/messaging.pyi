from collections.abc import Callable, Sequence
from enum import Enum
from typing import Any

from kombu.connection import Connection
from kombu.entity import Exchange, Queue
from kombu.message import Message
from kombu.transport.base import Channel

class Producer:
    def __init__(
        self,
        channel: Connection | Channel,
        exchange: Exchange | str | None = ...,
        routing_key: str | None = ...,
        serializer: str | None = ...,
        auto_declare: bool | None = ...,
        compression: str | None = ...,
        on_return: (
            Callable[[Exception, Exchange | str, str, Message], None] | None
        ) = ...,
    ) -> None: ...
    def publish(
        self,
        body: Any,
        routing_key: str | None = ...,
        delivery_mode: Enum | None = ...,
        mandatory: bool = ...,
        immediate: bool = ...,
        priority: int = ...,
        content_type: str | None = ...,
        content_encoding: str | None = ...,
        serializer: str | None = ...,
        headers: dict[Any, Any] | None = ...,
        compression: str | None = ...,
        exchange: Exchange | str | None = ...,
        retry: bool = ...,
        retry_policy: dict[Any, Any] | None = ...,
        declare: Sequence[Exchange | Queue] = ...,
        expiration: float | None = ...,
        timeout: float | None = ...,
        **properties: Any,
    ) -> None: ...

class Consumer:
    channel: Connection | Channel
    queues: Sequence[Queue]
    accept: Sequence[str] | None
    def __init__(
        self,
        channel: Connection | Channel,
        queues: Sequence[Queue] | None = ...,
        no_ack: bool | None = ...,
        auto_declare: bool | None = ...,
        callbacks: Sequence[Callable[[Any, Message], None]] | None = ...,
        on_decode_error: Callable[[Message, Exception], None] | None = ...,
        on_message: Callable[[Message], None] | None = ...,
        accept: Sequence[str] | None = ...,
        prefetch_count: int | None = ...,
        tag_prefix: str | None = ...,
    ) -> None: ...
