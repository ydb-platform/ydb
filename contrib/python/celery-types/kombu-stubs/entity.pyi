from typing import Any

from kombu.abstract import MaybeChannelBound
from kombu.transport.base import Channel

class Exchange(MaybeChannelBound):
    def __init__(
        self,
        name: str = ...,
        type: str = ...,
        channel: Channel | None = ...,
        **kwargs: Any,
    ) -> None: ...

class Queue(MaybeChannelBound):
    routing_key: str
    exchange: Exchange
    name: str
    def __init__(
        self,
        name: str = ...,
        exchange: Exchange | str | None = ...,
        routing_key: str = ...,
        channel: Channel | None = ...,
        bindings: Any = ...,
        on_declared: Any = ...,
        **kwargs: Any,
    ) -> None: ...
