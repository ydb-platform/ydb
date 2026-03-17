from typing import Any

from kombu.connection import Connection

class Transport: ...

class Channel:
    def __init__(self, connection: Connection, **kwargs: Any) -> None: ...
    def close(self) -> None: ...

StdChannel = Channel
