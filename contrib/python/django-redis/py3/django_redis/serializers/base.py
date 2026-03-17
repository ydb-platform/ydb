from typing import Any


class BaseSerializer:
    def __init__(self, options):
        pass

    def dumps(self, value: Any) -> bytes:
        raise NotImplementedError

    def loads(self, value: bytes) -> Any:
        raise NotImplementedError
