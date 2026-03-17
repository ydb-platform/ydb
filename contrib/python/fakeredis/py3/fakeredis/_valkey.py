import sys
from typing import Any, Dict

from . import FakeStrictRedis
from ._connection import FakeRedis
from .aioredis import FakeRedis as FakeAsyncRedis

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


def _validate_server_type(args_dict: Dict[str, Any]) -> None:
    if "server_type" in args_dict and args_dict["server_type"] != "valkey":
        raise ValueError("server_type must be valkey")
    args_dict.setdefault("server_type", "valkey")


class FakeValkey(FakeRedis):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _validate_server_type(kwargs)
        super().__init__(*args, **kwargs)

    @classmethod
    def from_url(cls, *args: Any, **kwargs: Any) -> Self:
        _validate_server_type(kwargs)
        return super().from_url(*args, **kwargs)


class FakeStrictValkey(FakeStrictRedis):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _validate_server_type(kwargs)
        super().__init__(*args, **kwargs)

    @classmethod
    def from_url(cls, *args: Any, **kwargs: Any) -> Self:
        _validate_server_type(kwargs)
        return super().from_url(*args, **kwargs)


class FakeAsyncValkey(FakeAsyncRedis):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _validate_server_type(kwargs)
        super().__init__(*args, **kwargs)

    @classmethod
    def from_url(cls, *args: Any, **kwargs: Any) -> Self:
        _validate_server_type(kwargs)
        return super().from_url(*args, **kwargs)
