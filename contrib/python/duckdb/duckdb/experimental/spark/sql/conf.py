from typing import Optional, Union  # noqa: D100

from duckdb import DuckDBPyConnection
from duckdb.experimental.spark._globals import _NoValue, _NoValueType


class RuntimeConfig:  # noqa: D101
    def __init__(self, connection: DuckDBPyConnection) -> None:  # noqa: D107
        self._connection = connection

    def set(self, key: str, value: str) -> None:  # noqa: D102
        raise NotImplementedError

    def isModifiable(self, key: str) -> bool:  # noqa: D102
        raise NotImplementedError

    def unset(self, key: str) -> None:  # noqa: D102
        raise NotImplementedError

    def get(self, key: str, default: Union[Optional[str], _NoValueType] = _NoValue) -> str:  # noqa: D102
        raise NotImplementedError


__all__ = ["RuntimeConfig"]
