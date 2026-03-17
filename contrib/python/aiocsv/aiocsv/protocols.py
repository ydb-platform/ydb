# © Copyright 2020-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import sys
from typing import TYPE_CHECKING, Any, Literal, Optional, Protocol, Type, TypedDict, Union

from typing_extensions import NotRequired

if TYPE_CHECKING:
    import csv

if sys.version_info < (3, 12):
    _QuotingType = Literal[0, 1, 2, 3]
else:
    _QuotingType = Literal[0, 1, 2, 3, 4, 5]


class WithAsyncWrite(Protocol):
    async def write(self, __b: str) -> Any: ...


class WithAsyncRead(Protocol):
    async def read(self, __size: int) -> str: ...


class DialectLike(Protocol):
    delimiter: str
    quotechar: Optional[str]
    escapechar: Optional[str]
    doublequote: bool
    skipinitialspace: bool
    quoting: _QuotingType
    strict: bool


CsvDialectArg = Union[str, "csv.Dialect", Type["csv.Dialect"]]


class CsvDialectKwargs(TypedDict):
    delimiter: NotRequired[str]
    quotechar: NotRequired[Optional[str]]
    escapechar: NotRequired[Optional[str]]
    doublequote: NotRequired[bool]
    skipinitialspace: NotRequired[bool]
    lineterminator: NotRequired[str]
    quoting: NotRequired[_QuotingType]
    strict: NotRequired[bool]
