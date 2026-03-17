# © Copyright 2020-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

from typing import AsyncIterator, Awaitable, List

from typing_extensions import Self

from .protocols import DialectLike, WithAsyncRead

class Parser:
    """Return type of the "Parser" function, not accessible from Python."""

    def __new__(cls, reader: WithAsyncRead, dialect: DialectLike) -> Self: ...
    def __aiter__(self) -> AsyncIterator[List[str]]: ...
    def __anext__(self) -> Awaitable[List[str]]: ...
    @property
    def line_num(self) -> int: ...
