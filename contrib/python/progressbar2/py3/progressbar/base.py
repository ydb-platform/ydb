from __future__ import annotations

import typing
from typing import IO, TextIO


class FalseMeta(type):
    @classmethod
    def __bool__(cls) -> bool:  # pragma: no cover
        return False

    @classmethod
    def __cmp__(cls, other: typing.Any) -> int:  # pragma: no cover
        return -1

    __nonzero__ = __bool__


class UnknownLength(metaclass=FalseMeta):
    pass


class Undefined(metaclass=FalseMeta):
    pass


assert IO is not None
assert TextIO is not None

__all__ = (
    'FalseMeta',
    'UnknownLength',
    'Undefined',
    'IO',
    'TextIO',
)
