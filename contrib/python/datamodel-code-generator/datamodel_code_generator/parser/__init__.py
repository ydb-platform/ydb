from __future__ import annotations

from collections import UserDict
from enum import Enum
from typing import Callable, TypeVar

TK = TypeVar("TK")
TV = TypeVar("TV")


class LiteralType(Enum):
    All = "all"
    One = "one"


class DefaultPutDict(UserDict[TK, TV]):
    def get_or_put(
        self,
        key: TK,
        default: TV | None = None,
        default_factory: Callable[[TK], TV] | None = None,
    ) -> TV:
        if key in self:
            return self[key]
        if default:  # pragma: no cover
            value = self[key] = default
            return value
        if default_factory:
            value = self[key] = default_factory(key)
            return value
        msg = "Not found default and default_factory"
        raise ValueError(msg)  # pragma: no cover


__all__ = [
    "DefaultPutDict",
    "LiteralType",
]
