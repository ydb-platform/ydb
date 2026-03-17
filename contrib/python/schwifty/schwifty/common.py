from __future__ import annotations

import re
from functools import total_ordering
from typing import Any


try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


_clean_regex = re.compile(r"\s+")


@total_ordering
class Base(str):
    def __new__(cls: type[Self], value: str, **kwargs: Any) -> Self:
        return super().__new__(cls, clean(value))

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}={self!s}>"

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: Any) -> bool:
        return str(self) == str(other)

    def __lt__(self, other: Any) -> bool:
        return str(self) < str(other)

    def __deepcopy__(self, memo: dict[str, Any] | None = None) -> Self:
        return self.__class__(str(self))

    @property
    def compact(self) -> str:
        """str: Compact representation of the code. It's preferable to call ``str(obj)``"""
        return str(self)

    @property
    def length(self) -> int:
        """int: Length of the compact code. It's preferable to call ``len(obj)``"""
        return len(self)

    def _get_slice(self, start: int, end: int | None = None) -> str:
        if start < len(self) and (end is None or end <= len(self)):
            return self.compact[start:end] if end is not None else self.compact[start:]
        return ""


def clean(s: str) -> str:
    return _clean_regex.sub("", s).upper()
