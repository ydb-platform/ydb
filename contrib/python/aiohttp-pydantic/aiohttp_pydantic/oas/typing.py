"""
This module provides type to annotate the content of web.Response returned by
the HTTP handlers.

The type are: r100, r101, ..., r599

Example:

    class PetCollectionView(PydanticView):
        async def get(self) -> Union[r200[List[Pet]], r404]:
            ...
"""

from functools import lru_cache
from types import new_class
from typing import Protocol, TypeVar

RespContents = TypeVar("RespContents", covariant=True)

_status_code = frozenset([f"r{code}" for code in range(100, 600)] + ["default"])


@lru_cache(maxsize=len(_status_code))
def _make_status_code_type(status_code):
    if status_code in _status_code:
        return new_class(status_code, (Protocol[RespContents],))
    return None


def is_status_code_type(obj) -> bool:
    """
    Return True if obj is a status code type such as _200 or _404.
    """
    name = getattr(obj, "__name__", None)
    if name not in _status_code:
        return False

    return obj is _make_status_code_type(name)


def __getattr__(name):
    if (status_code_type := _make_status_code_type(name)) is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return status_code_type


__all__ = list(_status_code)
__all__.append("is_status_code_type")
