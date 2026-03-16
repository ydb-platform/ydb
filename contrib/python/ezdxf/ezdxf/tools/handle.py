# Copyright (c) 2011-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from ezdxf.lldxf.types import is_valid_handle

if TYPE_CHECKING:
    from ezdxf.document import Drawing

START_HANDLE = "1"


class HandleGenerator:
    def __init__(self, start_value: str = START_HANDLE):
        self._handle: int = max(1, int(start_value, 16))

    reset = __init__

    def __str__(self):
        return "%X" % self._handle

    def next(self) -> str:
        next_handle = self.__str__()
        self._handle += 1
        return next_handle

    __next__ = next

    def copy(self) -> HandleGenerator:
        return HandleGenerator(str(self))


class UnderlayKeyGenerator(HandleGenerator):
    def __str__(self):
        return "Underlay%05d" % self._handle


def safe_handle(handle: Optional[str], doc: Optional["Drawing"] = None) -> str:
    if handle is None:
        return "0"
    assert isinstance(handle, str), "invalid type"
    if doc is not None:
        if handle not in doc.entitydb:
            return "0"
        return handle
    if not is_valid_handle(handle):
        return "0"
    return handle.upper()
