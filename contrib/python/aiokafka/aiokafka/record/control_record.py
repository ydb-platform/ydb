import struct
from dataclasses import dataclass

from typing_extensions import Self

_SCHEMA = struct.Struct(">HH")


@dataclass(frozen=True)
class ControlRecord:
    __slots__ = ("type_", "version")

    version: int
    type_: int

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ControlRecord):
            return other.version == self.version and other.type_ == self.type_
        return False

    __hash__ = object.__hash__  # unhashable

    @classmethod
    def parse(cls, data: bytes) -> Self:
        version, type_ = _SCHEMA.unpack_from(data)
        return cls(version, type_)

    def __repr__(self) -> str:
        return f"ControlRecord(version={self.version}, type_={self.type_})"


ABORT_MARKER = ControlRecord(0, 0)
COMMIT_MARKER = ControlRecord(0, 1)
