from collections.abc import Iterator
from struct import Struct
from typing import IO, Generic

from pure_protobuf.helpers.io import read_checked
from pure_protobuf.interfaces._repr import ReprWithInner
from pure_protobuf.interfaces._vars import RecordT_co, RecordT_contra
from pure_protobuf.interfaces.read import Read
from pure_protobuf.interfaces.write import Write


class ReadStruct(Read[RecordT_co], ReprWithInner, Generic[RecordT_co]):
    inner: Struct

    __slots__ = ("inner",)

    # noinspection PyProtocol
    def __init__(self, format_: str) -> None:  # noqa: D107
        self.inner = Struct(format_)

    def __call__(self, io: IO[bytes]) -> Iterator[RecordT_co]:
        inner = self.inner
        yield from inner.unpack(read_checked(io, inner.size))


class WriteStruct(Write[RecordT_contra], ReprWithInner, Generic[RecordT_contra]):
    inner: Struct

    __slots__ = ("inner",)

    # noinspection PyProtocol
    def __init__(self, format_: str) -> None:  # noqa: D107
        self.inner = Struct(format_)

    def __call__(self, value: RecordT_contra, io: IO[bytes]) -> None:
        io.write(self.inner.pack(value))
