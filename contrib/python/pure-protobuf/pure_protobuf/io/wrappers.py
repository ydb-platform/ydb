from collections.abc import Iterable, Iterator
from io import BytesIO
from typing import IO, Generic, Optional, cast

from pure_protobuf.exceptions import UnexpectedWireTypeError
from pure_protobuf.interfaces._repr import ReprWithInner
from pure_protobuf.interfaces._vars import FieldT_contra, RecordT
from pure_protobuf.interfaces.read import Read, ReadTyped
from pure_protobuf.interfaces.write import Write
from pure_protobuf.io.bytes_ import read_bytes, write_bytes
from pure_protobuf.io.tag import Tag
from pure_protobuf.io.wire_type import WireType


class ReadStrictlyTyped(ReadTyped[RecordT], ReprWithInner):
    """Verifies the actual wire type."""

    __slots__ = ("inner", "expected_wire_type")

    # noinspection PyProtocol
    def __init__(self, inner: Read[RecordT], expected_wire_type: WireType) -> None:
        self.inner = inner
        self.expected_wire_type = expected_wire_type

    def __call__(self, io: IO[bytes], actual_wire_type: WireType) -> Iterator[RecordT]:
        if actual_wire_type != self.expected_wire_type:
            raise UnexpectedWireTypeError(
                f"expected {self.expected_wire_type!r} but received {actual_wire_type!r}",
            )
        yield from self.inner(io)


class ReadMaybePacked(ReadTyped[RecordT], ReprWithInner):
    """Can be called for either packed or unpacked record."""

    __slots__ = ("inner", "inner_packed", "unpacked_wire_type")

    # noinspection PyProtocol
    def __init__(self, inner: Read[RecordT], unpacked_wire_type: WireType) -> None:
        self.inner = inner
        self.unpacked_wire_type = unpacked_wire_type
        self.inner_packed = ReadLengthDelimited[RecordT](ReadRepeated[RecordT](inner))

    def __call__(self, io: IO[bytes], actual_wire_type: WireType) -> Iterator[RecordT]:
        if actual_wire_type == self.unpacked_wire_type:
            yield from self.inner(io)
        elif actual_wire_type == WireType.LEN:
            yield from self.inner_packed(io)
        else:
            raise UnexpectedWireTypeError(
                f"expected {self.unpacked_wire_type!r} or a packed record but received {actual_wire_type!r}",
            )


class ReadLengthDelimited(Read[RecordT], ReprWithInner):
    """
    Make the inner reader length-delimited.

    This reader wraps the inner reader so that it would first read a byte string,
    and then call the inner reader on that byte string.
    """

    __slots__ = ("inner",)

    # noinspection PyProtocol
    def __init__(self, inner: Read[RecordT]) -> None:
        self.inner = inner

    def __call__(self, io: IO[bytes]) -> Iterator[RecordT]:
        yield from self.inner(BytesIO(read_bytes(io)))


class ReadRepeated(Read[RecordT], ReprWithInner):
    """Wrap an inner reader to read repeated records."""

    __slots__ = ("inner",)

    # noinspection PyProtocol
    def __init__(self, inner: Read[RecordT]) -> None:
        self.inner = inner

    def __call__(self, io: IO[bytes]) -> Iterator[RecordT]:
        while True:
            try:
                yield from self.inner(io)
            except EOFError:
                break


class WriteRepeated(Generic[RecordT, FieldT_contra], Write[FieldT_contra], ReprWithInner):
    """Wrap an inner writer to produce repeated records."""

    __slots__ = ("inner",)

    inner: Write[RecordT]

    # noinspection PyProtocol
    def __init__(self, inner: Write[RecordT]) -> None:
        self.inner = inner

    def __call__(self, values: FieldT_contra, io: IO[bytes]) -> None:
        for value in cast(Iterable[RecordT], values):
            self.inner(value, io)


def to_bytes(write: Write[RecordT], value: RecordT) -> bytes:
    io = BytesIO()
    write(value, io)
    return io.getvalue()


class WriteTagged(Write[RecordT], ReprWithInner):
    """Prepends the tag to a value being written."""

    __slots__ = ("inner", "encoded_tag")

    # noinspection PyProtocol
    def __init__(self, inner: Write[RecordT], tag: Tag) -> None:
        self.inner = inner
        self.encoded_tag = to_bytes(Tag.write_to, tag)

    def __call__(self, value: RecordT, io: IO[bytes]) -> None:
        io.write(self.encoded_tag)
        self.inner(value, io)


class WriteOptional(Write[RecordT], ReprWithInner):
    """Wrap an inner writer to skip serialization of `None`."""

    __slots__ = ("inner",)

    inner: Write[RecordT]

    # noinspection PyProtocol
    def __init__(self, inner: Write[RecordT]) -> None:
        self.inner = inner

    def __call__(self, value: Optional[RecordT], io: IO[bytes]) -> None:
        if value is not None:
            self.inner(value, io)


class WriteLengthDelimited(Write[RecordT], ReprWithInner):
    """Wrap an inner writer into a length-delimited record."""

    __slots__ = ("inner",)

    inner: Write[RecordT]

    # noinspection PyProtocol
    def __init__(self, inner: Write[RecordT]) -> None:
        self.inner = inner

    def __call__(self, value: RecordT, io: IO[bytes]) -> None:
        write_bytes(to_bytes(self.inner, value), io)
