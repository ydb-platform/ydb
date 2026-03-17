"""
Reading and writing varints and the derivative types.

See Also:
    - https://developers.google.com/protocol-buffers/docs/encoding#varints

"""

from collections.abc import Iterator
from enum import IntEnum
from itertools import count
from sys import byteorder
from typing import IO, TypeVar

from pure_protobuf.exceptions import IncorrectValueError
from pure_protobuf.helpers.io import read_byte_checked
from pure_protobuf.interfaces._skip import Skip
from pure_protobuf.interfaces.read import Read, ReadSingular
from pure_protobuf.interfaces.write import Write


class SkipVarint(Skip):
    def __call__(self, io: IO[bytes]) -> None:
        while read_byte_checked(io) & 0x80:
            pass


class ReadUnsignedVarint(ReadSingular[int]):
    """Reads unsigned varint from the stream."""

    def __call__(self, io: IO[bytes]) -> int:
        value = 0
        for shift in count(0, 7):
            byte = read_byte_checked(io)
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return value
        raise AssertionError("unreachable code")


class WriteUnsignedVarint(Write[int]):
    """Writes unsigned varint to the stream."""

    def __call__(self, value: int, io: IO[bytes]) -> None:
        while value > 0x7F:
            io.write(bytes((value & 0x7F | 0x80,)))
            value >>= 7
        io.write(bytes((value,)))


skip_varint = SkipVarint()
read_unsigned_varint = ReadUnsignedVarint()
write_unsigned_varint = WriteUnsignedVarint()


class ReadZigZagVarint(ReadSingular[int]):
    """
    Reads a ZigZag-encoded varint.

    See Also:
        - https://stackoverflow.com/a/2211086/359730.
    """

    def __call__(self, io: IO[bytes]) -> int:
        value = read_unsigned_varint(io)
        return (value >> 1) ^ (-(value & 1))


class WriteZigZagVarint(Write[int]):
    """Writes a ZigZag-encoded varint."""

    def __call__(self, value: int, io: IO[bytes]) -> None:
        write_unsigned_varint(abs(value) * 2 - (value < 0), io)


class ReadTwosComplimentVarint(ReadSingular[int]):
    """
    Reads a two's compliment varint.

    See Also:
        - https://protobuf.dev/programming-guides/encoding/#signed-ints
    """

    def __call__(self, io: IO[bytes]) -> int:
        varint = read_unsigned_varint(io)
        return int.from_bytes(
            varint.to_bytes(8, byteorder, signed=False),
            byteorder,
            signed=True,
        )


class WriteTwosComplimentVarint(Write[int]):
    """
    Writes a two's compliment varint.

    See Also:
        - https://protobuf.dev/programming-guides/encoding/#signed-ints
    """

    def __call__(self, value: int, io: IO[bytes]) -> None:
        compliment = int.from_bytes(
            value.to_bytes(8, byteorder, signed=True),
            byteorder,
            signed=False,
        )
        return write_unsigned_varint(compliment, io)


class ReadBool(ReadSingular[bool]):
    def __call__(self, io: IO[bytes]) -> bool:
        return bool(read_unsigned_varint(io))


class WriteBool(Write[bool]):
    def __call__(self, value: bool, io: IO[bytes]) -> None:
        write_unsigned_varint(int(value), io)


read_bool = ReadBool()
write_bool = WriteBool()


EnumT = TypeVar("EnumT", bound=IntEnum)


class ReadEnum(Read[EnumT]):
    __slots__ = ("enum_type",)

    # noinspection PyProtocol
    def __init__(self, enum_type: type[EnumT]) -> None:
        self.enum_type = enum_type

    def __call__(self, io: IO[bytes]) -> Iterator[EnumT]:
        value = read_unsigned_varint(io)
        try:
            yield self.enum_type(value)
        except ValueError as e:
            raise IncorrectValueError(
                f"incorrect value {value} for enum `{self.enum_type!r}`",
            ) from e

    def __repr__(self) -> str:  # noqa: D105
        return f"{type(self).__name__}({self.enum_type.__name__})"


class WriteEnum(Write[EnumT]):
    __slots__ = ()

    def __call__(self, value: EnumT, io: IO[bytes]) -> None:
        # noinspection PyTypeChecker
        write_unsigned_varint(value.value, io)
