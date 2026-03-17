"""
Reading and writing scalar length-delimited values.

See Also:
    - https://developers.google.com/protocol-buffers/docs/encoding#length-types

"""

from io import SEEK_CUR
from typing import IO

from pure_protobuf.helpers.io import read_checked
from pure_protobuf.interfaces._skip import Skip
from pure_protobuf.interfaces.read import ReadSingular
from pure_protobuf.interfaces.write import Write
from pure_protobuf.io.varint import read_unsigned_varint, write_unsigned_varint


class SkipBytes(Skip):
    def __call__(self, io: IO[bytes]) -> None:
        length = read_unsigned_varint(io)
        io.seek(length, SEEK_CUR)


class WriteBytes(Write[bytes]):
    def __call__(self, value: bytes, io: IO[bytes]) -> None:
        write_unsigned_varint(len(value), io)
        io.write(value)


class ReadBytes(ReadSingular[bytes]):
    def __call__(self, io: IO[bytes]) -> bytes:
        length = read_unsigned_varint(io)
        return read_checked(io, length)


skip_bytes = SkipBytes()
read_bytes = ReadBytes()
write_bytes = WriteBytes()


class ReadString(ReadSingular[str]):
    def __call__(self, io: IO[bytes]) -> str:
        return read_bytes(io).decode("utf-8")


class WriteString(Write[str]):
    def __call__(self, value: str, io: IO[bytes]) -> None:
        write_bytes(value.encode("utf-8"), io)


read_string = ReadString()
write_string = WriteString()
