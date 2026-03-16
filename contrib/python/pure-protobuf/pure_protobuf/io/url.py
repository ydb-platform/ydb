"""Reading and writing parsed URLs."""

from collections.abc import Iterator
from typing import IO
from urllib.parse import ParseResult, urlparse

from pure_protobuf.interfaces.read import Read
from pure_protobuf.interfaces.write import Write
from pure_protobuf.io.bytes_ import read_string, write_string


class ReadUrl(Read[ParseResult]):
    __slots__ = ()

    def __call__(self, io: IO[bytes]) -> Iterator[ParseResult]:
        yield urlparse(read_string(io))


class WriteUrl(Write[ParseResult]):
    __slots__ = ()

    def __call__(self, value: ParseResult, io: IO[bytes]) -> None:
        write_string(value.geturl(), io)
