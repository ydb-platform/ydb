from __future__ import annotations

import sys
import typing
from types import TracebackType
from typing import Iterable, Iterator

from progressbar import base


class TextIOOutputWrapper(base.TextIO):  # pragma: no cover
    def __init__(self, stream: base.TextIO) -> None:
        self.stream = stream

    def close(self) -> None:
        self.stream.close()

    def fileno(self) -> int:
        return self.stream.fileno()

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return self.stream.isatty()

    def read(self, __n: int = -1) -> str:
        return self.stream.read(__n)

    def readable(self) -> bool:
        return self.stream.readable()

    def readline(self, __limit: int = -1) -> str:
        return self.stream.readline(__limit)

    def readlines(self, __hint: int = -1) -> list[str]:
        return self.stream.readlines(__hint)

    def seek(self, __offset: int, __whence: int = 0) -> int:
        return self.stream.seek(__offset, __whence)

    def seekable(self) -> bool:
        return self.stream.seekable()

    def tell(self) -> int:
        return self.stream.tell()

    def truncate(self, __size: int | None = None) -> int:
        return self.stream.truncate(__size)

    def writable(self) -> bool:
        return self.stream.writable()

    def writelines(self, __lines: Iterable[str]) -> None:
        return self.stream.writelines(__lines)

    def __next__(self) -> str:
        return self.stream.__next__()

    def __iter__(self) -> Iterator[str]:
        return self.stream.__iter__()

    def __exit__(
        self,
        __t: type[BaseException] | None,
        __value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> None:
        return self.stream.__exit__(__t, __value, __traceback)

    def __enter__(self) -> base.TextIO:
        return self.stream.__enter__()


class LineOffsetStreamWrapper(TextIOOutputWrapper):
    UP = '\033[F'
    DOWN = '\033[B'

    def __init__(
        self, lines: int = 0, stream: typing.TextIO = sys.stderr
    ) -> None:
        self.lines = lines
        super().__init__(stream)

    def write(self, data: str) -> int:
        data = data.rstrip('\n')
        # Move the cursor up
        self.stream.write(self.UP * self.lines)
        # Print a carriage return to reset the cursor position
        self.stream.write('\r')
        # Print the data without newlines so we don't change the position
        self.stream.write(data)
        # Move the cursor down
        self.stream.write(self.DOWN * self.lines)

        self.flush()
        return len(data)


class LastLineStream(TextIOOutputWrapper):
    line: str = ''

    def seekable(self) -> bool:
        return False

    def readable(self) -> bool:
        return True

    def read(self, __n: int = -1) -> str:
        if __n < 0:
            return self.line
        else:
            return self.line[:__n]

    def readline(self, __limit: int = -1) -> str:
        if __limit < 0:
            return self.line
        else:
            return self.line[:__limit]

    def write(self, data: str) -> int:
        self.line = data
        return len(data)

    def truncate(self, __size: int | None = None) -> int:
        if __size is None:
            self.line = ''
        else:
            self.line = self.line[:__size]

        return len(self.line)

    def __iter__(self) -> typing.Generator[str, typing.Any, typing.Any]:
        yield self.line

    def writelines(self, __lines: Iterable[str]) -> None:
        line = ''
        # Walk through the lines and take the last one
        for line in __lines:  # noqa: B007
            pass

        self.line = line
