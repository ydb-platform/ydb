# © Copyright 2020-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT
# pyright: reportUnnecessaryComparison=false

import csv
import sys
from enum import IntEnum, auto
from typing import Any, AsyncIterator, Awaitable, Final, Generator, List, Optional, Sequence, Union

from .protocols import DialectLike, WithAsyncRead


class ParserState(IntEnum):
    START_RECORD = auto()
    START_FIELD = auto()
    IN_FIELD = auto()
    ESCAPE = auto()
    IN_QUOTED_FIELD = auto()
    ESCAPE_IN_QUOTED = auto()
    QUOTE_IN_QUOTED = auto()
    EAT_NEWLINE = auto()

    def is_end_of_record(self) -> bool:
        return self is ParserState.START_RECORD or self is ParserState.EAT_NEWLINE

    def is_unexpected_at_eof(self) -> bool:
        return self in (
            ParserState.ESCAPE,
            ParserState.IN_QUOTED_FIELD,
            ParserState.ESCAPE_IN_QUOTED,
        )


class Decision(IntEnum):
    CONTINUE = auto()
    DONE = auto()
    DONE_WITHOUT_CONSUMING = auto()


QUOTE_MINIMAL = csv.QUOTE_MINIMAL
QUOTE_ALL = csv.QUOTE_ALL
QUOTE_NONNUMERIC = csv.QUOTE_NONNUMERIC
QUOTE_NONE = csv.QUOTE_NONE
if sys.version_info >= (3, 12):
    QUOTE_STRINGS = csv.QUOTE_STRINGS
    QUOTE_NOTNULL = csv.QUOTE_NOTNULL
else:
    QUOTE_STRINGS: Final = 4
    QUOTE_NOTNULL: Final = 5


class Parser:
    def __init__(self, reader: WithAsyncRead, dialect: DialectLike) -> None:
        self.dialect = dialect
        self.reader = reader

        self.current_read: Optional[Generator[Any, None, str]] = None
        self.buffer: str = ""
        self.eof: bool = False
        self.line_num: int = 0

        self.state = ParserState.START_RECORD
        self.record_so_far: List[str] = []
        self.field_so_far: List[str] = []
        self.field_limit: int = csv.field_size_limit()
        self.field_was_quoted: bool = False
        self.last_char_was_cr: bool = False

    # AsyncIterator[List[str]] interface

    def __aiter__(self) -> AsyncIterator[List[str]]:
        return self

    def __anext__(self) -> Awaitable[List[str]]:
        return self

    # Awaitable[List[str]] interface

    def __await__(self) -> Generator[Any, None, List[str]]:
        return self  # type: ignore

    # Generator[Any, None, List[str]] interface

    def __iter__(self) -> Generator[Any, None, List[str]]:
        return self  # type: ignore

    def __next__(self) -> Any:
        # Loop until a record has been successfully parsed or EOF has been hit
        record: Optional[List[str]] = None
        while record is None and (self.buffer or not self.eof):
            # No pending read and no data available - initiate one
            if not self.buffer and self.current_read is None:
                self.current_read = self.reader.read(4096).__await__()

            # Await on the pending read
            if self.current_read is not None:
                try:
                    return next(self.current_read)
                except StopIteration as e:
                    assert not self.buffer, "a read was pending even though data was available"
                    self.current_read.close()
                    self.current_read = None
                    self.buffer = e.value
                    self.eof = not e.value

            # Advance parsing
            record = self.try_parse()

        # Generate a row, or stop iteration altogether
        if record is None:
            raise StopAsyncIteration
        else:
            raise StopIteration(record)

    # Straightforward parser interface

    def try_parse(self) -> Optional[List[str]]:
        decision = Decision.CONTINUE

        while decision is Decision.CONTINUE and self.buffer:
            decision = self.process_char(self.get_char_and_increment_line_num())
            if decision is not Decision.DONE_WITHOUT_CONSUMING:
                self.buffer = self.buffer[1:]

        if decision is not Decision.CONTINUE or (self.eof and not self.state.is_end_of_record()):
            if self.dialect.strict and self.state.is_unexpected_at_eof():
                raise csv.Error("unexpected end of data")
            self.add_field_at_eof()
            return self.extract_record()
        else:
            return None

    def process_char(self, c: str) -> Decision:
        if self.state == ParserState.START_RECORD:
            return self.process_char_in_start_record(c)
        elif self.state == ParserState.START_FIELD:
            return self.process_char_in_start_field(c)
        elif self.state == ParserState.ESCAPE:
            return self.process_char_in_escape(c)
        elif self.state == ParserState.IN_FIELD:
            return self.process_char_in_field(c)
        elif self.state == ParserState.IN_QUOTED_FIELD:
            return self.process_char_in_quoted_field(c)
        elif self.state == ParserState.ESCAPE_IN_QUOTED:
            return self.process_char_in_escape_in_quoted(c)
        elif self.state == ParserState.QUOTE_IN_QUOTED:
            return self.process_char_in_quote_in_quoted(c)
        elif self.state == ParserState.EAT_NEWLINE:
            return self.process_char_in_eat_newline(c)
        else:
            raise RuntimeError(f"unhandled parser state: {self.state}")

    def process_char_in_start_record(self, c: str) -> Decision:
        if c == "\r":
            self.state = ParserState.EAT_NEWLINE
            return Decision.CONTINUE
        elif c == "\n":
            self.state = ParserState.START_RECORD
            return Decision.DONE
        else:
            return self.process_char_in_start_field(c)

    def process_char_in_start_field(self, c: str) -> Decision:
        if c == "\r":
            self.save_field()
            self.state = ParserState.EAT_NEWLINE
        elif c == "\n":
            self.save_field()
            self.state = ParserState.START_RECORD
            return Decision.DONE
        elif c == self.dialect.quotechar and self.dialect.quoting != QUOTE_NONE:
            self.field_was_quoted = True
            self.state = ParserState.IN_QUOTED_FIELD
        elif c == self.dialect.escapechar:
            self.state = ParserState.ESCAPE
        # XXX: skipinitialspace handling is done in save_field()
        elif c == self.dialect.delimiter:
            self.save_field()
            self.state = ParserState.START_FIELD
        else:
            self.add_char(c)
            self.state = ParserState.IN_FIELD
        return Decision.CONTINUE

    def process_char_in_escape(self, c: str) -> Decision:
        self.add_char(c)
        self.state = ParserState.IN_FIELD
        return Decision.CONTINUE

    def process_char_in_field(self, c: str) -> Decision:
        if c == "\r":
            self.save_field()
            self.state = ParserState.EAT_NEWLINE
        elif c == "\n":
            self.save_field()
            self.state = ParserState.START_RECORD
            return Decision.DONE
        elif c == self.dialect.escapechar:
            self.state = ParserState.ESCAPE
        elif c == self.dialect.delimiter:
            self.save_field()
            self.state = ParserState.START_FIELD
        else:
            self.add_char(c)
        return Decision.CONTINUE

    def process_char_in_quoted_field(self, c: str) -> Decision:
        if c == self.dialect.escapechar:
            self.state = ParserState.ESCAPE_IN_QUOTED
        elif c == self.dialect.quotechar and self.dialect.quoting != QUOTE_NONE:
            # XXX: Is this check for quoting necessary?
            if self.dialect.doublequote:
                self.state = ParserState.QUOTE_IN_QUOTED
            else:
                self.state = ParserState.IN_FIELD
        else:
            self.add_char(c)
        return Decision.CONTINUE

    def process_char_in_escape_in_quoted(self, c: str) -> Decision:
        self.add_char(c)
        self.state = ParserState.IN_QUOTED_FIELD
        return Decision.CONTINUE

    def process_char_in_quote_in_quoted(self, c: str) -> Decision:
        if c == self.dialect.quotechar and self.dialect.quoting != QUOTE_NONE:
            # XXX: Is this check for quoting necessary?
            self.add_char(c)
            self.state = ParserState.IN_QUOTED_FIELD
        elif c == self.dialect.delimiter:
            self.save_field()
            self.state = ParserState.START_FIELD
        elif c == "\r":
            self.save_field()
            self.state = ParserState.EAT_NEWLINE
        elif c == "\n":
            self.save_field()
            self.state = ParserState.START_RECORD
            return Decision.DONE
        elif not self.dialect.strict:
            self.add_char(c)
            self.state = ParserState.IN_FIELD
        else:
            raise csv.Error(
                f"{self.dialect.delimiter!r} expected after {self.dialect.quotechar!r}",
            )
        return Decision.CONTINUE

    def process_char_in_eat_newline(self, c: str) -> Decision:
        self.state = ParserState.START_RECORD
        return Decision.DONE if c == "\n" else Decision.DONE_WITHOUT_CONSUMING

    def add_char(self, c: str) -> None:
        if len(self.field_so_far) == self.field_limit:
            raise csv.Error(f"field larger than field limit ({self.field_limit})")
        self.field_so_far.append(c)

    def save_field(self) -> None:
        field: Union[str, float, None]
        if self.dialect.skipinitialspace:
            field = "".join(self.field_so_far[self.find_first_non_space(self.field_so_far) :])
        else:
            field = "".join(self.field_so_far)

        # Handle unquoted fields for special quote modes
        if self.dialect.quoting == QUOTE_NONNUMERIC and not self.field_was_quoted:
            field = float(field) if field else ""
        elif self.dialect.quoting == QUOTE_STRINGS and not self.field_was_quoted:
            field = float(field) if field else None
        elif self.dialect.quoting == QUOTE_NOTNULL and not self.field_was_quoted:
            field = field if field else None

        self.field_was_quoted = False
        self.record_so_far.append(field)  # type: ignore
        self.field_so_far.clear()

    def add_field_at_eof(self) -> None:
        # Decide if self.record_so_far needs to be added at an EOF
        if self.state in (ParserState.ESCAPE, ParserState.ESCAPE_IN_QUOTED):
            self.add_char("\n")
        if not self.state.is_end_of_record():
            self.save_field()

    def extract_record(self) -> List[str]:
        r = self.record_so_far.copy()
        self.record_so_far.clear()
        return r

    def get_char_and_increment_line_num(self) -> str:
        c = self.buffer[0]
        if c == "\r":
            self.line_num += 1
            self.last_char_was_cr = True
        elif c == "\n":
            if self.last_char_was_cr:
                self.last_char_was_cr = False
            else:
                self.line_num += 1
        else:
            self.last_char_was_cr = False
        return c

    @staticmethod
    def find_first_non_space(x: Sequence[str]) -> int:
        for i, c in enumerate(x):
            if not c.isspace():
                return i
        return len(x)
