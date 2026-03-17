import contextlib
from collections import deque
from collections.abc import Generator, Iterable, Sequence
from itertools import islice
from textwrap import dedent
from typing import TypeVar

CB = TypeVar("CB", bound="CodeBuilder")


class CodeBuilder:
    __slots__ = ("_current_indent", "_indent_delta", "_lines")

    def __init__(self, indent_delta: int = 4):
        self._lines: deque[str] = deque()
        self._current_indent = 0
        self._indent_delta = indent_delta

    @property
    def indent_delta(self) -> int:
        return self._indent_delta

    @property
    def lines(self) -> Sequence[str]:
        return self._lines.copy()

    def _extract_lines(self, line_or_text: str) -> Iterable[str]:
        if "\n" in line_or_text:
            return dedent(line_or_text).strip("\n").split("\n")
        return [line_or_text]

    def _add_indented_lines(self, lines: Iterable[str]):
        if self._current_indent == 0:
            self._lines.extend(lines)
            return

        indent = " " * self._current_indent
        self._lines.extend(
            indent + line
            for line in lines
        )

    def __call__(self: CB, line_or_text: str) -> CB:
        """Append lines to builder"""
        lines = self._extract_lines(line_or_text)
        self._add_indented_lines(lines)
        return self

    __iadd__ = __call__

    def include(self: CB, line_or_text: str) -> CB:
        """Add the first line of input text to the last line of builder and append other lines"""
        first_line, *other_lines = self._extract_lines(line_or_text)
        self._include_line(first_line)
        self._add_indented_lines(other_lines)
        return self

    def _include_line(self, line: str) -> None:
        if self._lines:
            self._lines[-1] += line
        else:
            self._lines.append(line)

    __lshift__ = include
    __ilshift__ = include

    def empty_line(self: CB) -> CB:
        self("")
        return self

    @contextlib.contextmanager
    def indent(self, indent_delta: int) -> Generator[None, None, None]:
        self._current_indent += indent_delta
        try:
            yield
        finally:
            self._current_indent -= indent_delta

    def __enter__(self):
        self._current_indent += self._indent_delta

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._current_indent -= self._indent_delta

    def extend(self: CB, other: CB) -> CB:
        self._add_indented_lines(other._lines)
        return self

    def extend_including(self: CB, other: CB) -> CB:
        if not other._lines:
            return self
        self._include_line(other._lines[0])
        self._add_indented_lines(islice(other._lines, 1, None))
        return self

    def extend_above(self: CB, other: CB) -> CB:
        self._lines.extendleft(reversed(other._lines))
        return self

    def string(self) -> str:
        return "\n".join(self._lines)
