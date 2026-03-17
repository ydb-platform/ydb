import re
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from contextlib import AbstractContextManager, contextmanager
from re import RegexFlag
from textwrap import dedent


class TextSliceWriter(AbstractContextManager[None]):
    @abstractmethod
    def write(self, text: str, /) -> None:
        ...


class LinesWriter(TextSliceWriter):
    __slots__ = ("_indent", "_slices")

    def __init__(self, start_indent: str = ""):
        self._slices: list[str] = []
        self._indent = start_indent

    def __enter__(self) -> None:
        self._indent += "    "

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._indent = self._indent[:-4]

    def write(self, text: str) -> None:
        new_line_indented = text.replace("\n", f"\n{self._indent}")
        self._slices.append(f"{self._indent}{new_line_indented}")

    def make_string(self) -> str:
        return "\n".join(self._slices)


class OneLineWriter(LinesWriter):
    def make_string(self) -> str:
        return "".join(self._slices)


@contextmanager
def at_one_line(writer: TextSliceWriter):
    sub_writer = OneLineWriter()
    yield sub_writer
    writer.write(sub_writer.make_string())


@contextmanager
def at_multi_line(writer: TextSliceWriter):
    sub_writer = LinesWriter()
    yield sub_writer
    writer.write(sub_writer.make_string())


class Statement(ABC):
    @abstractmethod
    def write_lines(self, writer: TextSliceWriter) -> None:
        ...


class Expression(Statement, ABC):
    pass


class ConcatStatements(Statement):
    def __init__(self, elements: Iterable[Statement]):
        self._elements = elements

    def write_lines(self, writer: TextSliceWriter) -> None:
        for stmt in self._elements:
            stmt.write_lines(writer)


def statements(*elements: Statement) -> ConcatStatements:
    return ConcatStatements(elements)


class RawStatement(Statement):
    def __init__(self, text: str):
        self._text = text

    def write_lines(self, writer: TextSliceWriter) -> None:
        writer.write(self._text)


class RawExpr(RawStatement, Expression):
    pass


class _TemplatedStatement(Statement):
    def __init__(self, template: str, **stmts: Statement):
        self._template = template
        self._name_to_stmt = stmts

    _PLACEHOLDER_REGEX = re.compile(r"<(\w+)>", RegexFlag.MULTILINE)
    _INDENT_REGEX = re.compile(r"^[ \t]*", RegexFlag.MULTILINE)

    def _format_template(self) -> str:
        return self._PLACEHOLDER_REGEX.sub(self._replace_placeholder, dedent(self._template).strip())

    def _replace_placeholder(self, match: re.Match[str]) -> str:
        stmt = self._name_to_stmt[match.group(1)]
        start_idx = match.string.rfind("\n", 0, match.end(0))
        indent_match = self._INDENT_REGEX.search(match.string, 0 if start_idx == -1 else start_idx)
        if indent_match is None:
            raise ValueError

        writer = LinesWriter(indent_match.group(0))
        stmt.write_lines(writer)
        return writer.make_string().lstrip()

    def write_lines(self, writer: TextSliceWriter) -> None:
        writer.write(self._format_template())


class CodeBlock(_TemplatedStatement):
    EMPTY_LINE: Statement = RawStatement("")
    PASS: Statement = RawStatement("pass")


class CodeExpr(_TemplatedStatement, Expression):
    def __init__(self, template: str, **exprs: Expression):
        super().__init__(template, **exprs)


class DictItem(ABC):
    @abstractmethod
    def write_fragment(self, writer: TextSliceWriter) -> None:
        ...


class MappingUnpack(DictItem):
    def __init__(self, expr: Expression):
        self._expr = expr

    def write_fragment(self, writer: TextSliceWriter) -> None:
        writer.write("**")
        with at_multi_line(writer) as sub_writer:
            self._expr.write_lines(sub_writer)
        writer.write(",")


class DictKeyValue(DictItem):
    def __init__(self, key: Expression, value: Expression):
        self._key = key
        self._value = value

    def write_fragment(self, writer: TextSliceWriter) -> None:
        with at_multi_line(writer) as sub_writer:
            self._key.write_lines(sub_writer)
        writer.write(": ")
        with at_multi_line(writer) as sub_writer:
            self._value.write_lines(sub_writer)
        writer.write(",")


class DictLiteral(Expression):
    def __init__(self, items: Sequence[DictItem]):
        self._items = items

    def write_lines(self, writer: TextSliceWriter) -> None:
        if len(self._items) == 0:
            writer.write("{}")
            return

        writer.write("{")
        with writer:
            for item in self._items:
                with at_one_line(writer) as sub_writer:
                    item.write_fragment(sub_writer)
        writer.write("}")


class ListLiteral(Expression):
    def __init__(self, items: Sequence[Expression]):
        self._items = items

    def write_lines(self, writer: TextSliceWriter) -> None:
        writer.write("[")
        with writer:
            for item in self._items:
                with at_one_line(writer) as sub_writer:
                    with at_multi_line(sub_writer) as sub_sub_writer:
                        item.write_lines(sub_sub_writer)
                    sub_writer.write(",")
        writer.write("]")


class StringLiteral(Expression):
    def __init__(self, text: str):
        self._text = text

    def write_lines(self, writer: TextSliceWriter) -> None:
        writer.write(repr(self._text))
