"""
Extra elements
"""

from __future__ import annotations

import itertools
import re
from typing import Any, cast

from marko import block, inline
from marko.source import Source


class Paragraph(block.Paragraph):
    _task_list_item_pattern = re.compile(r"(\[[\sxX]\])\s+\S")
    override = True

    def __init__(self, lines):
        super().__init__(lines)
        m = self._task_list_item_pattern.match(self.inline_body)
        if m:
            self.checked = m.group(1)[1:-1].lower() == "x"
            self.inline_body = self.inline_body[m.end(1) :]


class Strikethrough(inline.InlineElement):
    pattern = re.compile(r"(?<!~)(~|~~)([^~]+)\1(?!~)")
    priority = 5
    parse_children = True
    parse_group = 2


class _MatchObj:
    def __init__(self, match, start_shift=0, end_shift=0):
        self._match = match
        self._start_shift = start_shift
        self._end_shift = end_shift

    def start(self, n=0):
        start = self._match.start() + self._start_shift
        if n == 0:
            return start
        return max(start, self._match.start(n))

    def end(self, n=0):
        end = self._match.end() + self._end_shift
        if n == 0:
            return end
        return min(end, self._match.end(n))

    def group(self, n=0):
        start = max(self.start(n) - self._match.start(n), 0) or None
        end = min(self.end(n) - self._match.end(n), 0) or None
        return self._match.group(n)[start:end]

    def __getattr__(self, name):
        return getattr(self._match, name)


class Url(inline.AutoLink):
    www_pattern = re.compile(
        r"(?:^|(?<=[\s*_~(\uff00-\uffef]))(www\.([\w.\-]*?\.[\w.\-]+)[^<\s]*)"
    )
    email_pattern = r"[\w.\-+]+@[\w.\-]*?\.[\w.\-]*[a-zA-Z0-9]"
    bare_pattern = re.compile(
        r"(?:^|(?<=[\s*_~(\uff00-\uffef]))((?:https?|ftp)://([\w.\-]*?\.[\w.\-]+)"
        r"[^<\s]*|%s(?=[\s.<]|\Z))" % email_pattern
    )
    priority = 5

    def __init__(self, match):
        super().__init__(match)
        if self.www_pattern.match(self.dest):
            self.dest = "http://" + self.dest

    @classmethod
    def find(cls, text, *, source):
        for match in itertools.chain(
            cls.www_pattern.finditer(text), cls.bare_pattern.finditer(text)
        ):
            domain = match.group(2)
            if domain:
                parts = domain.split(".")
                if len(parts) < 2 or any("_" in p for p in parts[-2:]):
                    continue
            link_text = match.group()
            if link_text[-1] in ("?", "!", ".", ",", ":", "*", "_", "~"):
                match = _MatchObj(match, end_shift=-1)
            elif link_text[-1] == ")" and link_text.count(")") > link_text.count("("):
                shift = link_text.count(")") - link_text.count("(")
                match = _MatchObj(match, end_shift=-shift)
            else:
                m = re.search(r"&[a-zA-Z]+;$", link_text)
                if m:
                    match = _MatchObj(match, end_shift=-len(m.group()))
            yield match


class Table(block.BlockElement):
    """A table element."""

    _prefix = ""

    def __init__(self, children: list[TableRow], delimiters: list[str]) -> None:
        self.children = children
        self.delimiters = delimiters

    @property
    def head(self) -> TableRow:
        return cast(TableRow, self.children[0])

    @property
    def num_of_cols(self) -> int:
        return len(self.head.children)

    @classmethod
    def match(cls, source):
        source.anchor()
        if not TableRow.match(source) or source.context.is_delimiter:
            return False
        if TableRow.splitter.search(source.next_line()) is None:
            return False
        # consume the first row, we don't use source.consume() here
        # because that may unexpectedly update the line prefix.
        source.pos = source.match.end()
        head = TableRow([TableCell(cell) for cell in source.context.cells])
        if (
            not TableRow.match(source)
            or not source.context.is_delimiter
            or len(source.context.cells) != len(head.children)
        ):
            source.reset()  # invalid table, revert the source position
            return False
        source.context.table_info = {
            "children": [head],
            "delimiters": source.context.cells,
        }
        source.consume()  # consume the second row
        return True

    @classmethod
    def parse(cls, source):
        rv = cls(**source.context.table_info)
        with source.under_state(rv):
            for d, th in zip(rv.delimiters, rv.head.children):
                stripped_d = d.strip()
                th.header = True
                if stripped_d[0] == ":" and stripped_d[-1] == ":":
                    th.align = "center"
                elif stripped_d[0] == ":":
                    th.align = "left"
                elif stripped_d[-1] == ":":
                    th.align = "right"
            while not source.exhausted:
                for e in source.parser._build_block_element_list():
                    if issubclass(e, (Table, block.Paragraph)):
                        continue
                    if e.match(source):
                        break
                else:
                    if TableRow.match(source):
                        rv.children.append(TableRow.parse(source))
                        continue
                break
        return rv


class TableRow(block.BlockElement):
    """A table row element."""

    splitter = re.compile(r"\s*(?<!\\)\|\s*")
    delimiter = re.compile(r":?-+:?")
    virtual = True

    def __init__(self, cells: list[TableCell]) -> None:
        self.children = cells

    @classmethod
    def match(cls, source: Source) -> Any:
        line = source.next_line()
        if not line or not re.match(r" {,3}\S", line):
            return False
        parts = cls.splitter.split(line.strip())
        if parts and not parts[0]:
            parts.pop(0)
        if parts and not parts[-1]:
            parts.pop()
        if len(parts) < 1:
            return False
        source.context.cells = parts
        source.context.is_delimiter = all(cls.delimiter.match(cell) for cell in parts)
        return True

    @classmethod
    def parse(cls, source: Source) -> TableRow:
        source.consume()
        parent = cast(Table, source.state)
        cells: list[str] = source.context.cells[:]
        if len(cells) < parent.num_of_cols:
            cells.extend("" for _ in range(parent.num_of_cols - len(cells)))
        elif len(cells) > parent.num_of_cols:
            cells = cells[: parent.num_of_cols]
        cell_elements = [TableCell(cell) for cell in cells]
        for head, cell in zip(parent.head.children, cell_elements):
            cell.align = cast(TableCell, head).align
        return cls(cell_elements)


class TableCell(block.BlockElement):
    """A table cell element."""

    virtual = True

    def __init__(self, text: str) -> None:
        self.inline_body = text.strip().replace("\\|", "|")
        self.header = False
        self.align: str | None = None


class Alert(block.Quote):
    """Alert block element: block quote with a header like WARNING, NOTE, TIP, IMPORTANT, or CAUTION."""

    priority = block.Quote.priority + 1

    @classmethod
    def match(cls, source):
        return source.expect_re(
            r"(?im) {,3}>\s*\[\!(WARNING|NOTE|TIP|IMPORTANT|CAUTION)\]\s*$"
        )

    @classmethod
    def parse(cls, source):
        alert_type = source.match.group(1).upper()
        source.next_line(require_prefix=False)
        source.consume()
        state = cls(alert_type)
        with source.under_state(state):
            state.children = source.parser.parse_source(source)
        return state

    def __init__(self, alert_type):
        self.alert_type = alert_type
