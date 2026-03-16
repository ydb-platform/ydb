"""
Inline(span) level elements
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Iterator, Pattern, Sequence

from . import patterns
from .element import Element

if TYPE_CHECKING:
    from .inline_parser import _Match
    from .source import Source

__all__ = (
    "LineBreak",
    "Literal",
    "InlineHTML",
    "CodeSpan",
    "Emphasis",
    "StrongEmphasis",
    "Link",
    "Image",
    "AutoLink",
    "RawText",
)


class InlineElement(Element):
    """Any inline element should inherit this class"""

    #: Use to denote the precedence in parsing.
    priority = 5
    #: element regex pattern.
    pattern: Pattern[str] | str = ""
    #: whether to parse children.
    parse_children = False
    #: which match group to parse.
    parse_group = 1
    #: if True, it won't be included in parsing process but produced by
    #: other elements instead.
    virtual = False
    #: If true, will replace the element which it derives from.
    override = False

    if TYPE_CHECKING:
        children: str | Sequence[Element]

    def __init__(self, match: _Match) -> None:
        """Parses the matched object into an element"""
        if not self.parse_children:
            self.children = match.group(self.parse_group)

    @classmethod
    def find(cls, text: str, *, source: Source) -> Iterator[_Match]:
        """This method should return an iterable containing matches of this element."""
        if isinstance(cls.pattern, str):
            cls.pattern = re.compile(cls.pattern)
        return cls.pattern.finditer(text)


class Literal(InlineElement):
    """Literal escapes need to be parsed at the first."""

    priority = 7
    pattern = re.compile(r'\\([!"#\$%&\'()*+,\-./:;<=>?@\[\\\]^_`{|}~])')

    @classmethod
    def strip_backslash(cls, text: str) -> str:
        return cls.pattern.sub(r"\1", text)  # type: ignore[unio]


class LineBreak(InlineElement):
    """Line breaks:

    Soft: '\n'
    Hard: '  \n'
    """

    priority = 2
    pattern = r"( *|\\)\n(?!\Z)"

    def __init__(self, match: _Match) -> None:
        self.soft = not match.group(1).startswith(("  ", "\\"))
        self.children = "\n"


class InlineHTML(InlineElement):
    priority = 7
    pattern = re.compile(
        r"(<%s(?:%s)* */?>"  # open tag
        r"|</%s *>"  # closing tag
        r"|<!--(?:>|->|[\s\S]*?-->)"  # HTML comment
        r"|<\?[\s\S]*?\?>"  # processing instruction
        r"|<![A-Z]+ +[\s\S]*?>"  # declaration
        r"|<!\[CDATA\[[\s\S]*?\]\]>)"  # CDATA section
        % (patterns.tag_name, patterns.attribute, patterns.tag_name)
    )


class StrongEmphasis(InlineElement):
    """Strong emphasis: **sample text**"""

    virtual = True
    parse_children = True


class Emphasis(InlineElement):
    """Emphasis: *sample text*"""

    virtual = True
    parse_children = True


class Link(InlineElement):
    """Link: [text](/link/destination)"""

    virtual = True
    parse_children = True

    def __init__(self, match: _Match) -> None:
        if match.group(2) and match.group(2)[0] == "<" and match.group(2)[-1] == ">":
            self.dest = match.group(2)[1:-1]
        else:
            self.dest = match.group(2) or ""
        self.dest = Literal.strip_backslash(self.dest)
        self.title = (
            Literal.strip_backslash(match.group(3)[1:-1]) if match.group(3) else None
        )


class Image(InlineElement):
    """Image: ![alt](/src/address)"""

    virtual = True
    parse_children = True

    def __init__(self, match: _Match) -> None:
        if match.group(2) and match.group(2)[0] == "<" and match.group(2)[-1] == ">":
            self.dest = match.group(2)[1:-1]
        else:
            self.dest = match.group(2) or ""
        self.dest = Literal.strip_backslash(self.dest)
        self.title = (
            Literal.strip_backslash(match.group(3)[1:-1]) if match.group(3) else None
        )


class CodeSpan(InlineElement):
    """Inline code span: `code sample`"""

    priority = 7
    pattern = re.compile(r"(?<!`)(`+)(?!`)([\s\S]+?)(?<!`)\1(?!`)")

    def __init__(self, match: _Match) -> None:
        self.children = match.group(2).replace("\n", " ")
        if self.children.strip() and self.children[0] == self.children[-1] == " ":
            self.children = self.children[1:-1]


class AutoLink(InlineElement):
    """Autolinks: <http://example.org>"""

    priority = 7
    pattern = re.compile(rf"<({patterns.uri}|{patterns.email})>")

    def __init__(self, match: _Match) -> None:
        self.dest = match.group(1)
        if re.match(patterns.email, self.dest):
            self.dest = "mailto:" + self.dest
        self.children = [RawText(match.group(1))]
        self.title = ""


class RawText(InlineElement):
    """The raw text is the fallback for all holes that doesn't match any others."""

    virtual = True
    if TYPE_CHECKING:
        children: str

    def __init__(self, match: str, escape: bool = True) -> None:
        self.children = match
        self.escape = escape
