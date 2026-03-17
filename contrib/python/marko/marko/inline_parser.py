"""
Parse inline elements
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Match, NamedTuple, Union

from . import patterns
from .helpers import find_next, is_paired, normalize_label
from .inline import InlineElement

if TYPE_CHECKING:
    from .source import Source

    _Match = Union[Match[str], "MatchObj"]

    ElementType = type[InlineElement]


class Group(NamedTuple):
    start: int
    end: int
    text: str | None


_EMPTY_GROUP = Group(-1, -1, None)
WHITESPACE = " \n\t"
ASCII_CONTROL = "".join(chr(i) for i in range(0, 32)) + chr(127)


class ParseError(ValueError):
    """Raised when parsing fails."""


def parse(
    text: str, elements: list[ElementType], fallback: ElementType, source: Source
) -> list[InlineElement]:
    """Parse given text and produce a list of inline elements.

    :param text: the text to be parsed.
    :param elements: the element types to be included in parsing
    :param fallback: fallback class when no other element type is matched.
    """

    class LinkOrEmph(InlineElement):
        parse_children = True

        def __new__(cls, match: _Match) -> InlineElement:  # type: ignore
            assert isinstance(match, MatchObj)
            return source.parser.inline_elements[match.etype](match)

    # A raw list of elements that may contain overlaps.
    tokens: list[Token] = []
    for m in find_links_or_emphs(text, source.root.link_ref_defs):
        tokens.append(Token(LinkOrEmph, m, text, fallback))

    for etype in elements:
        for match in etype.find(text, source=source):
            tokens.append(Token(etype, match, text, fallback))
    tokens.sort()
    tokens = _resolve_overlap(tokens)
    return make_elements(tokens, text, fallback=fallback)


def _resolve_overlap(tokens: list[Token]) -> list[Token]:
    if not tokens:
        return tokens
    result = []
    prev = tokens[0]
    for cur in tokens[1:]:
        r = prev.relation(cur)
        if r == Token.PRECEDE:
            result.append(prev)
            prev = cur
        elif r == Token.CONTAIN:
            prev.append_child(cur)
        elif r == Token.INTERSECT and prev.etype.priority < cur.etype.priority:
            prev = cur
    result.append(prev)
    return result


def make_elements(
    tokens: list[Token],
    text: str,
    start: int = 0,
    end: int | None = None,
    fallback: ElementType | None = None,
) -> list[InlineElement]:
    """Make elements from a list of parsed tokens.
    It will turn all unmatched holes into fallback elements.

    :param tokens: a list of parsed tokens.
    :param text: the original tet.
    :param start: the offset of where parsing starts. Defaults to the start of text.
    :param end: the offset of where parsing ends. Defauls to the end of text.
    :param fallback: fallback element type.
    :returns: a list of inline elements.
    """
    result: list[InlineElement] = []
    end = end or len(text)
    prev_end = start
    for token in tokens:
        if prev_end < token.start:
            result.append(fallback(text[prev_end : token.start]))  # type: ignore
        result.append(token.as_element())
        prev_end = token.end
    if prev_end < end:
        result.append(fallback(text[prev_end:end]))  # type: ignore
    return result


class Token:
    """An intermediate class to wrap the match object.
    It can be converted to element by :meth:`as_element()`
    """

    PRECEDE = 0
    INTERSECT = 1
    CONTAIN = 2
    SHADE = 3

    def __init__(
        self, etype: ElementType, match: _Match, text: str, fallback: ElementType
    ) -> None:
        self.etype = etype
        self.match = match
        self.start = match.start()
        self.end = match.end()
        self.inner_start = match.start(etype.parse_group)
        self.inner_end = match.end(etype.parse_group)
        self.text = text
        self.fallback = fallback
        self.children: list[Token] = []

    def relation(self, other: Token) -> int:
        if self.end <= other.start:
            return Token.PRECEDE
        if self.end >= other.end:
            if (
                self.etype.parse_children
                and other.start >= self.inner_start
                and other.end <= self.inner_end
            ):
                return Token.CONTAIN
            if self.etype.parse_children and self.inner_end <= other.start:
                return Token.SHADE
        return Token.INTERSECT

    def append_child(self, child: Token) -> None:
        if not self.etype.parse_children:
            return
        self.children.append(child)

    def as_element(self) -> InlineElement:
        e = self.etype(self.match)
        if e.parse_children:
            self.children = _resolve_overlap(self.children)
            e.children = make_elements(
                self.children,
                self.text,
                self.inner_start,
                self.inner_end,
                self.fallback,
            )
        return e

    def __repr__(self) -> str:
        return "<{}: {} start={} end={}>".format(
            self.__class__.__name__, self.etype.__name__, self.start, self.end
        )

    def __lt__(self, o: Token) -> bool:
        return self.start < o.start


def find_links_or_emphs(
    text: str, link_ref_defs: dict[str, tuple[str, str]]
) -> list[MatchObj]:
    """Fink links/images or emphasis from text.

    :param text: the original text.
    :param link_ref_defs: a mapping of link ref definitions.
    :returns: an iterable of match object.
    """
    delimiters_re = re.compile(r"(?:!?\[|\*+|_+)")
    i = 0
    delimiters: list[Delimiter] = []
    escape = False
    matches: list[MatchObj] = []
    code_pattern = re.compile(r"(?<!`)(`+)(?!`)([\s\S]+?)(?<!`)\1(?!`)")

    while i < len(text):
        if escape:
            escape = False
            i += 1
        elif text[i] == "\\":
            escape = True
            i += 1
        elif code_pattern.match(text, i):
            i = code_pattern.match(text, i).end()  # type: ignore
        elif text[i] == "]":
            node = look_for_image_or_link(text, delimiters, i, link_ref_defs, matches)
            if node:
                i = node.end()
                matches.append(node)
            else:
                i += 1
        else:
            m = delimiters_re.match(text, i)
            if m:
                delimiters.append(Delimiter(m, text))
                i = m.end()
            else:
                i += 1
    process_emphasis(text, delimiters, None, matches)
    return matches


def look_for_image_or_link(
    text: str,
    delimiters: list[Delimiter],
    close: int,
    link_ref_defs: dict[str, tuple[str, str]],
    matches: list[MatchObj],
) -> MatchObj | None:
    for i, d in list(enumerate(delimiters))[::-1]:
        if d.content not in ("[", "!["):
            continue
        if not d.active:
            break  # break to remove the delimiter and return None
        if not _is_legal_link_text(text[d.end : close]):
            break
        link_text = Group(d.end, close, text[d.end : close])
        assert link_text.text is not None
        etype = "Image" if d.content == "![" else "Link"
        match = _expect_inline_link(text, close + 1) or _expect_reference_link(
            text, close + 1, link_text.text, link_ref_defs
        )
        if not match:  # not a link
            break
        rv = MatchObj(etype, text, d.start, match[2], link_text, match[0], match[1])
        process_emphasis(text, delimiters, i, matches)
        if etype == "Link":
            for d in delimiters[:i]:
                if d.content == "[":
                    d.active = False
        del delimiters[i]
        return rv

    else:
        # no matching opener is found
        return None

    del delimiters[i]
    return None


def _is_legal_link_text(text: str) -> bool:
    return is_paired(text, "[", "]")


def _parse_link_separator(text: str, start: int) -> int:
    i = start
    has_newline = False
    while i < len(text):
        if text[i] == "\n":
            if has_newline:
                break
            has_newline = True
        elif text[i] not in WHITESPACE:
            break
        i += 1
    return i


def _parse_link_label(text: str, start: int) -> Group | None:
    if text[start : start + 1] != "[":
        return None
    i = find_next(text, "]", start + 1, disallowed="[")
    if i < 0:
        return None
    label = text[start + 1 : i]
    if not label.strip() or len(label) > 999:
        return None
    return Group(start, i + 1, text[start : i + 1])


def _parse_link_dest_title(
    link_text: str, start: int = 0, is_inline: bool = False
) -> tuple[Group, Group]:
    if start >= len(link_text):
        raise ParseError()
    if link_text[start] == "<":
        right_bracket = find_next(link_text, ">", start + 1, disallowed="<\n")
        if right_bracket < 0:
            raise ParseError()
        i = right_bracket + 1
        link_dest = Group(start, i, link_text[start:i])
    else:
        escaped = False
        pairs = 0
        i = start
        while i < len(link_text):
            c = link_text[i]
            if escaped:
                escaped = False
            elif c == "\\":
                escaped = True
            elif c in WHITESPACE:
                break
            elif c in ASCII_CONTROL:
                raise ParseError("Invalid character in link destination")
            elif c == "(":
                pairs += 1
            elif c == ")":
                if pairs > 0:
                    pairs -= 1
                elif is_inline:
                    link_dest = Group(start, i, link_text[start:i])
                    return link_dest, _EMPTY_GROUP
                else:
                    raise ParseError("unmatched parenthesis")
            i += 1
        else:
            if is_inline:
                raise ParseError("No right parenthesis is found")
        link_dest = Group(start, i, link_text[start:i])
        if not link_dest.text:
            raise ParseError("Empty link destination")
    prev = i
    i = _parse_link_separator(link_text, i)
    if i >= len(link_text) or link_text[i] == "\n" or link_text[i] == ")" and is_inline:
        return link_dest, _EMPTY_GROUP
    if link_text[i] == '"':
        end = find_next(link_text, '"', i + 1)
    elif link_text[i] == "'":
        end = find_next(link_text, "'", i + 1)
    elif link_text[i] == "(":
        end = find_next(link_text, ")", i + 1, disallowed="(")
    elif "\n" in link_text[prev:i]:
        return link_dest, _EMPTY_GROUP
    else:
        raise ParseError()
    if 0 < i < len(link_text) and link_text[i - 1] not in WHITESPACE:
        raise ParseError()
    if end < 0:
        raise ParseError()
    if "\n\n" in link_text[i:end]:
        raise ParseError()
    link_title = Group(i, end + 1, link_text[i : end + 1])
    return link_dest, link_title


def _expect_inline_link(text: str, start: int) -> tuple[Group, Group, int] | None:
    """(link_dest "link_title")"""
    if start >= len(text) - 1 or text[start] != "(":
        return None
    i = _parse_link_separator(text, start + 1)

    try:
        link_dest, link_title = _parse_link_dest_title(text, i, is_inline=True)
    except ParseError:
        return None
    end = max(link_dest.end, link_title.end)
    end = _parse_link_separator(text, end)
    if end >= len(text) or text[end] != ")":
        return None
    return link_dest, link_title, end + 1


def _expect_reference_link(
    text: str, start: int, link_text: str, link_ref_defs: dict[str, tuple[str, str]]
) -> tuple[Group, Group, int] | None:
    link_label = _parse_link_label(text, start)
    label = link_text
    if link_label is not None:
        assert link_label.text is not None
        label = link_label.text[1:-1] or link_text
    elif text[start : start + 2] == "[]":
        link_label = Group(start, start + 2, "[]")
    result = _get_reference_link(label, link_ref_defs)
    if not result:
        return None
    link_dest = Group(start, start, result[0])
    link_title = Group(start, start, result[1])
    return (link_dest, link_title, link_label.end if link_label else start)


def _get_reference_link(
    link_label: str, link_ref_defs: dict[str, tuple[str, str]]
) -> tuple[str, str] | None:
    normalized_label = normalize_label(link_label)
    return link_ref_defs.get(normalized_label)


def process_emphasis(
    text: str,
    delimiters: list[Delimiter],
    stack_bottom: int | None,
    matches: list[MatchObj],
) -> None:
    star_bottom = underscore_bottom = stack_bottom
    cur = _next_closer(delimiters, stack_bottom)
    while cur is not None:
        d_closer = delimiters[cur]
        bottom = star_bottom if d_closer.content[0] == "*" else underscore_bottom
        opener = _nearest_opener(delimiters, cur, bottom)
        if opener is not None:
            d_opener = delimiters[opener]
            n = 2 if len(d_opener.content) >= 2 and len(d_closer.content) >= 2 else 1
            match = MatchObj(
                "StrongEmphasis" if n == 2 else "Emphasis",
                text,
                d_opener.end - n,
                d_closer.start + n,
                Group(
                    d_opener.end, d_closer.start, text[d_opener.end : d_closer.start]
                ),
            )
            matches.append(match)
            del delimiters[opener + 1 : cur]
            cur -= cur - opener - 1
            if d_opener.remove(n):
                delimiters.remove(d_opener)
                cur -= 1
            if d_closer.remove(n, True):
                delimiters.remove(d_closer)
            cur = cur - 1 if cur > 0 else None
        else:
            bottom = cur - 1 if cur > 1 else None
            if d_closer.content[0] == "*":
                star_bottom = bottom
            else:
                underscore_bottom = bottom
            if not d_closer.can_open:
                delimiters.remove(d_closer)
                cur = cur - 1 if cur > 0 else None
        cur = _next_closer(delimiters, cur)
    lower = stack_bottom + 1 if stack_bottom is not None else 0
    del delimiters[lower:]


def _next_closer(delimiters: list[Delimiter], bound: int | None) -> int | None:
    i = bound + 1 if bound is not None else 0
    while i < len(delimiters):
        d = delimiters[i]
        if getattr(d, "can_close", False):
            return i
        i += 1
    return None


def _nearest_opener(
    delimiters: list[Delimiter], higher: int, lower: int | None
) -> int | None:
    i = higher - 1
    lower = lower if lower is not None else -1
    while i > lower:
        d = delimiters[i]
        if getattr(d, "can_open", False) and d.closed_by(delimiters[higher]):
            return i
        i -= 1
    return None


class Delimiter:
    whitespace_re = re.compile(r"\s", flags=re.UNICODE)

    def __init__(self, match: _Match, text: str) -> None:
        self.start = match.start()
        self.end = match.end()
        self.content = match.group()
        self.text = text
        self.active = True
        if self.content[0] in ("*", "_"):
            self.can_open = self._can_open()
            self.can_close = self._can_close()

    def _can_open(self) -> bool:
        if self.content[0] == "*":
            return self.is_left_flanking()
        return self.is_left_flanking() and (
            not self.is_right_flanking() or self.preceded_by_punc()
        )

    def _can_close(self) -> bool:
        if self.content[0] == "*":
            return self.is_right_flanking()
        return self.is_right_flanking() and (
            not self.is_left_flanking() or self.followed_by_punc()
        )

    def is_left_flanking(self) -> bool:
        return (
            self.end < len(self.text)
            and self.whitespace_re.match(self.text, self.end) is None
        ) and (
            not self.followed_by_punc()
            or self.start == 0
            or self.preceded_by_punc()
            or self.whitespace_re.match(self.text, self.start - 1) is not None
        )

    def is_right_flanking(self) -> bool:
        return (
            self.start > 0
            and self.whitespace_re.match(self.text, self.start - 1) is None
        ) and (
            not self.preceded_by_punc()
            or self.end == len(self.text)
            or self.followed_by_punc()
            or self.whitespace_re.match(self.text, self.end) is not None
        )

    def followed_by_punc(self) -> bool:
        return self.end < len(self.text) and patterns.is_punctuation(
            self.text[self.end]
        )

    def preceded_by_punc(self) -> bool:
        return self.start > 0 and patterns.is_punctuation(self.text[self.start - 1])

    def closed_by(self, other: Delimiter) -> bool:
        return not (
            self.content[0] != other.content[0]
            or (self.can_open and self.can_close or other.can_open and other.can_close)
            and len(self.content + other.content) % 3 == 0
            and not all(len(d.content) % 3 == 0 for d in [self, other])
        )

    def remove(self, n: int, left: bool = False) -> bool:
        if len(self.content) <= n:
            return True
        if left:
            self.start += n
        else:
            self.end -= n
        self.content = self.content[n:]
        return False

    def __repr__(self) -> str:
        return "<Delimiter {!r} start={} end={}>".format(
            self.content, self.start, self.end
        )


class MatchObj:
    """A fake match object that memes re.match methods"""

    def __init__(
        self, etype: str, text: str, start: int, end: int, *groups: Group
    ) -> None:
        self._text = text
        self._start = start
        self._end = end
        self._groups = groups
        self.etype = etype

    def group(self, n: int = 0) -> str:
        if n == 0:
            return self._text[self._start : self._end]
        return self._groups[n - 1][2]  # type: ignore

    def start(self, n: int = 0) -> int:
        if n == 0:
            return self._start
        return self._groups[n - 1][0]

    def end(self, n: int = 0) -> int:
        if n == 0:
            return self._end
        return self._groups[n - 1][1]

    def span(self, n: int = 0) -> tuple[int, int]:
        return (self.start(n), self.end(n))
