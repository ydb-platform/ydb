import re
import sys
from typing import Optional, cast, TYPE_CHECKING, Iterable, Union, Dict, Callable
import weakref
from weakref import ReferenceType

from debian._deb822_repro._util import BufferingIterator
from debian._deb822_repro.locatable import (
    Locatable,
    START_POSITION,
    Range,
    ONE_CHAR_RANGE,
    ONE_LINE_RANGE,
    Position,
)
from debian._util import resolve_ref, _strI


if TYPE_CHECKING:
    from debian._deb822_repro.parsing import Deb822Element


# Consume whitespace and a single word.
_RE_WHITESPACE_SEPARATED_WORD_LIST = re.compile(
    r"""
    (?P<space_before>\s*)                # Consume any whitespace before the word
                                         # The space only occurs in practise if the line starts
                                         # with space.

                                         # Optionally consume a word (needed to handle the case
                                         # when there are no words left and someone applies this
                                         # pattern to the remaining text). This is mostly here as
                                         # a fail-safe.

    (?P<word>\S+)                        # Consume the word (if present)
    (?P<trailing_whitespace>\s*)         # Consume trailing whitespace
""",
    re.VERBOSE,
)
_RE_COMMA_SEPARATED_WORD_LIST = re.compile(
    r"""
    # This regex is slightly complicated by the fact that it should work with
    # finditer and consume the entire value.
    #
    # To do this, we structure the regex so it always starts on a comma (except
    # for the first iteration, where we permit the absence of a comma)

    (?:                                      # Optional space followed by a mandatory comma unless
                                             # it is the start of the "line" (in which case, we
                                             # allow the comma to be omitted)
        ^
        |
        (?:
            (?P<space_before_comma>\s*)      # This space only occurs in practise if the line
                                             # starts with space + comma.
            (?P<comma> ,)
        )
    )

    # From here it is "optional space, maybe a word and then optional space" again.  One reason why
    # all of it is optional is to gracefully cope with trailing commas.
    (?P<space_before_word>\s*)
    (?P<word> [^,\s] (?: [^,]*[^,\s])? )?    # "Words" can contain spaces for comma-separated list.
                                             # But surrounding whitespace is ignored
    (?P<space_after_word>\s*)
""",
    re.VERBOSE,
)

# From Policy 5.1:
#
#    The field name is composed of US-ASCII characters excluding control
#    characters, space, and colon (i.e., characters in the ranges U+0021
#    (!) through U+0039 (9), and U+003B (;) through U+007E (~),
#    inclusive). Field names must not begin with the comment character
#    (U+0023 #), nor with the hyphen character (U+002D -).
#
# That combines to this regex of questionable readability
_RE_FIELD_LINE = re.compile(
    r"""
    ^                                          # Start of line
    (?P<field_name>                            # Capture group for the field name
        [\x21\x22\x24-\x2C\x2F-\x39\x3B-\x7F]  # First character
        [\x21-\x39\x3B-\x7F]*                  # Subsequent characters (if any)
    )
    (?P<separator> : )
    (?P<space_before_value> \s* )
    (?:                                        # Field values are not mandatory on the same line
                                               # as the field name.

      (?P<value>  \S(?:.*\S)?  )               # Values must start and end on a "non-space"
      (?P<space_after_value> \s* )             # We can have optional space after the value
    )?
""",
    re.VERBOSE,
)


class Deb822Token(Locatable):
    """A token is an atomic syntactical element from a deb822 file

    A file is parsed into a series of tokens.  If these tokens are converted to
    text in exactly the same order, you get exactly the same file - bit-for-bit.
    Accordingly ever bit of text in a file must be assigned to exactly one
    Deb822Token.
    """

    __slots__ = ("_text", "_parent_element", "_token_size", "__weakref__")

    def __init__(self, text):
        # type: (str) -> None
        if text == "":  # pragma: no cover
            raise ValueError("Tokens must have content")
        self._text = text  # type: str
        self._parent_element = None  # type: Optional[ReferenceType['Deb822Element']]
        self._token_size = None  # type: Optional[Range]
        self._verify_token_text()

    def __repr__(self) -> str:
        return "{clsname}('{text}')".format(
            clsname=self.__class__.__name__, text=self._text.replace("\n", "\\n")
        )

    def _verify_token_text(self) -> None:
        if "\n" in self._text:
            is_single_line_token = False
            if self.is_comment or self.is_error:
                is_single_line_token = True
            if not is_single_line_token and not self.is_whitespace:
                raise ValueError(
                    "Only whitespace, error and comment tokens may contain newlines"
                )
            if not self.text.endswith("\n"):
                raise ValueError("Tokens containing whitespace must end on a newline")
            if is_single_line_token and "\n" in self.text[:-1]:
                raise ValueError(
                    "Comments and error tokens must not contain embedded newlines"
                    " (only end on one)"
                )

    @property
    def is_whitespace(self) -> bool:
        return False

    @property
    def is_comment(self) -> bool:
        return False

    @property
    def is_error(self) -> bool:
        return False

    @property
    def is_separator(self) -> bool:
        return False

    @property
    def text(self) -> str:
        return self._text

    # To support callers that want a simple interface for converting tokens and elements to text
    def convert_to_text(self) -> str:
        return self._text

    def size(self) -> Range:
        # As tokens are an atomic unit
        token_size = self._token_size
        if token_size is not None:
            return token_size
        token_len = len(self._text)
        if token_len == 1:
            # The indirection with `r` because mypy gets confused and thinks that `token_size`
            # cannot have any type at all.
            token_size = ONE_CHAR_RANGE if self._text != "\n" else ONE_LINE_RANGE
        else:
            new_lines = self._text.count("\n")
            assert not new_lines or self._text[-1] == "\n"
            end_pos = Position(new_lines, 0) if new_lines else Position(0, token_len)
            token_size = Range(START_POSITION, end_pos)
        self._token_size = token_size
        return token_size

    @property
    def parent_element(self):
        # type: () -> Optional[Deb822Element]
        return resolve_ref(self._parent_element)

    @parent_element.setter
    def parent_element(self, new_parent):
        # type: (Optional[Deb822Element]) -> None
        self._parent_element = (
            weakref.ref(new_parent) if new_parent is not None else None
        )

    def clear_parent_if_parent(self, parent):
        # type: (Deb822Element) -> None
        if parent is self.parent_element:
            self._parent_element = None


class Deb822WhitespaceToken(Deb822Token):
    """The token is a kind of whitespace.

    Some whitespace tokens are critical for the format (such as the Deb822ValueContinuationToken,
    spaces that separate words in list separated by spaces or newlines), while other whitespace
    tokens are truly insignificant (space before a newline, space after a comma in a comma
    list, etc.).
    """

    __slots__ = ()

    @property
    def is_whitespace(self) -> bool:
        return True


class Deb822SemanticallySignificantWhiteSpace(Deb822WhitespaceToken):
    """Whitespace that (if removed) would change the meaning of the file (or cause syntax errors)"""

    __slots__ = ()


class Deb822NewlineAfterValueToken(Deb822SemanticallySignificantWhiteSpace):
    """The newline after a value token.

    If not followed by a continuation token, this also marks the end of the field.
    """

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__("\n")


class Deb822ValueContinuationToken(Deb822SemanticallySignificantWhiteSpace):
    """The whitespace denoting a value spanning an additional line (the first space on a line)"""

    __slots__ = ()


class Deb822SpaceSeparatorToken(Deb822SemanticallySignificantWhiteSpace):
    """Whitespace between values in a space list (e.g. "Architectures")"""

    __slots__ = ()

    @property
    def is_separator(self) -> bool:
        return True


class Deb822ErrorToken(Deb822Token):
    """Token that represents a syntactical error"""

    __slots__ = ()

    @property
    def is_error(self) -> bool:
        return True


class Deb822CommentToken(Deb822Token):

    __slots__ = ()

    @property
    def is_comment(self) -> bool:
        return True


class Deb822FieldNameToken(Deb822Token):

    __slots__ = ()

    def __init__(self, text):
        # type: (str) -> None
        if not isinstance(text, _strI):
            text = _strI(sys.intern(text))
        super().__init__(text)

    @property
    def text(self):
        # type: () -> _strI
        return cast("_strI", self._text)


# The colon after the field name, parenthesis, etc.
class Deb822SeparatorToken(Deb822Token):

    __slots__ = ()

    @property
    def is_separator(self) -> bool:
        return True


class Deb822FieldSeparatorToken(Deb822SeparatorToken):

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(":")


class Deb822CommaToken(Deb822SeparatorToken):
    """Used by the comma-separated list value parsers to denote a comma between two value tokens."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(",")


class Deb822PipeToken(Deb822SeparatorToken):
    """Used in some dependency fields as OR relation"""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__("|")


class Deb822ValueToken(Deb822Token):
    """A field value can be split into multi "Deb822ValueToken"s (as well as separator tokens)"""

    __slots__ = ()


class Deb822ValueDependencyToken(Deb822Token):
    """Package name, architecture name, a version number, or a profile name in a dependency field"""

    __slots__ = ()


class Deb822ValueDependencyVersionRelationOperatorToken(Deb822Token):

    __slots__ = ()


def tokenize_deb822_file(sequence, encoding="utf-8"):
    # type: (Iterable[Union[str, bytes]], str) -> Iterable[Deb822Token]
    """Tokenize a deb822 file

    :param sequence: An iterable of lines (a file open for reading will do)
    :param encoding: The encoding to use (this is here to support Deb822-like
       APIs, new code should not use this parameter).
    """
    current_field_name = None
    field_name_cache = {}  # type: Dict[str, _strI]

    def _normalize_input(s):
        # type: (Iterable[Union[str, bytes]]) -> Iterable[str]
        for x in s:
            if isinstance(x, bytes):
                x = x.decode(encoding)
            if not x.endswith("\n"):
                # We always end on a newline because it makes a lot of code simpler.  The pain
                # points relates to mutations that add content after the last field.  Sadly, these
                # mutations can happen via adding fields, reordering fields, etc. and are too hard
                # to track to make it worth it to support the special case that makes up missing
                # a newline at the end of the file.
                x += "\n"
            yield x

    text_stream = BufferingIterator(
        _normalize_input(sequence)
    )  # type: BufferingIterator[str]

    for line in text_stream:
        if line.isspace():
            if current_field_name:
                # Blank lines terminate fields
                current_field_name = None

            # If there are multiple whitespace-only lines, we combine them
            # into one token.
            r = list(text_stream.takewhile(str.isspace))
            if r:
                line += "".join(r)

            # whitespace tokens are likely to have duplicate cases (like
            # single newline tokens), so we intern the strings there.
            yield Deb822WhitespaceToken(sys.intern(line))
            continue

        if line[0] == "#":
            yield Deb822CommentToken(line)
            continue

        if line[0] in (" ", "\t"):
            if current_field_name is not None:
                # We emit a separate whitespace token for the newline as it makes some
                # things easier later (see _build_value_line)
                leading = sys.intern(line[0])
                # Pull out the leading space and newline
                line = line[1:-1]
                yield Deb822ValueContinuationToken(leading)
                yield Deb822ValueToken(line)
                yield Deb822NewlineAfterValueToken()
            else:
                yield Deb822ErrorToken(line)
            continue

        field_line_match = _RE_FIELD_LINE.match(line)
        if field_line_match:
            # The line is a field, which means there is a bit to unpack
            # - note that by definition, leading and trailing whitespace is insignificant
            #   on the value part directly after the field separator
            (field_name, _, space_before, value, space_after) = (
                field_line_match.groups()
            )

            current_field_name = field_name_cache.get(field_name)

            if value is None or value == "":
                # If there is no value, then merge the two space elements into space_after
                # as it makes it easier to handle the newline.
                space_after = (
                    space_before + space_after if space_after else space_before
                )
                space_before = ""

            if space_after:
                # We emit a separate whitespace token for the newline as it makes some
                # things easier later (see _build_value_line)
                if space_after.endswith("\n"):
                    space_after = space_after[:-1]

            if current_field_name is None:
                field_name = sys.intern(field_name)
                current_field_name = _strI(field_name)
                field_name_cache[field_name] = current_field_name

            # We use current_field_name from here as it is a _strI.
            # Delete field_name to avoid accidentally using it and getting bugs
            # that should not happen.
            del field_name

            yield Deb822FieldNameToken(current_field_name)
            yield Deb822FieldSeparatorToken()
            if space_before:
                yield Deb822WhitespaceToken(sys.intern(space_before))
            if value:
                yield Deb822ValueToken(value)
            if space_after:
                yield Deb822WhitespaceToken(sys.intern(space_after))
            yield Deb822NewlineAfterValueToken()
        else:
            yield Deb822ErrorToken(line)


def _value_line_tokenizer(func):
    # type: (Callable[[str], Iterable[Deb822Token]]) -> (Callable[[str], Iterable[Deb822Token]])
    def impl(v):
        # type: (str) -> Iterable[Deb822Token]
        first_line = True
        for no, line in enumerate(v.splitlines(keepends=True)):
            assert not v.isspace() or no == 0
            if line.startswith("#"):
                yield Deb822CommentToken(line)
                continue
            has_newline = False
            continuation_line_marker = None
            if not first_line:
                continuation_line_marker = line[0]
                line = line[1:]
            first_line = False
            if line.endswith("\n"):
                has_newline = True
                line = line[:-1]
            if continuation_line_marker is not None:
                yield Deb822ValueContinuationToken(sys.intern(continuation_line_marker))
            yield from func(line)
            if has_newline:
                yield Deb822NewlineAfterValueToken()

    return impl


@_value_line_tokenizer
def whitespace_split_tokenizer(v):
    # type: (str) -> Iterable[Deb822Token]
    assert "\n" not in v
    if not v or v.isspace():
        # Special-case: Empty field/whitespace only field
        if v:
            yield Deb822SpaceSeparatorToken(sys.intern(v))
        return
    for match in _RE_WHITESPACE_SEPARATED_WORD_LIST.finditer(v):
        space_before, word, space_after = match.groups()
        if space_before:
            yield Deb822SpaceSeparatorToken(sys.intern(space_before))
        yield Deb822ValueToken(word)
        if space_after:
            yield Deb822SpaceSeparatorToken(sys.intern(space_after))


@_value_line_tokenizer
def comma_split_tokenizer(v):
    # type: (str) -> Iterable[Deb822Token]
    assert "\n" not in v
    for match in _RE_COMMA_SEPARATED_WORD_LIST.finditer(v):
        space_before_comma, comma, space_before_word, word, space_after_word = (
            match.groups()
        )
        if space_before_comma:
            yield Deb822WhitespaceToken(sys.intern(space_before_comma))
        if comma:
            yield Deb822CommaToken()
        if space_before_word:
            yield Deb822WhitespaceToken(sys.intern(space_before_word))
        if word:
            yield Deb822ValueToken(word)
        if space_after_word:
            yield Deb822WhitespaceToken(sys.intern(space_after_word))
