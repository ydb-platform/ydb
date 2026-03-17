import operator
from typing import Iterator, Union

try:
    from typing_extensions import (
        Literal,
    )
except ImportError:
    pass

from debian._deb822_repro._util import BufferingIterator
from debian._deb822_repro.tokens import Deb822Token
from debian._deb822_repro.types import TokenOrElement, FormatterCallback


# Consider these "opaque" enum-like values.  The actual value was chosen to
# make repr easier to implement, but they are subject to change.
_CONTENT_TYPE_VALUE = "is_value"
_CONTENT_TYPE_COMMENT = "is_comment"
_CONTENT_TYPE_SEPARATOR = "is_separator"


class FormatterContentToken:
    """Typed, tagged text for use with the formatting API

    The FormatterContentToken is used by the formatting API and provides the
    formatter callback with context about the textual tokens it is supposed
    to format.
    """

    __slots__ = ("_text", "_content_type")

    def __init__(self, text, content_type):
        # type: (str, object) -> None
        self._text = text
        self._content_type = content_type

    @classmethod
    def from_token_or_element(cls, token_or_element):
        # type: (TokenOrElement) -> FormatterContentToken
        if isinstance(token_or_element, Deb822Token):
            if token_or_element.is_comment:
                return cls.comment_token(token_or_element.text)
            if token_or_element.is_whitespace:
                raise ValueError("FormatterContentType cannot be whitespace")
            return cls.value_token(token_or_element.text)
        # Elements are assumed to be content (this is specialized for the
        # interpretations where comments are always tokens).
        return cls.value_token(token_or_element.convert_to_text())

    @classmethod
    def separator_token(cls, text):
        # type: (str) -> FormatterContentToken
        # Special-case separators as a minor memory optimization
        if text == " ":
            return SPACE_SEPARATOR_FT
        if text == ",":
            return COMMA_SEPARATOR_FT
        return cls(text, _CONTENT_TYPE_SEPARATOR)

    @classmethod
    def comment_token(cls, text):
        # type: (str) -> FormatterContentToken
        """Generates a single comment token with the provided text

        Mostly useful for creating test cases
        """
        return cls(text, _CONTENT_TYPE_COMMENT)

    @classmethod
    def value_token(cls, text):
        # type: (str) -> FormatterContentToken
        """Generates a single value token with the provided text

        Mostly useful for creating test cases
        """
        return cls(text, _CONTENT_TYPE_VALUE)

    @property
    def is_comment(self) -> bool:
        """True if this formatter token represent a comment

        This should be used for determining whether the token is a comment
        or not. It might be tempting to check whether the text in the token
        starts with a "#" but that is insufficient because a value *can*
        start with that as well.  Whether it is a comment or a value is
        based on the context (it is a comment if and only if the "#" was
        at the start of a line) but the formatter often do not have the
        context available to assert this.

        The formatter *should* preserve the order of comments and interleave
        between the value tokens in the same order as it see them.  Failing
        to preserve the order of comments and values can cause confusing
        comments (such as associating the comment with a different value
        than it was written for).

        The formatter *may* discard comment tokens if it does not want to
        preserve them.  If so, they would be omitted in the output, which
        may be acceptable in some cases.  This is a lot better than
        re-ordering comments.

        Formatters must be aware of the following special cases for comments:
         * Comments *MUST* be emitted after a newline.  If the very first token
           is a comment, the formatter is expected to emit a newline before it
           as well (Fields cannot start immediately on a comment).
        """
        return self._content_type is _CONTENT_TYPE_COMMENT

    @property
    def is_value(self) -> bool:
        """True if this formatter token represents a semantic value

        The formatter *MUST* preserve values as-in in its output.  It may
        "unpack" it from the token (as in, return it as a part of a plain
        str) but the value content must not be changed nor re-ordered relative
        to other value tokens (as that could change the meaning of the field).
        """
        return self._content_type is _CONTENT_TYPE_VALUE

    @property
    def is_separator(self) -> bool:
        """True if this formatter token represents a separator token

        The formatter is not required to preserve the provided separators but it
        is required to properly separate values.  In fact, often is a lot easier
        to discard existing separator tokens.  As an example, in whitespace
        separated list of values space, tab and newline all counts as separator.
        However, formatting-wise, there is a world of difference between the
        a space, tab and a newline. In particularly, newlines must be followed
        by an additional space or tab (to act as a value continuation line) if
        there is a value following it (otherwise, the generated output is
        invalid).
        """
        return self._content_type is _CONTENT_TYPE_SEPARATOR

    @property
    def is_whitespace(self) -> bool:
        """True if this formatter token represents a whitespace token"""
        return self._content_type is _CONTENT_TYPE_SEPARATOR and self._text.isspace()

    @property
    def text(self) -> str:
        """The actual context of the token

        This field *must not* be used to determine the type of token.  The
        formatter cannot reliably tell whether "#..." is a comment or a value
        (it can be both).  Use is_value and is_comment instead for discriminating
        token types.

        For value tokens, this the concrete value to be omitted.

        For comment token, this is the full comment text.

        This is the same as str(token).
        """
        return self._text

    def __str__(self) -> str:
        return self._text

    def __repr__(self) -> str:
        return "{}({!r}, {}=True)".format(
            self.__class__.__name__, self._text, self._content_type
        )


SPACE_SEPARATOR_FT = FormatterContentToken(" ", _CONTENT_TYPE_SEPARATOR)
COMMA_SEPARATOR_FT = FormatterContentToken(",", _CONTENT_TYPE_SEPARATOR)


def one_value_per_line_formatter(
    indentation,  # type: Union[int, Literal["FIELD_NAME_LENGTH"]]
    trailing_separator=True,  # type: bool
    immediate_empty_line=False,  # type: bool
):
    # type: (...) -> FormatterCallback
    """Provide a simple formatter that can handle indentation and trailing separators

    All formatters returned by this function puts exactly one value per line.  This
    pattern is commonly seen in the "Depends" field and similar fields of
    debian/control files.

    :param indentation: Either the literal string "FIELD_NAME_LENGTH" or a positive
    integer, which determines the indentation for fields.  If it is an integer,
    then a fixed indentation is used (notably the value 1 ensures the shortest
    possible indentation).  Otherwise, if it is "FIELD_NAME_LENGTH", then the
    indentation is set so that it aligns the values based on the field name.
    :param trailing_separator: If True, then the last value will have a trailing
    separator token (e.g., ",") after it.
    :param immediate_empty_line: Whether the value should always start with an
    empty line.  If True, then the result becomes something like "Field:\n value".

    """
    if indentation != "FIELD_NAME_LENGTH" and indentation < 1:
        raise ValueError('indentation must be at least 1 (or "FIELD_NAME_LENGTH")')

    def _formatter(
        name,  # type: str
        sep_token,  # type: FormatterContentToken
        formatter_tokens,  # type: Iterator[FormatterContentToken]
    ):
        # type: (...) -> Iterator[Union[FormatterContentToken, str]]
        if indentation == "FIELD_NAME_LENGTH":
            indent_len = len(name) + 2
        else:
            indent_len = indentation
            assert isinstance(indent_len, int)  # hint for PyCharm
        indent = " " * indent_len

        emitted_first_line = False
        tok_iter = BufferingIterator(formatter_tokens)
        is_value = operator.attrgetter("is_value")
        if immediate_empty_line:
            emitted_first_line = True
            yield "\n"
        for t in tok_iter:
            if t.is_comment:
                if not emitted_first_line:
                    yield "\n"
                yield t
            elif t.is_value:
                if not emitted_first_line:
                    yield " "
                else:
                    yield indent
                yield t
                if not sep_token.is_whitespace and (
                    trailing_separator or tok_iter.peek_find(is_value)
                ):
                    yield sep_token
                yield "\n"
            else:
                # Skip existing separators (etc.)
                continue
            emitted_first_line = True

    return _formatter


one_value_per_line_trailing_separator = one_value_per_line_formatter(
    "FIELD_NAME_LENGTH", trailing_separator=True
)


def format_field(
    formatter,  # type: FormatterCallback
    field_name,  # type: str
    separator_token,  # type: FormatterContentToken
    token_iter,  # type: Iterator[FormatterContentToken]
):
    # type: (...) -> str
    """Format a field using a provided formatter

    This function formats a series of tokens using the provided formatter.
    It can be used as a standalone formatter engine and can be used in test
    suites to validate third-party formatters (enabling them to test for
    corner cases without involving parsing logic).

    The formatter receives series of FormatterContentTokens (via the
    token_iter) and is expected to yield one or more str or
    FormatterContentTokens.  The calling function will combine all of
    these into a single string, which will be used as the value.

    The formatter is recommended to yield the provided value and comment
    tokens interleaved with text segments of whitespace and separators
    as part of its output.  If it preserve comment and value tokens, the
    calling function can provide some runtime checks to catch bugs
    (like the formatter turning a comment into a value because it forgot
    to ensure that the comment was emitted directly after a newline
    character).

    When writing a formatter, please keep the following in mind:

     * The output of the formatter is appended directly after the ":" separator.
       Most formatters will want to emit either a space or a newline as the very
       first character for readability.
       (compare "Depends:foo\\n" to "Depends: foo\\n")

     * The formatter must always end its output on a newline.  This is a design
       choice of how the round-trip safe parser represent values that is imposed
       on the formatter.

     * It is often easier to discard/ignore all separator tokens from the
       the provided token sequence and instead just yield separator tokens/str
       where the formatter wants to place them.

         - The formatter is strongly recommended to special-case formatting
           for whitespace separators (check for `separator_token.is_whitespace`).

           This is because space, tab and newline all counts as valid separators
           and can all appear in the token sequence. If the original field uses
           a mix of these separators it is likely to completely undermine the
           desired result. Not to mention the additional complexity of handling
           when a separator token happens to use the newline character which
           affects how the formatter is supposed what comes after it
           (see the rules for comments, empty lines and continuation line
           markers).

     * The formatter must remember to emit a "continuation line" marker
       (typically a single space or tab) when emitting a value after
       a newline or a comment. A `yield " "` is sufficient.

        - The continuation line marker may be embedded inside a str
          with other whitespace (such as the newline coming before it
          or/and whitespace used for indentation purposes following
          the marker).

     * The formatter must not cause the output to contain completely
       empty/whitespace lines as these cause syntax errors.  The first
       line never counts as an empty line (as it will be appended after
       the field name).

     * Tokens must be discriminated via the `token.is_value` (etc.)
       properties. Assuming that `token.text.startswith("#")` implies a
       comment and similar stunts are wrong.  As an example, "#foo" is a
       perfectly valid value in some contexts.

     * Comment tokens *always* take up exactly one complete line including
       the newline character at the end of the line. They must be emitted
       directly after a newline character or another comment token.

     * Special cases that are rare but can happen:

       - Fields *can* start with comments and requires a formatter provided newline.
         (Example: "Depends:\\n# Comment here\\n foo")

       - Fields *can* start on a separator or have two separators in a row.
         This is especially true for whitespace separated fields where every
         whitespace counts as a separator, but it can also happen with other
         separators (such as comma).

       - Value tokens can contain whitespace (for non-whitespace separators).
         When they do, the formatter must not attempt change nor "normalize"
         the whitespace inside the value token as that might change how the
         value is interpreted.  (If you want to normalize such whitespace,
         the formatter is at the wrong abstraction level.  Instead, manipulate
         the values directly in the value interpretation layer)

    This function will provide *some* runtime checks of its input and the
    output from the formatter to detect some errors early and provide
    helpful diagnostics.  If you use the function for testing, you are
    recommended to rely on verifying the output of the function rather than
    relying on the runtime checks (as these are subject to change).

    :param formatter: A formatter (see FormatterCallback for the type).
    Basic formatting is provided via one_value_per_line_trailing_separator
    (a formatter) or one_value_per_line_formatter (a formatter generator).
    :param field_name: The name of the field.
    :param separator_token: One of SPACE_SEPARATOR and COMMA_SEPARATOR
    :param token_iter: An iterable of tokens to be formatted.

    The following example shows how to define a formatter_callback along with
    a few verifications.

    >>> fmt_field_len_sep = one_value_per_line_trailing_separator
    >>> fmt_shortest = one_value_per_line_formatter(
    ...   1,
    ...   trailing_separator=False
    ... )
    >>> fmt_newline_first = one_value_per_line_formatter(
    ...   1,
    ...   trailing_separator=False,
    ...   immediate_empty_line=True
    ... )
    >>> # Omit separator tokens for in the token list for simplicity (the formatter does
    >>> # not use them, and it enables us to keep the example simple by reusing the list)
    >>> tokens = [
    ...     FormatterContentToken.value_token("foo"),
    ...     FormatterContentToken.comment_token("# some comment about bar\\n"),
    ...     FormatterContentToken.value_token("bar"),
    ... ]
    >>> # Starting with fmt_dl_ts
    >>> print(format_field(fmt_field_len_sep, "Depends", COMMA_SEPARATOR_FT, tokens), end='')
    Depends: foo,
    # some comment about bar
             bar,
    >>> print(format_field(fmt_field_len_sep, "Architecture", SPACE_SEPARATOR_FT, tokens), end='')
    Architecture: foo
    # some comment about bar
                  bar
    >>> # Control check for the special case where the field starts with a comment
    >>> print(format_field(fmt_field_len_sep, "Depends", COMMA_SEPARATOR_FT, tokens[1:]), end='')
    Depends:
    # some comment about bar
             bar,
    >>> # Also, check single line values (to ensure it ends on a newline)
    >>> print(format_field(fmt_field_len_sep, "Depends", COMMA_SEPARATOR_FT, tokens[2:]), end='')
    Depends: bar,
    >>> ### Changing format to the shortest length
    >>> print(format_field(fmt_shortest, "Depends", COMMA_SEPARATOR_FT, tokens), end='')
    Depends: foo,
    # some comment about bar
     bar
    >>> print(format_field(fmt_shortest, "Architecture", SPACE_SEPARATOR_FT, tokens), end='')
    Architecture: foo
    # some comment about bar
     bar
    >>> # Control check for the special case where the field starts with a comment
    >>> print(format_field(fmt_shortest, "Depends", COMMA_SEPARATOR_FT, tokens[1:]), end='')
    Depends:
    # some comment about bar
     bar
    >>> # Also, check single line values (to ensure it ends on a newline)
    >>> print(format_field(fmt_shortest, "Depends", COMMA_SEPARATOR_FT, tokens[2:]), end='')
    Depends: bar
    >>> ### Changing format to the newline first format
    >>> print(format_field(fmt_newline_first, "Depends", COMMA_SEPARATOR_FT, tokens), end='')
    Depends:
     foo,
    # some comment about bar
     bar
    >>> print(format_field(fmt_newline_first, "Architecture", SPACE_SEPARATOR_FT, tokens), end='')
    Architecture:
     foo
    # some comment about bar
     bar
    >>> # Control check for the special case where the field starts with a comment
    >>> print(format_field(fmt_newline_first, "Depends", COMMA_SEPARATOR_FT, tokens[1:]), end='')
    Depends:
    # some comment about bar
     bar
    >>> # Also, check single line values (to ensure it ends on a newline)
    >>> print(format_field(fmt_newline_first, "Depends", COMMA_SEPARATOR_FT, tokens[2:]), end='')
    Depends:
     bar
    """
    formatted_tokens = [field_name, ":"]
    just_after_newline = False
    last_was_value_token = False
    if isinstance(token_iter, list):
        # Stop people from using this to test known "invalid" cases.
        last_token = token_iter[-1]
        if last_token.is_comment:
            raise ValueError(
                "Invalid token_iter: Field values cannot end with comments"
            )
    for token in formatter(field_name, separator_token, token_iter):
        token_as_text = str(token)
        # If we are given formatter tokens, then use them to verify the output.
        if isinstance(token, FormatterContentToken):
            if token.is_comment:
                if not just_after_newline:
                    raise ValueError(
                        "Bad format: Comments must appear directly after a newline."
                    )
                # for the sake of ensuring people use proper test data.
                if not token_as_text.startswith("#"):
                    raise ValueError("Invalid Comment token: Must start with #")
                if not token_as_text.endswith("\n"):
                    raise ValueError("Invalid Comment token: Must end on a newline")
            elif token.is_value:
                if token_as_text[0].isspace() or token_as_text[-1].isspace():
                    raise ValueError(
                        "Invalid Value token: It cannot start nor end on whitespace"
                    )
                if just_after_newline:
                    raise ValueError("Bad format: Missing continuation line marker")
                if last_was_value_token:
                    raise ValueError("Bad format: Formatter omitted a separator")

            last_was_value_token = token.is_value
        else:
            last_was_value_token = False

        if just_after_newline:
            if token_as_text[0] in ("\r", "\n"):
                raise ValueError("Bad format: Saw completely empty line.")
            if not token_as_text[0].isspace() and not token_as_text.startswith("#"):
                raise ValueError("Bad format: Saw completely empty line.")
        formatted_tokens.append(token_as_text)
        just_after_newline = token_as_text.endswith("\n")

    formatted_text = "".join(formatted_tokens)
    if not formatted_text.endswith("\n"):
        raise ValueError("Bad format: The field value must end on a newline")
    return formatted_text
