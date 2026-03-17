"""Functions for manipulating strings and bytes."""

import re

from icalendar.parser_tools import DEFAULT_ENCODING


def escape_char(text: str | bytes) -> str | bytes:
    r"""Format value according to iCalendar TEXT escaping rules.

    Escapes special characters in text values according to :rfc:`5545#section-3.3.11`
    rules.
    The order of replacements matters to avoid double-escaping.

    Parameters:
        text: The text to escape.

    Returns:
        The escaped text with special characters escaped.

    Note:
        The replacement order is critical:

        1. ``\N`` -> ``\n`` (normalize newlines to lowercase)
        2. ``\`` -> ``\\`` (escape backslashes)
        3. ``;`` -> ``\;`` (escape semicolons)
        4. ``,`` -> ``\,`` (escape commas)
        5. ``\r\n`` -> ``\n`` (normalize line endings)
        6. ``"\n"`` -> ``r"\n"`` (transform a newline character to a literal, or raw,
           newline character)
    """
    assert isinstance(text, (str, bytes))
    # NOTE: ORDER MATTERS!
    return (
        text.replace(r"\N", "\n")
        .replace("\\", "\\\\")
        .replace(";", r"\;")
        .replace(",", r"\,")
        .replace("\r\n", r"\n")
        .replace("\n", r"\n")
    )


def unescape_char(text: str | bytes) -> str | bytes | None:
    r"""Unescape iCalendar TEXT values.

    Reverses the escaping applied by :func:`escape_char` according to
    :rfc:`5545#section-3.3.11` TEXT escaping rules.

    Parameters:
        text: The escaped text.

    Returns:
        The unescaped text, or ``None`` if ``text`` is neither ``str`` nor ``bytes``.

    Note:
        The replacement order is critical to avoid double-unescaping:

        1. ``\N`` -> ``\n`` (intermediate step)
        2. ``\r\n`` -> ``\n`` (normalize line endings)
        3. ``\n`` -> newline (unescape newlines)
        4. ``\,`` -> ``,`` (unescape commas)
        5. ``\;`` -> ``;`` (unescape semicolons)
        6. ``\\`` -> ``\`` (unescape backslashes last)
    """
    assert isinstance(text, (str, bytes))
    # NOTE: ORDER MATTERS!
    if isinstance(text, str):
        return (
            text.replace("\\N", "\\n")
            .replace("\r\n", "\n")
            .replace("\\n", "\n")
            .replace("\\,", ",")
            .replace("\\;", ";")
            .replace("\\\\", "\\")
        )
    if isinstance(text, bytes):
        return (
            text.replace(b"\\N", b"\\n")
            .replace(b"\r\n", b"\n")
            .replace(b"\\n", b"\n")
            .replace(b"\\,", b",")
            .replace(b"\\;", b";")
            .replace(b"\\\\", b"\\")
        )
    return None


def foldline(line: str, limit: int = 75, fold_sep: str = "\r\n ") -> str:
    """Make a string folded as defined in RFC5545
    Lines of text SHOULD NOT be longer than 75 octets, excluding the line
    break.  Long content lines SHOULD be split into a multiple line
    representations using a line "folding" technique.  That is, a long
    line can be split between any two characters by inserting a CRLF
    immediately followed by a single linear white-space character (i.e.,
    SPACE or HTAB).
    """
    assert isinstance(line, str)
    assert "\n" not in line

    # Use a fast and simple variant for the common case that line is all ASCII.
    try:
        line.encode("ascii")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    else:
        return fold_sep.join(
            line[i : i + limit - 1] for i in range(0, len(line), limit - 1)
        )

    ret_chars: list[str] = []
    byte_count = 0
    for char in line:
        char_byte_len = len(char.encode(DEFAULT_ENCODING))
        byte_count += char_byte_len
        if byte_count >= limit:
            ret_chars.append(fold_sep)
            byte_count = char_byte_len
        ret_chars.append(char)

    return "".join(ret_chars)


def escape_string(val: str) -> str:
    r"""Escape backslash sequences to URL-encoded hex values.

    Converts backslash-escaped characters to their percent-encoded hex
    equivalents. This is used for parameter parsing to preserve escaped
    characters during processing.

    Parameters:
        val: The string with backslash escapes.

    Returns:
        The string with backslash escapes converted to percent encoding.

    Note:
        Conversions:

        - ``\,`` -> ``%2C``
        - ``\:`` -> ``%3A``
        - ``\;`` -> ``%3B``
        - ``\\`` -> ``%5C``
    """
    # f'{i:02X}'
    return (
        val.replace(r"\,", "%2C")
        .replace(r"\:", "%3A")
        .replace(r"\;", "%3B")
        .replace(r"\\", "%5C")
    )


def unescape_string(val: str) -> str:
    r"""Unescape URL-encoded hex values to their original characters.

    Reverses :func:`escape_string` by converting percent-encoded hex values
    back to their original characters. This is used for parameter parsing.

    Parameters:
        val: The string with percent-encoded values.

    Returns:
        The string with percent encoding converted to characters.

    Note:
        Conversions:

        - ``%2C`` -> ``,``
        - ``%3A`` -> ``:``
        - ``%3B`` -> ``;``
        - ``%5C`` -> ``\``
    """
    return (
        val.replace("%2C", ",")
        .replace("%3A", ":")
        .replace("%3B", ";")
        .replace("%5C", "\\")
    )


# [\w-] because of the iCalendar RFC
# . because of the vCard RFC
NAME = re.compile(r"[\w.-]+")


def validate_token(name: str) -> None:
    r"""Validate that a name is a valid iCalendar token.

    Checks if the name matches the :rfc:`5545` token syntax using the NAME
    regex pattern (``[\w.-]+``).

    Parameters:
        name: The token name to validate.

    Raises:
        ValueError: If the name is not a valid token.
    """
    match = NAME.findall(name)
    if len(match) == 1 and name == match[0]:
        return
    raise ValueError(name)


__all__ = [
    "escape_char",
    "escape_string",
    "foldline",
    "unescape_char",
    "unescape_string",
    "validate_token",
]
