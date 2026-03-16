"""
Functions to encode and decode sequences to/from strings using custom escaping rules.
Useful for dealing with objects with multiple primary keys.

Copied from: https://github.com/flask-admin/flask-admin/blob/master/flask_admin/tools.py

"""

from typing import Iterable, Tuple

CHAR_ESCAPE = "."
CHAR_SEPARATOR = ","


def escape(value: str) -> str:
    """Escape a string using custom escaping rules."""
    return value.replace(CHAR_ESCAPE, CHAR_ESCAPE + CHAR_ESCAPE).replace(
        CHAR_SEPARATOR, CHAR_ESCAPE + CHAR_SEPARATOR
    )


def iterencode(iter: Iterable[str]) -> str:
    """Encode a sequence of strings into a single string. Each value in the sequence is escaped before being joined
    with the separator character."""
    return CHAR_SEPARATOR.join(escape(value) for value in iter)


def iterdecode(value: str) -> Tuple[str, ...]:
    """Decode an encoded string back to a tuple of string values."""
    result = []
    accumulator = ""

    escaped = False

    for char in value:
        if not escaped:
            if char == CHAR_ESCAPE:
                escaped = True
                continue
            if char == CHAR_SEPARATOR:
                result.append(accumulator)
                accumulator = ""
                continue
        else:
            escaped = False

        accumulator += char

    result.append(accumulator)

    return tuple(result)
