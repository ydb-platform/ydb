"""Tools for parsing properties."""

import re

from icalendar.parser.string import unescape_string


def unescape_list_or_string(val: str | list[str]) -> str | list[str]:
    """Unescape a value that may be a string or list of strings.

    Applies :func:`unescape_string` to the value. If the value is a list,
    unescapes each element.

    Parameters:
        val: A string or list of strings to unescape.

    Returns:
        The unescaped values.
    """
    if isinstance(val, list):
        return [unescape_string(s) for s in val]
    return unescape_string(val)


_unescape_backslash_regex = re.compile(r"\\([\\,;:nN])")


def unescape_backslash(val: str):
    r"""Unescape backslash sequences in iCalendar text.

    Unlike :py:meth:`unescape_string`, this only handles actual backslash escapes
    per :rfc:`5545`, not URL encoding. This preserves URL-encoded values
    like ``%3A`` in URLs.

    Processes backslash escape sequences in a single pass using regex matching.
    """
    return _unescape_backslash_regex.sub(
        lambda m: "\n" if m.group(1) in "nN" else m.group(1), val
    )


def split_on_unescaped_comma(text: str) -> list[str]:
    r"""Split text on unescaped commas and unescape each part.

    Splits only on commas not preceded by backslash.
    After splitting, unescapes backslash sequences in each part.

    Parameters:
        text: Text with potential escaped commas (e.g., "foo\\, bar,baz")

    Returns:
        List of unescaped category strings

    Examples:
        .. code-block:: pycon

            >>> from icalendar.parser import split_on_unescaped_comma
            >>> split_on_unescaped_comma(r"foo\, bar,baz")
            ['foo, bar', 'baz']
            >>> split_on_unescaped_comma("a,b,c")
            ['a', 'b', 'c']
            >>> split_on_unescaped_comma(r"a\,b\,c")
            ['a,b,c']
            >>> split_on_unescaped_comma(r"Work,Personal\,Urgent")
            ['Work', 'Personal,Urgent']
    """
    if not text:
        return [""]

    result = []
    current = []
    i = 0

    while i < len(text):
        if text[i] == "\\" and i + 1 < len(text):
            # Escaped character - keep both backslash and next char
            current.append(text[i])
            current.append(text[i + 1])
            i += 2
        elif text[i] == ",":
            # Unescaped comma - split point
            result.append(unescape_backslash("".join(current)))
            current = []
            i += 1
        else:
            current.append(text[i])
            i += 1

    # Add final part
    result.append(unescape_backslash("".join(current)))

    return result


def split_on_unescaped_semicolon(text: str) -> list[str]:
    r"""Split text on unescaped semicolons and unescape each part.

    Splits only on semicolons not preceded by a backslash.
    After splitting, unescapes backslash sequences in each part.
    Used by vCard structured properties (ADR, N, ORG) per :rfc:`6350`.

    Parameters:
        text: Text with potential escaped semicolons (e.g., "field1\\;with;field2")

    Returns:
        List of unescaped field strings

    Examples:
        .. code-block:: pycon

            >>> from icalendar.parser import split_on_unescaped_semicolon
            >>> split_on_unescaped_semicolon(r"field1\;with;field2")
            ['field1;with', 'field2']
            >>> split_on_unescaped_semicolon("a;b;c")
            ['a', 'b', 'c']
            >>> split_on_unescaped_semicolon(r"a\;b\;c")
            ['a;b;c']
            >>> split_on_unescaped_semicolon(r"PO Box 123\;Suite 200;City")
            ['PO Box 123;Suite 200', 'City']
    """
    if not text:
        return [""]

    result = []
    current = []
    i = 0

    while i < len(text):
        if text[i] == "\\" and i + 1 < len(text):
            # Escaped character - keep both backslash and next char
            current.append(text[i])
            current.append(text[i + 1])
            i += 2
        elif text[i] == ";":
            # Unescaped semicolon - split point
            result.append(unescape_backslash("".join(current)))
            current = []
            i += 1
        else:
            current.append(text[i])
            i += 1

    # Add final part
    result.append(unescape_backslash("".join(current)))

    return result


__all__ = [
    "split_on_unescaped_comma",
    "split_on_unescaped_semicolon",
    "unescape_backslash",
    "unescape_list_or_string",
]
