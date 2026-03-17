"""
This module provides functions for justifying Unicode text in a monospaced
display such as a terminal.

We used to have our own implementation here, but now we mostly rely on
the 'wcwidth' library.
"""

from unicodedata import normalize

from wcwidth import wcswidth, wcwidth

from ftfy.fixes import remove_terminal_escapes


def character_width(char: str) -> int:
    r"""
    Determine the width that a character is likely to be displayed as in
    a monospaced terminal. The width for a printable character will
    always be 0, 1, or 2.

    Nonprintable or control characters will return -1, a convention that comes
    from wcwidth.

    >>> character_width('車')
    2
    >>> character_width('A')
    1
    >>> character_width('\N{ZERO WIDTH JOINER}')
    0
    >>> character_width('\n')
    -1
    """
    return int(wcwidth(char))


def monospaced_width(text: str) -> int:
    r"""
    Return the number of character cells that this string is likely to occupy
    when displayed in a monospaced, modern, Unicode-aware terminal emulator.
    We refer to this as the "display width" of the string.

    This can be useful for formatting text that may contain non-spacing
    characters, or CJK characters that take up two character cells.

    Returns -1 if the string contains a non-printable or control character.

    >>> monospaced_width('ちゃぶ台返し')
    12
    >>> len('ちゃぶ台返し')
    6
    >>> monospaced_width('owl\N{SOFT HYPHEN}flavored')
    11
    >>> monospaced_width('example\x80')
    -1

    A more complex example: The Korean word 'ibnida' can be written with 3
    pre-composed characters or 7 jamo. Either way, it *looks* the same and
    takes up 6 character cells.

    >>> monospaced_width('입니다')
    6
    >>> monospaced_width('\u110b\u1175\u11b8\u1102\u1175\u1103\u1161')
    6

    The word "blue" with terminal escapes to make it blue still takes up only
    4 characters, when shown as intended.
    >>> monospaced_width('\x1b[34mblue\x1b[m')
    4
    """
    # NFC-normalize the text first, so that we don't need special cases for
    # Hangul jamo.
    #
    # Remove terminal escapes before calculating width, because if they are
    # displayed as intended, they will have zero width.
    return int(wcswidth(remove_terminal_escapes(normalize("NFC", text))))


def display_ljust(text: str, width: int, fillchar: str = " ") -> str:
    """
    Return `text` left-justified in a Unicode string whose display width,
    in a monospaced terminal, should be at least `width` character cells.
    The rest of the string will be padded with `fillchar`, which must be
    a width-1 character.

    "Left" here means toward the beginning of the string, which may actually
    appear on the right in an RTL context. This is similar to the use of the
    word "left" in "left parenthesis".

    >>> lines = ['Table flip', '(╯°□°)╯︵ ┻━┻', 'ちゃぶ台返し']
    >>> for line in lines:
    ...     print(display_ljust(line, 20, '▒'))
    Table flip▒▒▒▒▒▒▒▒▒▒
    (╯°□°)╯︵ ┻━┻▒▒▒▒▒▒▒
    ちゃぶ台返し▒▒▒▒▒▒▒▒

    This example, and the similar ones that follow, should come out justified
    correctly when viewed in a monospaced terminal. It will probably not look
    correct if you're viewing this code or documentation in a Web browser.
    """
    if character_width(fillchar) != 1:
        raise ValueError("The padding character must have display width 1")

    text_width = monospaced_width(text)
    if text_width == -1:
        # There's a control character here, so just don't add padding
        return text

    padding = max(0, width - text_width)
    return text + fillchar * padding


def display_rjust(text: str, width: int, fillchar: str = " ") -> str:
    """
    Return `text` right-justified in a Unicode string whose display width,
    in a monospaced terminal, should be at least `width` character cells.
    The rest of the string will be padded with `fillchar`, which must be
    a width-1 character.

    "Right" here means toward the end of the string, which may actually be on
    the left in an RTL context. This is similar to the use of the word "right"
    in "right parenthesis".

    >>> lines = ['Table flip', '(╯°□°)╯︵ ┻━┻', 'ちゃぶ台返し']
    >>> for line in lines:
    ...     print(display_rjust(line, 20, '▒'))
    ▒▒▒▒▒▒▒▒▒▒Table flip
    ▒▒▒▒▒▒▒(╯°□°)╯︵ ┻━┻
    ▒▒▒▒▒▒▒▒ちゃぶ台返し
    """
    if character_width(fillchar) != 1:
        raise ValueError("The padding character must have display width 1")

    text_width = monospaced_width(text)
    if text_width == -1:
        return text

    padding = max(0, width - text_width)
    return fillchar * padding + text


def display_center(text: str, width: int, fillchar: str = " ") -> str:
    """
    Return `text` centered in a Unicode string whose display width, in a
    monospaced terminal, should be at least `width` character cells. The rest
    of the string will be padded with `fillchar`, which must be a width-1
    character.

    >>> lines = ['Table flip', '(╯°□°)╯︵ ┻━┻', 'ちゃぶ台返し']
    >>> for line in lines:
    ...     print(display_center(line, 20, '▒'))
    ▒▒▒▒▒Table flip▒▒▒▒▒
    ▒▒▒(╯°□°)╯︵ ┻━┻▒▒▒▒
    ▒▒▒▒ちゃぶ台返し▒▒▒▒
    """
    if character_width(fillchar) != 1:
        raise ValueError("The padding character must have display width 1")

    text_width = monospaced_width(text)
    if text_width == -1:
        return text

    padding = max(0, width - text_width)
    left_padding = padding // 2
    right_padding = padding - left_padding
    return fillchar * left_padding + text + fillchar * right_padding
