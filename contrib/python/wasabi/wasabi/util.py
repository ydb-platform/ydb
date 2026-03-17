import difflib
import os
import sys
import textwrap
from typing import Any, Optional, Tuple, Union

STDOUT_ENCODING = sys.stdout.encoding if hasattr(sys.stdout, "encoding") else None
ENCODING = STDOUT_ENCODING or "ascii"
NO_UTF8 = ENCODING.lower() not in ("utf8", "utf-8")


# Environment variables
ENV_ANSI_DISABLED = "ANSI_COLORS_DISABLED"  # no colors


class MESSAGES(object):
    GOOD = "good"
    FAIL = "fail"
    WARN = "warn"
    INFO = "info"


COLORS = {
    MESSAGES.GOOD: 2,
    MESSAGES.FAIL: 1,
    MESSAGES.WARN: 3,
    MESSAGES.INFO: 4,
    "red": 1,
    "green": 2,
    "yellow": 3,
    "blue": 4,
    "pink": 5,
    "cyan": 6,
    "white": 7,
    "grey": 8,
    "black": 16,
}


ICONS = {
    MESSAGES.GOOD: "\u2714" if not NO_UTF8 else "[+]",
    MESSAGES.FAIL: "\u2718" if not NO_UTF8 else "[x]",
    MESSAGES.WARN: "\u26a0" if not NO_UTF8 else "[!]",
    MESSAGES.INFO: "\u2139" if not NO_UTF8 else "[i]",
}

INSERT_SYMBOL = "+"
DELETE_SYMBOL = "-"


def color(
    text: str,
    fg: Optional[Union[str, int]] = None,
    bg: Optional[Union[str, int]] = None,
    bold: bool = False,
    underline: bool = False,
) -> str:
    """Color text by applying ANSI escape sequence.

    text (str): The text to be formatted.
    fg (Optional[Union[str, int]]): Optional foreground color. String name or 0 - 256 (see COLORS).
    bg (Optional[Union[str, int]]): Optional background color. String name or 0 - 256 (see COLORS).
    bold (bool): Format text in bold.
    underline (bool): Underline text.
    RETURNS (str): The formatted text.
    """
    fg = COLORS.get(fg, fg)  # type: ignore
    bg = COLORS.get(bg, bg)  # type: ignore
    if not any([fg, bg, bold]):
        return text
    styles = []
    if bold:
        styles.append("1")
    if underline:
        styles.append("4")
    if fg:
        styles.append("38;5;{}".format(fg))
    if bg:
        styles.append("48;5;{}".format(bg))
    return "\x1b[{}m{}\x1b[0m".format(";".join(styles), text)


def wrap(text: Any, wrap_max: int = 80, indent: int = 4) -> str:
    """Wrap text at given width using textwrap module.

    text (Any): The text to wrap.
    wrap_max (int): Maximum line width, including indentation. Defaults to 80.
    indent (int): Number of spaces used for indentation. Defaults to 4.
    RETURNS (str): The wrapped text with line breaks.
    """
    indent_str = indent * " "
    wrap_width = wrap_max - len(indent_str)
    text = str(text)
    return textwrap.fill(
        text,
        width=wrap_width,
        initial_indent=indent_str,
        subsequent_indent=indent_str,
        break_long_words=False,
        break_on_hyphens=False,
    )


def format_repr(obj: Any, max_len: int = 50, ellipsis: str = "...") -> str:
    """Wrapper around `repr()` to print shortened and formatted string version.

    obj: The object to represent.
    max_len (int): Maximum string length. Longer strings will be cut in the
        middle so only the beginning and end is displayed, separated by ellipsis.
    ellipsis (str): Ellipsis character(s), e.g. "...".
    RETURNS (str): The formatted representation.
    """
    string = repr(obj)
    if len(string) >= max_len:
        half = int(max_len / 2)
        return "{} {} {}".format(string[:half], ellipsis, string[-half:])
    else:
        return string


def diff_strings(
    a: str,
    b: str,
    fg: Union[str, int] = "black",
    bg: Union[Tuple[str, str], Tuple[int, int]] = ("green", "red"),
    add_symbols: bool = False,
) -> str:
    """Compare two strings and return a colored diff with red/green background
    for deletion and insertions.

    a (str): The first string to diff.
    b (str): The second string to diff.
    fg (Union[str, int]): Foreground color. String name or 0 - 256 (see COLORS).
    bg (Union[Tuple[str, str], Tuple[int, int]]): Background colors as
        (insert, delete) tuple of string name or 0 - 256 (see COLORS).
    add_symbols (bool): Whether to add symbols before the diff lines. Uses '+'
        for inserts and '-' for deletions. Default is False.
    RETURNS (str): The formatted diff.
    """
    a_list = a.split("\n")
    b_list = b.split("\n")
    output = []
    matcher = difflib.SequenceMatcher(None, a_list, b_list)
    for opcode, a0, a1, b0, b1 in matcher.get_opcodes():
        if opcode == "equal":
            for item in a_list[a0:a1]:
                output.append(item)
        if opcode == "insert" or opcode == "replace":
            for item in b_list[b0:b1]:
                item = "{} {}".format(INSERT_SYMBOL, item) if add_symbols else item
                output.append(color(item, fg=fg, bg=bg[0]))
        if opcode == "delete" or opcode == "replace":
            for item in a_list[a0:a1]:
                item = "{} {}".format(DELETE_SYMBOL, item) if add_symbols else item
                output.append(color(item, fg=fg, bg=bg[1]))
    return "\n".join(output)


def get_raw_input(
    description: str, default: Optional[Union[str, bool]] = False, indent: int = 4
) -> str:
    """Get user input from the command line via raw_input / input.

    description (str): Text to display before prompt.
    default (Optional[Union[str, bool]]): Optional default value to display with prompt.
    indent (int): Indentation in spaces.
    RETURNS (str): User input.
    """
    additional = " (default: {})".format(default) if default else ""
    prompt = wrap("{}{}: ".format(description, additional), indent=indent)
    user_input = input(prompt)
    return user_input


def locale_escape(string: Any, errors: str = "replace") -> str:
    """Mangle non-supported characters, for savages with ASCII terminals.

    string (Any): The string to escape.
    errors (str): The str.encode errors setting. Defaults to `"replace"`.
    RETURNS (str): The escaped string.
    """
    string = str(string)
    string = string.encode(ENCODING, errors).decode("utf8")
    return string


def can_render(string: str) -> bool:
    """Check if terminal can render unicode characters, e.g. special loading
    icons. Can be used to display fallbacks for ASCII terminals.

    string (str): The string to render.
    RETURNS (bool): Whether the terminal can render the text.
    """
    try:
        string.encode(ENCODING)
        return True
    except UnicodeEncodeError:
        return False


def supports_ansi() -> bool:
    """Returns True if the running system's terminal supports ANSI escape
    sequences for color, formatting etc. and False otherwise.

    RETURNS (bool): Whether the terminal supports ANSI colors.
    """
    if os.getenv(ENV_ANSI_DISABLED):
        return False
    # We require colorama on Windows Python 3.7+, but we might be running on Unix, or we
    # might be running on Windows Python 3.6. In both cases, colorama might be missing,
    # *or* there might by accident happen to be an install of an old version that
    # doesn't have just_fix_windows_console. So we need to confirm not just that we can
    # import colorama, but that we can import just_fix_windows_console.
    try:
        from colorama import just_fix_windows_console
    except ImportError:
        if sys.platform == "win32" and "ANSICON" not in os.environ:
            return False
    else:
        just_fix_windows_console()
    return True
