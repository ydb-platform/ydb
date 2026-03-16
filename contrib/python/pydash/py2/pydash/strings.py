# -*- coding: utf-8 -*-
"""
String functions.

.. versionadded:: 1.1.0
"""

import math
import re
import unicodedata

import pydash as pyd

from ._compat import (
    _range,
    html_unescape,
    iteritems,
    parse_qsl,
    text_type,
    urlencode,
    urlsplit,
    urlunsplit,
)
from .helpers import NoValue
from .objects import to_string


__all__ = (
    "camel_case",
    "capitalize",
    "chop",
    "chop_right",
    "chars",
    "clean",
    "count_substr",
    "deburr",
    "decapitalize",
    "ends_with",
    "ensure_ends_with",
    "ensure_starts_with",
    "escape",
    "escape_reg_exp",
    "has_substr",
    "human_case",
    "insert_substr",
    "join",
    "kebab_case",
    "lines",
    "lower_case",
    "lower_first",
    "number_format",
    "pad",
    "pad_end",
    "pad_start",
    "pascal_case",
    "predecessor",
    "prune",
    "quote",
    "reg_exp_js_match",
    "reg_exp_js_replace",
    "reg_exp_replace",
    "repeat",
    "replace",
    "replace_end",
    "replace_start",
    "separator_case",
    "series_phrase",
    "series_phrase_serial",
    "slugify",
    "snake_case",
    "split",
    "start_case",
    "starts_with",
    "strip_tags",
    "substr_left",
    "substr_left_end",
    "substr_right",
    "substr_right_end",
    "successor",
    "surround",
    "swap_case",
    "title_case",
    "to_lower",
    "to_upper",
    "trim",
    "trim_end",
    "trim_start",
    "truncate",
    "unescape",
    "unquote",
    "upper_case",
    "upper_first",
    "url",
    "words",
)


HTML_ESCAPES = {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;", "`": "&#96;"}

DEBURRED_LETTERS = {
    "\xC0": "A",
    "\xC1": "A",
    "\xC2": "A",
    "\xC3": "A",
    "\xC4": "A",
    "\xC5": "A",
    "\xE0": "a",
    "\xE1": "a",
    "\xE2": "a",
    "\xE3": "a",
    "\xE4": "a",
    "\xE5": "a",
    "\xC7": "C",
    "\xE7": "c",
    "\xD0": "D",
    "\xF0": "d",
    "\xC8": "E",
    "\xC9": "E",
    "\xCA": "E",
    "\xCB": "E",
    "\xE8": "e",
    "\xE9": "e",
    "\xEA": "e",
    "\xEB": "e",
    "\xCC": "I",
    "\xCD": "I",
    "\xCE": "I",
    "\xCF": "I",
    "\xEC": "i",
    "\xED": "i",
    "\xEE": "i",
    "\xEF": "i",
    "\xD1": "N",
    "\xF1": "n",
    "\xD2": "O",
    "\xD3": "O",
    "\xD4": "O",
    "\xD5": "O",
    "\xD6": "O",
    "\xD8": "O",
    "\xF2": "o",
    "\xF3": "o",
    "\xF4": "o",
    "\xF5": "o",
    "\xF6": "o",
    "\xF8": "o",
    "\xD9": "U",
    "\xDA": "U",
    "\xDB": "U",
    "\xDC": "U",
    "\xF9": "u",
    "\xFA": "u",
    "\xFB": "u",
    "\xFC": "u",
    "\xDD": "Y",
    "\xFD": "y",
    "\xFF": "y",
    "\xC6": "Ae",
    "\xE6": "ae",
    "\xDE": "Th",
    "\xFE": "th",
    "\xDF": "ss",
    "\xD7": " ",
    "\xF7": " ",
}

# Use Javascript style regex to make Lo-Dash compatibility easier.
UPPER = "[A-Z\\xC0-\\xD6\\xD8-\\xDE]"
LOWER = "[a-z\\xDf-\\xF6\\xF8-\\xFF]+"
RE_WORDS = "/{upper}+(?={upper}{lower})|{upper}?{lower}|{upper}+|[0-9]+/g".format(
    upper=UPPER, lower=LOWER
)
RE_LATIN1 = "/[\xC0-\xFF]/g"


def camel_case(text):
    """
    Converts `text` to camel case.

    Args:
        text (str): String to convert.

    Returns:
        str: String converted to camel case.

    Example:

        >>> camel_case('FOO BAR_bAz')
        'fooBarBAz'

    .. versionadded:: 1.1.0
    """
    text = "".join(word.title() for word in words(text))
    return text[:1].lower() + text[1:]


def capitalize(text, strict=True):
    """
    Capitalizes the first character of `text`.

    Args:
        text (str): String to capitalize.
        strict (bool, optional): Whether to cast rest of string to lower case. Defaults to ``True``.

    Returns:
        str: Capitalized string.

    Example:

        >>> capitalize('once upon a TIME')
        'Once upon a time'
        >>> capitalize('once upon a TIME', False)
        'Once upon a TIME'

    .. versionadded:: 1.1.0

    .. versionchanged:: 3.0.0
        Added `strict` option.
    """
    text = pyd.to_string(text)
    return text.capitalize() if strict else text[:1].upper() + text[1:]


def chars(text):
    """
    Split `text` into a list of single characters.

    Args:
        text (str): String to split up.

    Returns:
        list: List of individual characters.

    Example:

        >>> chars('onetwo')
        ['o', 'n', 'e', 't', 'w', 'o']

    .. versionadded:: 3.0.0
    """
    return list(pyd.to_string(text))


def chop(text, step):
    """
    Break up `text` into intervals of length `step`.

    Args:
        text (str): String to chop.
        step (int): Interval to chop `text`.

    Returns:
        list: List of chopped characters. If `text` is `None` an empty list is returned.

    Example:

        >>> chop('abcdefg', 3)
        ['abc', 'def', 'g']

    .. versionadded:: 3.0.0
    """
    if text is None:
        return []

    text = pyd.to_string(text)

    if step <= 0:
        chopped = [text]
    else:
        chopped = [text[i : i + step] for i in _range(0, len(text), step)]

    return chopped


def chop_right(text, step):
    """
    Like :func:`chop` except `text` is chopped from right.

    Args:
        text (str): String to chop.
        step (int): Interval to chop `text`.

    Returns:
        list: List of chopped characters.

    Example:

        >>> chop_right('abcdefg', 3)
        ['a', 'bcd', 'efg']

    .. versionadded:: 3.0.0
    """
    if text is None:
        return []

    text = pyd.to_string(text)

    if step <= 0:
        chopped = [text]
    else:
        text_len = len(text)
        chopped = [text[-(i + step) : text_len - i] for i in _range(0, text_len, step)][::-1]

    return chopped


def clean(text):
    """
    Trim and replace multiple spaces with a single space.

    Args:
        text (str): String to clean.

    Returns:
        str: Cleaned string.

    Example:

        >>> clean('a  b   c    d')
        'a b c d'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    return " ".join(pyd.compact(text.split()))


def count_substr(text, subtext):
    """
    Count the occurrences of `subtext` in `text`.

    Args:
        text (str): Source string to count from.
        subtext (str): String to count.

    Returns:
        int: Number of occurrences of `subtext` in `text`.

    Example:

        >>> count_substr('aabbccddaabbccdd', 'bc')
        2

    .. versionadded:: 3.0.0
    """
    if text is None or subtext is None:
        return 0

    text = pyd.to_string(text)
    subtext = pyd.to_string(subtext)

    return text.count(subtext)


def deburr(text):
    """
    Deburrs `text` by converting latin-1 supplementary letters to basic latin letters.

    Args:
        text (str): String to deburr.

    Returns:
        str: Deburred string.

    Example:

        >>> deburr('déjà vu')
        '...
        >>> 'deja vu'
        'deja vu'

    .. versionadded:: 2.0.0
    """
    return reg_exp_js_replace(
        pyd.to_string(text),
        RE_LATIN1,
        lambda match: DEBURRED_LETTERS.get(match.group(), match.group()),
    )


def decapitalize(text):
    """
    Decaptitalizes the first character of `text`.

    Args:
        text (str): String to decapitalize.

    Returns:
        str: Decapitalized string.

    Example:

        >>> decapitalize('FOO BAR')
        'fOO BAR'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    return text[:1].lower() + text[1:]


def ends_with(text, target, position=None):
    """
    Checks if `text` ends with a given target string.

    Args:
        text (str): String to check.
        target (str): String to check for.
        position (int, optional): Position to search from. Defaults to end of `text`.

    Returns:
        bool: Whether `text` ends with `target`.

    Example:

        >>> ends_with('abc def', 'def')
        True
        >>> ends_with('abc def', 4)
        False

    .. versionadded:: 1.1.0
    """
    target = pyd.to_string(target)
    text = pyd.to_string(text)

    if position is None:
        position = len(text)

    return text[:position].endswith(target)


def ensure_ends_with(text, suffix):
    """
    Append a given suffix to a string, but only if the source string does not end with that suffix.

    Args:
        text (str): Source string to append `suffix` to.
        suffix (str): String to append to the source string if the source string does not end with
            `suffix`.

    Returns:
        str: source string possibly extended by `suffix`.

    Example:

        >>> ensure_ends_with('foo bar', '!')
        'foo bar!'
        >>> ensure_ends_with('foo bar!', '!')
        'foo bar!'

    .. versionadded:: 2.4.0
    """
    text = pyd.to_string(text)
    suffix = pyd.to_string(suffix)
    return text if text.endswith(suffix) else "{0}{1}".format(text, suffix)


def ensure_starts_with(text, prefix):
    """
    Prepend a given prefix to a string, but only if the source string does not start with that
    prefix.

    Args:
        text (str): Source string to prepend `prefix` to.
        suffix (str): String to prepend to the source string if the source string does not start
            with `prefix`.

    Returns:
        str: source string possibly prefixed by `prefix`

    Example:

        >>> ensure_starts_with('foo bar', 'Oh my! ')
        'Oh my! foo bar'
        >>> ensure_starts_with('Oh my! foo bar', 'Oh my! ')
        'Oh my! foo bar'

    .. versionadded:: 2.4.0
    """
    text = pyd.to_string(text)
    prefix = pyd.to_string(prefix)
    return text if text.startswith(prefix) else "{1}{0}".format(text, prefix)


def escape(text):
    r"""
    Converts the characters ``&``, ``<``, ``>``, ``"``, ``'``, and ``\``` in `text` to their
    corresponding HTML entities.

    Args:
        text (str): String to escape.

    Returns:
        str: HTML escaped string.

    Example:

        >>> escape('"1 > 2 && 3 < 4"')
        '&quot;1 &gt; 2 &amp;&amp; 3 &lt; 4&quot;'

    .. versionadded:: 1.0.0

    .. versionchanged:: 1.1.0
        Moved function to :mod:`pydash.strings`.
    """
    text = pyd.to_string(text)
    # NOTE: Not using _compat.html_escape because Lo-Dash escapes certain chars differently (e.g.
    # "'" isn't escaped by html_escape() but is by Lo-Dash).
    return "".join(HTML_ESCAPES.get(char, char) for char in text)


def escape_reg_exp(text):
    """
    Escapes the RegExp special characters in `text`.

    Args:
        text (str): String to escape.

    Returns:
        str: RegExp escaped string.

    Example:

        >>> escape_reg_exp('[()]')
        '\\\\[\\\\(\\\\)\\\\]'

    .. versionadded:: 1.1.0

    .. versionchanged:: 4.0.0
        Removed alias ``escape_re``
    """
    text = pyd.to_string(text)
    return re.escape(text)


def has_substr(text, subtext):
    """
    Returns whether `subtext` is included in `text`.

    Args:
        text (str): String to search.
        subtext (str): String to search for.

    Returns:
        bool: Whether `subtext` is found in `text`.

    Example:

        >>> has_substr('abcdef', 'bc')
        True
        >>> has_substr('abcdef', 'bb')
        False

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    subtext = pyd.to_string(subtext)
    return text.find(subtext) >= 0


def human_case(text):
    """
    Converts `text` to human case which has only the first letter capitalized and each word
    separated by a space.

    Args:
        text (str): String to convert.

    Returns:
        str: String converted to human case.

    Example:

        >>> human_case('abc-def_hij lmn')
        'Abc def hij lmn'
        >>> human_case('user_id')
        'User'

    .. versionadded:: 3.0.0
    """
    return (
        pyd.chain(text)
        .snake_case()
        .reg_exp_replace("_id$", "")
        .replace("_", " ")
        .capitalize()
        .value()
    )


def insert_substr(text, index, subtext):
    """
    Insert `subtext` in `text` starting at position `index`.

    Args:
        text (str): String to add substring to.
        index (int): String index to insert into.
        subtext (str): String to insert.

    Returns:
        str: Modified string.

    Example:

        >>> insert_substr('abcdef', 3, '--')
        'abc--def'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    subtext = pyd.to_string(subtext)
    return text[:index] + subtext + text[index:]


def join(array, separator=""):
    """
    Joins an iterable into a string using `separator` between each element.

    Args:
        array (iterable): Iterable to implode.
        separator (str, optional): Separator to using when joining. Defaults to ``''``.

    Returns:
        str: Joined string.

    Example:

        >>> join(['a', 'b', 'c']) == 'abc'
        True
        >>> join([1, 2, 3, 4], '&') == '1&2&3&4'
        True
        >>> join('abcdef', '-') == 'a-b-c-d-e-f'
        True

    .. versionadded:: 2.0.0

    .. versionchanged:: 4.0.0
        Removed alias ``implode``.
    """
    return pyd.to_string(separator).join(pyd.map_(array or (), pyd.to_string))


def kebab_case(text):
    """
    Converts `text` to kebab case (a.k.a. spinal case).

    Args:
        text (str): String to convert.

    Returns:
        str: String converted to kebab case.

    Example:

        >>> kebab_case('a b c_d-e!f')
        'a-b-c-d-e-f'

    .. versionadded:: 1.1.0
    """
    return separator_case(text, "-")


def lines(text):
    r"""Split lines in `text` into an array.

    Args:
        text (str): String to split.

    Returns:
        list: String split by lines.

    Example:

        >>> lines('a\nb\r\nc')
        ['a', 'b', 'c']

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    return text.splitlines()


def lower_case(text):
    """
    Converts string to lower case as space separated words.

    Args:
        text (str): String to convert.

    Returns:
        str: String converted to lower case as space separated words.

    Example:

        >>> lower_case('fooBar')
        'foo bar'
        >>> lower_case('--foo-Bar--')
        'foo bar'
        >>> lower_case('/?*Foo10/;"B*Ar')
        'foo 10 b ar'

    .. versionadded:: 4.0.0
    """
    return " ".join(words(text)).lower()


def lower_first(text):
    """
    Converts the first character of string to lower case.

    Args:
        text (str): String passed in by the user.

    Returns:
        str: String in which the first character is converted to lower case.

    Example:

        >>> lower_first('FRED')
        'fRED'
        >>> lower_first('Foo Bar')
        'foo Bar'
        >>> lower_first('1foobar')
        '1foobar'
        >>> lower_first(';foobar')
        ';foobar'

    .. versionadded:: 4.0.0
    """
    return text[:1].lower() + text[1:]


def number_format(number, scale=0, decimal_separator=".", order_separator=","):
    """
    Format a number to scale with custom decimal and order separators.

    Args:
        number (int|float): Number to format.
        scale (int, optional): Number of decimals to include. Defaults to ``0``.
        decimal_separator (str, optional): Decimal separator to use. Defaults to ``'.'``.
        order_separator (str, optional): Order separator to use. Defaults to ``','``.

    Returns:
        str: Formatted number as string.

    Example:

        >>> number_format(1234.5678)
        '1,235'
        >>> number_format(1234.5678, 2, ',', '.')
        '1.234,57'

    .. versionadded:: 3.0.0
    """
    # Create a string formatter which converts number to the appropriately scaled representation.
    fmt = "{{0:.{0:d}f}}".format(scale)

    try:
        num_parts = fmt.format(number).split(".")
    except ValueError:
        text = ""
    else:
        int_part = num_parts[0]
        dec_part = (num_parts + [""])[1]

        # Reverse the integer part, chop it into groups of 3, join on `order_separator`, and then
        # un-reverse the string.
        int_part = order_separator.join(chop(int_part[::-1], 3))[::-1]

        text = decimal_separator.join(pyd.compact([int_part, dec_part]))

    return text


def pad(text, length, chars=" "):
    """
    Pads `text` on the left and right sides if it is shorter than the given padding length. The
    `chars` string may be truncated if the number of padding characters can't be evenly divided by
    the padding length.

    Args:
        text (str): String to pad.
        length (int): Amount to pad.
        chars (str, optional): Characters to pad with. Defaults to ``" "``.

    Returns:
        str: Padded string.

    Example:

        >>> pad('abc', 5)
        ' abc '
        >>> pad('abc', 6, 'x')
        'xabcxx'
        >>> pad('abc', 5, '...')
        '.abc.'

    .. versionadded:: 1.1.0

    .. versionchanged:: 3.0.0
        Fix handling of multiple `chars` so that padded string isn't over padded.
    """
    # pylint: disable=redefined-outer-name
    text = pyd.to_string(text)
    text_len = len(text)

    if text_len >= length:
        return text

    mid = (length - text_len) / 2.0
    left_len = int(math.floor(mid))
    right_len = int(math.ceil(mid))
    chars = pad_end("", right_len, chars)

    return chars[:left_len] + text + chars


def pad_end(text, length, chars=" "):
    """
    Pads `text` on the right side if it is shorter than the given padding length. The `chars` string
    may be truncated if the number of padding characters can't be evenly divided by the padding
    length.

    Args:
        text (str): String to pad.
        length (int): Amount to pad.
        chars (str, optional): Characters to pad with. Defaults to ``" "``.

    Returns:
        str: Padded string.

    Example:

        >>> pad_end('abc', 5)
        'abc  '
        >>> pad_end('abc', 5, '.')
        'abc..'

    .. versionadded:: 1.1.0

    .. versionchanged:: 4.0.0
        Renamed from ``pad_right`` to ``pad_end``.
    """
    # pylint: disable=redefined-outer-name
    text = pyd.to_string(text)
    length = max((length, len(text)))
    return (text + repeat(chars, length))[:length]


def pad_start(text, length, chars=" "):
    """
    Pads `text` on the left side if it is shorter than the given padding length. The `chars` string
    may be truncated if the number of padding characters can't be evenly divided by the padding
    length.

    Args:
        text (str): String to pad.
        length (int): Amount to pad.
        chars (str, optional): Characters to pad with. Defaults to ``" "``.

    Returns:
        str: Padded string.

    Example:

        >>> pad_start('abc', 5)
        '  abc'
        >>> pad_start('abc', 5, '.')
        '..abc'

    .. versionadded:: 1.1.0

    .. versionchanged:: 4.0.0
        Renamed from ``pad_left`` to ``pad_start``.
    """
    # pylint: disable=redefined-outer-name
    text = pyd.to_string(text)
    length = max(length, len(text))
    return (repeat(chars, length) + text)[-length:]


def pascal_case(text, strict=True):
    """
    Like :func:`camel_case` except the first letter is capitalized.

    Args:
        text (str): String to convert.
        strict (bool, optional): Whether to cast rest of string to lower case. Defaults to ``True``.

    Returns:
        str: String converted to class case.

    Example:

        >>> pascal_case('FOO BAR_bAz')
        'FooBarBaz'
        >>> pascal_case('FOO BAR_bAz', False)
        'FooBarBAz'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)

    if strict:
        text = text.lower()

    return capitalize(camel_case(text), strict=False)


def predecessor(char):
    """
    Return the predecessor character of `char`.

    Args:
        char (str): Character to find the predecessor of.

    Returns:
        str: Predecessor character.

    Example:

        >>> predecessor('c')
        'b'
        >>> predecessor('C')
        'B'
        >>> predecessor('3')
        '2'

    .. versionadded:: 3.0.0
    """
    char = pyd.to_string(char)
    return chr(ord(char) - 1)


def prune(text, length=0, omission="..."):
    """
    Like :func:`truncate` except it ensures that the pruned string doesn't exceed the original
    length, i.e., it avoids half-chopped words when truncating. If the pruned text + `omission` text
    is longer than the original text, then the original text is returned.

    Args:
        text (str): String to prune.
        length (int, optional): Target prune length. Defaults to ``0``.
        omission (str, optional): Omission text to append to the end of the pruned string. Defaults
            to ``'...'``.

    Returns:
        str: Pruned string.

    Example:

        >>> prune('Fe fi fo fum', 5)
        'Fe fi...'
        >>> prune('Fe fi fo fum', 6)
        'Fe fi...'
        >>> prune('Fe fi fo fum', 7)
        'Fe fi...'
        >>> prune('Fe fi fo fum', 8, ',,,')
        'Fe fi fo,,,'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    text_len = len(text)
    omission_len = len(omission)

    if text_len <= length:
        return text

    # Replace non-alphanumeric chars with whitespace.
    def repl(match):
        char = match.group(0)
        return " " if char.upper() == char.lower() else char

    subtext = reg_exp_replace(text[: length + 1], r".(?=\W*\w*$)", repl)

    if re.match(r"\w\w", subtext[-2:]):
        # Last two characters are alphanumeric. Remove last "word" from end of string so that we
        # prune to the next whole word.
        subtext = reg_exp_replace(subtext, r"\s*\S+$", "")
    else:
        # Last character (at least) is whitespace. So remove that character as well as any other
        # whitespace.
        subtext = subtext[:-1].rstrip()

    subtext_len = len(subtext)

    # Only add omission text if doing so will result in a string that is equal two or smaller in
    # length than the original.
    if (subtext_len + omission_len) <= text_len:
        text = text[:subtext_len] + omission

    return text


def quote(text, quote_char='"'):
    """
    Quote a string with another string.

    Args:
        text (str): String to be quoted.
        quote_char (str, optional): the quote character. Defaults to ``"``.

    Returns:
        str: the quoted string.

    Example:

        >>> quote('To be or not to be')
        '"To be or not to be"'
        >>> quote('To be or not to be', "'")
        "'To be or not to be'"

    .. versionadded:: 2.4.0
    """
    return surround(text, quote_char)


def reg_exp_js_match(text, reg_exp):
    """
    Return list of matches using Javascript style regular expression.

    Args:
        text (str): String to evaluate.
        reg_exp (str): Javascript style regular expression.

    Returns:
        list: List of matches.

    Example:

        >>> reg_exp_js_match('aaBBcc', '/bb/')
        []
        >>> reg_exp_js_match('aaBBcc', '/bb/i')
        ['BB']
        >>> reg_exp_js_match('aaBBccbb', '/bb/i')
        ['BB']
        >>> reg_exp_js_match('aaBBccbb', '/bb/gi')
        ['BB', 'bb']

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.0.0
        Reordered arguments to make `text` first.

    .. versionchanged:: 4.0.0
        Renamed from ``js_match`` to ``reg_exp_js_match``.
    """
    text = pyd.to_string(text)
    return js_to_py_re_find(reg_exp)(text)


def reg_exp_js_replace(text, reg_exp, repl):
    """
    Replace `text` with `repl` using Javascript style regular expression to find matches.

    Args:
        text (str): String to evaluate.
        reg_exp (str): Javascript style regular expression.
        repl (str): Replacement string.

    Returns:
        str: Modified string.

    Example:

        >>> reg_exp_js_replace('aaBBcc', '/bb/', 'X')
        'aaBBcc'
        >>> reg_exp_js_replace('aaBBcc', '/bb/i', 'X')
        'aaXcc'
        >>> reg_exp_js_replace('aaBBccbb', '/bb/i', 'X')
        'aaXccbb'
        >>> reg_exp_js_replace('aaBBccbb', '/bb/gi', 'X')
        'aaXccX'

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.0.0
        Reordered arguments to make `text` first.

    .. versionchanged:: 4.0.0
        Renamed from ``js_replace`` to ``reg_exp_js_replace``.
    """
    text = pyd.to_string(text)
    if not pyd.is_function(repl):
        repl = pyd.to_string(repl)
    return js_to_py_reg_exp_replace(reg_exp)(text, repl)


def reg_exp_replace(text, pattern, repl, ignore_case=False, count=0):
    """
    Replace occurrences of regex `pattern` with `repl` in `text`. Optionally, ignore case when
    replacing. Optionally, set `count` to limit number of replacements.

    Args:
        text (str): String to replace.
        pattern (str): String pattern to find and replace.
        repl (str): String to substitute `pattern` with.
        ignore_case (bool, optional): Whether to ignore case when replacing. Defaults to ``False``.
        count (int, optional): Maximum number of occurrences to replace. Defaults to ``0`` which
            replaces all.

    Returns:
        str: Replaced string.

    Example:

        >>> reg_exp_replace('aabbcc', 'b', 'X')
        'aaXXcc'
        >>> reg_exp_replace('aabbcc', 'B', 'X', ignore_case=True)
        'aaXXcc'
        >>> reg_exp_replace('aabbcc', 'b', 'X', count=1)
        'aaXbcc'
        >>> reg_exp_replace('aabbcc', '[ab]', 'X')
        'XXXXcc'

    .. versionadded:: 3.0.0

    .. versionchanged:: 4.0.0
        Renamed from ``re_replace`` to ``reg_exp_replace``.
    """
    if pattern is None:
        return pyd.to_string(text)

    return replace(text, pattern, repl, ignore_case=ignore_case, count=count, escape=False)


def repeat(text, n=0):
    """
    Repeats the given string `n` times.

    Args:
        text (str): String to repeat.
        n (int, optional): Number of times to repeat the string.

    Returns:
        str: Repeated string.

    Example:

        >>> repeat('.', 5)
        '.....'

    .. versionadded:: 1.1.0
    """
    return pyd.to_string(text) * int(n)


def replace(
    text, pattern, repl, ignore_case=False, count=0, escape=True, from_start=False, from_end=False
):
    """
    Replace occurrences of `pattern` with `repl` in `text`. Optionally, ignore case when replacing.
    Optionally, set `count` to limit number of replacements.

    Args:
        text (str): String to replace.
        pattern (str): String pattern to find and replace.
        repl (str): String to substitute `pattern` with.
        ignore_case (bool, optional): Whether to ignore case when replacing. Defaults to ``False``.
        count (int, optional): Maximum number of occurrences to replace. Defaults to ``0`` which
            replaces all.
        escape (bool, optional): Whether to escape `pattern` when searching. This is needed if a
            literal replacement is desired when `pattern` may contain special regular expression
            characters. Defaults to ``True``.
        from_start (bool, optional): Whether to limit replacement to start of string.
        from_end (bool, optional): Whether to limit replacement to end of string.

    Returns:
        str: Replaced string.

    Example:

        >>> replace('aabbcc', 'b', 'X')
        'aaXXcc'
        >>> replace('aabbcc', 'B', 'X', ignore_case=True)
        'aaXXcc'
        >>> replace('aabbcc', 'b', 'X', count=1)
        'aaXbcc'
        >>> replace('aabbcc', '[ab]', 'X')
        'aabbcc'
        >>> replace('aabbcc', '[ab]', 'X', escape=False)
        'XXXXcc'

    .. versionadded:: 3.0.0

    .. versionchanged:: 4.1.0
        Added ``from_start`` and ``from_end`` arguments.
    """
    text = pyd.to_string(text)

    if pattern is None:
        return text

    pattern = pyd.to_string(pattern)

    if escape:
        pattern = re.escape(pattern)

    if from_start and not pattern.startswith("^"):
        pattern = "^" + pattern

    if from_end and not pattern.endswith("$"):
        pattern += "$"

    if not pyd.is_function(repl):
        repl = pyd.to_string(repl)

    flags = re.IGNORECASE if ignore_case else 0

    return re.sub(pattern, repl, text, count=count, flags=flags)


def replace_end(text, pattern, repl, ignore_case=False, escape=True):
    """
    Like :func:`replace` except it only replaces `text` with `repl` if `pattern` mathces the end of
    `text`.

    Args:
        text (str): String to replace.
        pattern (str): String pattern to find and replace.
        repl (str): String to substitute `pattern` with.
        ignore_case (bool, optional): Whether to ignore case when replacing. Defaults to ``False``.
        escape (bool, optional): Whether to escape `pattern` when searching. This is needed if a
            literal replacement is desired when `pattern` may contain special regular expression
            characters. Defaults to ``True``.

    Returns:
        str: Replaced string.

    Example:

        >>> replace_end('aabbcc', 'b', 'X')
        'aabbcc'
        >>> replace_end('aabbcc', 'c', 'X')
        'aabbcX'

    .. versionadded:: 4.1.0
    """
    return replace(text, pattern, repl, ignore_case=ignore_case, escape=escape, from_end=True)


def replace_start(text, pattern, repl, ignore_case=False, escape=True):
    """
    Like :func:`replace` except it only replaces `text` with `repl` if `pattern` mathces the start
    of `text`.

    Args:
        text (str): String to replace.
        pattern (str): String pattern to find and replace.
        repl (str): String to substitute `pattern` with.
        ignore_clase (bool, optional): Whether to ignore case when replacing. Defaults to ``False``.
        escape (bool, optional): Whether to escape `pattern` when searching. This is needed if a
            literal replacement is desired when `pattern` may contain special regular expression
            characters. Defaults to ``True``.

    Returns:
        str: Replaced string.

    Example:

        >>> replace_start('aabbcc', 'b', 'X')
        'aabbcc'
        >>> replace_start('aabbcc', 'a', 'X')
        'Xabbcc'

    .. versionadded:: 4.1.0
    """
    return replace(text, pattern, repl, ignore_case=ignore_case, escape=escape, from_start=True)


def separator_case(text, separator):
    """
    Splits `text` on words and joins with `separator`.

    Args:
        text (str): String to convert.
        separator (str): Separator to join words with.

    Returns:
        str: Converted string.

    Example:

        >>> separator_case('a!!b___c.d', '-')
        'a-b-c-d'

    .. versionadded:: 3.0.0
    """
    return separator.join(word.lower() for word in words(text) if word)


def series_phrase(items, separator=", ", last_separator=" and ", serial=False):
    """
    Join items into a grammatical series phrase, e.g., ``"item1, item2, item3 and item4"``.

    Args:
        items (list): List of string items to join.
        separator (str, optional): Item separator. Defaults to ``', '``.
        last_separator (str, optional): Last item separator. Defaults to ``' and '``.
        serial (bool, optional): Whether to include `separator` with `last_separator` when number of
            items is greater than 2. Defaults to ``False``.

    Returns:
        str: Joined string.

    Example:

        >>> series_phrase(['apples', 'bananas', 'peaches'])
        'apples, bananas and peaches'
        >>> series_phrase(['apples', 'bananas', 'peaches'], serial=True)
        'apples, bananas, and peaches'
        >>> series_phrase(['apples', 'bananas', 'peaches'], '; ', ', or ')
        'apples; bananas, or peaches'


    .. versionadded:: 3.0.0
    """
    items = pyd.chain(items).map(pyd.to_string).compact().value()
    item_count = len(items)

    separator = pyd.to_string(separator)
    last_separator = pyd.to_string(last_separator)

    if item_count > 2 and serial:
        last_separator = separator.rstrip() + last_separator

    if item_count >= 2:
        items = items[:-2] + [last_separator.join(items[-2:])]

    return separator.join(items)


def series_phrase_serial(items, separator=", ", last_separator=" and "):
    """
    Join items into a grammatical series phrase using a serial separator, e.g., ``"item1, item2,
    item3, and item4"``.

    Args:
        items (list): List of string items to join.
        separator (str, optional): Item separator. Defaults to ``', '``.
        last_separator (str, optional): Last item separator. Defaults to ``' and '``.

    Returns:
        str: Joined string.

    Example:

        >>> series_phrase_serial(['apples', 'bananas', 'peaches'])
        'apples, bananas, and peaches'

    .. versionadded:: 3.0.0
    """
    return series_phrase(items, separator, last_separator, serial=True)


def slugify(text, separator="-"):
    """
    Convert `text` into an ASCII slug which can be used safely in URLs. Incoming `text` is converted
    to unicode and noramlzied using the ``NFKD`` form. This results in some accented characters
    being converted to their ASCII "equivalent" (e.g. ``é`` is converted to ``e``). Leading and
    trailing whitespace is trimmed and any remaining whitespace or other special characters without
    an ASCII equivalent are replaced with ``-``.

    Args:
        text (str): String to slugify.
        separator (str, optional): Separator to use. Defaults to ``'-'``.

    Returns:
        str: Slugified string.

    Example:

        >>> slugify('This is a slug.') == 'this-is-a-slug'
        True
        >>> slugify('This is a slug.', '+') == 'this+is+a+slug'
        True

    .. versionadded:: 3.0.0
    """
    normalized = (
        unicodedata.normalize("NFKD", text_type(pyd.to_string(text)))
        .encode("ascii", "ignore")
        .decode("utf8")
    )

    return separator_case(normalized, separator)


def snake_case(text):
    """
    Converts `text` to snake case.

    Args:
        text (str): String to convert.

    Returns:
        str: String converted to snake case.

    Example:

        >>> snake_case('This is Snake Case!')
        'this_is_snake_case'

    .. versionadded:: 1.1.0

    .. versionchanged:: 4.0.0
        Removed alias ``underscore_case``.
    """
    return separator_case(text, "_")


def split(text, separator=NoValue):
    """
    Splits `text` on `separator`. If `separator` not provided, then `text` is split on whitespace.
    If `separator` is falsey, then `text` is split on every character.

    Args:
        text (str): String to explode.
        separator (str, optional): Separator string to split on. Defaults to ``NoValue``.

    Returns:
        list: Split string.

    Example:

        >>> split('one potato, two potatoes, three potatoes, four!')
        ['one', 'potato,', 'two', 'potatoes,', 'three', 'potatoes,', 'four!']
        >>> split('one potato, two potatoes, three potatoes, four!', ',')
        ['one potato', ' two potatoes', ' three potatoes', ' four!']

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.0.0
        Changed `separator` default to ``NoValue`` and supported splitting on whitespace by default.

    .. versionchanged:: 4.0.0
        Removed alias ``explode``.
    """
    text = pyd.to_string(text)

    if separator is NoValue:
        ret = text.split()
    elif separator:
        ret = text.split(separator)
    else:
        ret = chars(text)

    return ret


def start_case(text):
    """
    Convert `text` to start case.

    Args:
        text (str): String to convert.

    Returns:
        str: String converted to start case.

    Example:

        >>> start_case("fooBar")
        'Foo Bar'

    .. versionadded:: 3.1.0
    """
    text = pyd.to_string(text)
    return " ".join(capitalize(word, strict=False) for word in words(text))


def starts_with(text, target, position=0):
    """
    Checks if `text` starts with a given target string.

    Args:
        text (str): String to check.
        target (str): String to check for.
        position (int, optional): Position to search from. Defaults to beginning of `text`.

    Returns:
        bool: Whether `text` starts with `target`.

    Example:

        >>> starts_with('abcdef', 'a')
        True
        >>> starts_with('abcdef', 'b')
        False
        >>> starts_with('abcdef', 'a', 1)
        False

    .. versionadded:: 1.1.0
    """
    text = pyd.to_string(text)
    target = pyd.to_string(target)
    return text[position:].startswith(target)


def strip_tags(text):
    """
    Removes all HTML tags from `text`.

    Args:
        text (str): String to strip.

    Returns:
        str: String without HTML tags.

    Example:

        >>> strip_tags('<a href="#">Some link</a>')
        'Some link'

    .. versionadded:: 3.0.0
    """
    return reg_exp_replace(text, r"<\/?[^>]+>", "")


def substr_left(text, subtext):
    """
    Searches `text` from left-to-right for `subtext` and returns a substring consisting of the
    characters in `text` that are to the left of `subtext` or all string if no match found.

    Args:
        text (str): String to partition.
        subtext (str): String to search for.

    Returns:
        str: Substring to left of `subtext`.

    Example:

        >>> substr_left('abcdefcdg', 'cd')
        'ab'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    return text.partition(subtext)[0] if subtext else text


def substr_left_end(text, subtext):
    """
    Searches `text` from right-to-left for `subtext` and returns a substring consisting of the
    characters in `text` that are to the left of `subtext` or all string if no match found.

    Args:
        text (str): String to partition.
        subtext (str): String to search for.

    Returns:
        str: Substring to left of `subtext`.

    Example:

        >>> substr_left_end('abcdefcdg', 'cd')
        'abcdef'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    return text.rpartition(subtext)[0] or text if subtext else text


def substr_right(text, subtext):
    """
    Searches `text` from right-to-left for `subtext` and returns a substring consisting of the
    characters in `text` that are to the right of `subtext` or all string if no match found.

    Args:
        text (str): String to partition.
        subtext (str): String to search for.

    Returns:
        str: Substring to right of `subtext`.

    Example:

        >>> substr_right('abcdefcdg', 'cd')
        'efcdg'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    return text.partition(subtext)[2] or text if subtext else text


def substr_right_end(text, subtext):
    """
    Searches `text` from left-to-right for `subtext` and returns a substring consisting of the
    characters in `text` that are to the right of `subtext` or all string if no match found.

    Args:
        text (str): String to partition.
        subtext (str): String to search for.

    Returns:
        str: Substring to right of `subtext`.

    Example:

        >>> substr_right_end('abcdefcdg', 'cd')
        'g'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    return text.rpartition(subtext)[2] if subtext else text


def successor(char):
    """
    Return the successor character of `char`.

    Args:
        char (str): Character to find the successor of.

    Returns:
        str: Successor character.

    Example:

        >>> successor('b')
        'c'
        >>> successor('B')
        'C'
        >>> successor('2')
        '3'

    .. versionadded:: 3.0.0
    """
    char = pyd.to_string(char)
    return chr(ord(char) + 1)


def surround(text, wrapper):
    """
    Surround a string with another string.

    Args:
        text (str): String to surround with `wrapper`.
        wrapper (str): String by which `text` is to be surrounded.

    Returns:
        str: Surrounded string.

    Example:

        >>> surround('abc', '"')
        '"abc"'
        >>> surround('abc', '!')
        '!abc!'

    .. versionadded:: 2.4.0
    """
    return "{1}{0}{1}".format(pyd.to_string(text), pyd.to_string(wrapper))


def swap_case(text):
    """
    Swap case of `text` characters.

    Args:
        text (str): String to swap case.

    Returns:
        str: String with swapped case.

    Example:

        >>> swap_case('aBcDeF')
        'AbCdEf'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    return text.swapcase()


def title_case(text):
    """
    Convert `text` to title case.

    Args:
        text (str): String to convert.

    Returns:
        str: String converted to title case.

    Example:

        >>> title_case("bob's shop")
        "Bob's Shop"

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    # NOTE: Can't use text.title() since it doesn't handle apostrophes.
    return " ".join(word.capitalize() for word in re.split(" ", text))


def to_lower(text):
    """
    Converts the given :attr:`text` to lower text.

    Args:
        text (str): String to convert.

    Returns:
        str: String converted to lower case.

    Example:

        >>> to_lower('--Foo-Bar--')
        '--foo-bar--'
        >>> to_lower('fooBar')
        'foobar'
        >>> to_lower('__FOO_BAR__')
        '__foo_bar__'

    .. versionadded:: 4.0.0
    """
    return to_string(text).lower()


def to_upper(text):
    """
    Converts the given :attr:`text` to upper text.

    Args:
        text (str): String to convert.

    Returns:
        str: String converted to upper case.

    Example:

        >>> to_upper('--Foo-Bar--')
        '--FOO-BAR--'
        >>> to_upper('fooBar')
        'FOOBAR'
        >>> to_upper('__FOO_BAR__')
        '__FOO_BAR__'

    .. versionadded:: 4.0.0
    """
    return to_string(text).upper()


def trim(text, chars=None):
    r"""
    Removes leading and trailing whitespace or specified characters from `text`.

    Args:
        text (str): String to trim.
        chars (str, optional): Specific characters to remove.

    Returns:
        str: Trimmed string.

    Example:

        >>> trim('  abc efg\r\n ')
        'abc efg'

    .. versionadded:: 1.1.0
    """
    # pylint: disable=redefined-outer-name
    text = pyd.to_string(text)
    return text.strip(chars)


def trim_end(text, chars=None):
    r"""
    Removes trailing whitespace or specified characters from `text`.

    Args:
        text (str): String to trim.
        chars (str, optional): Specific characters to remove.

    Returns:
        str: Trimmed string.

    Example:

        >>> trim_end('  abc efg\r\n ')
        '  abc efg'

    .. versionadded:: 1.1.0

    .. versionchanged:: 4.0.0
        Renamed from ``trim_right`` to ``trim_end``.
    """
    text = pyd.to_string(text)
    return text.rstrip(chars)


def trim_start(text, chars=None):
    r"""
    Removes leading  whitespace or specified characters from `text`.

    Args:
        text (str): String to trim.
        chars (str, optional): Specific characters to remove.

    Returns:
        str: Trimmed string.

    Example:

        >>> trim_start('  abc efg\r\n ')
        'abc efg\r\n '

    .. versionadded:: 1.1.0

    .. versionchanged:: 4.0.0
        Renamed from ``trim_left`` to ``trim_start``.
    """
    text = pyd.to_string(text)
    return text.lstrip(chars)


def truncate(text, length=30, omission="...", separator=None):
    """
    Truncates `text` if it is longer than the given maximum string length. The last characters of
    the truncated string are replaced with the omission string which defaults to ``...``.

    Args:
        text (str): String to truncate.
        length (int, optional): Maximum string length. Defaults to ``30``.
        omission (str, optional): String to indicate text is omitted.
        separator (mixed, optional): Separator pattern to truncate to.

    Returns:
        str: Truncated string.

    Example:

        >>> truncate('hello world', 5)
        'he...'
        >>> truncate('hello world', 5, '..')
        'hel..'
        >>> truncate('hello world', 10)
        'hello w...'
        >>> truncate('hello world', 10, separator=' ')
        'hello...'

    .. versionadded:: 1.1.0

    .. versionchanged:: 4.0.0
        Removed alias ``trunc``.
    """
    text = pyd.to_string(text)

    if len(text) <= length:
        return text

    omission_len = len(omission)
    text_len = length - omission_len
    text = text[:text_len]

    trunc_len = len(text)

    if pyd.is_string(separator):
        trunc_len = text.rfind(separator)
    elif pyd.is_reg_exp(separator):
        last = None
        for match in separator.finditer(text):
            last = match

        if last is not None:
            trunc_len = last.start()

    return text[:trunc_len] + omission


def unescape(text):
    """
    The inverse of :func:`escape`. This method converts the HTML entities ``&amp;``, ``&lt;``,
    ``&gt;``, ``&quot;``, ``&#39;``, and ``&#96;`` in `text` to their corresponding characters.

    Args:
        text (str): String to unescape.

    Returns:
        str: HTML unescaped string.

    Example:

        >>> results = unescape('&quot;1 &gt; 2 &amp;&amp; 3 &lt; 4&quot;')
        >>> results == '"1 > 2 && 3 < 4"'
        True

    .. versionadded:: 1.0.0

    .. versionchanged:: 1.1.0
        Moved to :mod:`pydash.strings`.
    """
    text = pyd.to_string(text)
    return html_unescape(text)


def upper_case(text):
    """
    Converts string to upper case, as space separated words.

    Args:
        text (str): String to be converted to uppercase.

    Returns:
        str: String converted to uppercase, as space separated words.

    Example:

        >>> upper_case('--foo-bar--')
        'FOO BAR'
        >>> upper_case('fooBar')
        'FOO BAR'
        >>> upper_case('/?*Foo10/;"B*Ar')
        'FOO 10 B AR'

    .. versionadded:: 4.0.0
    """
    return " ".join(words(text)).upper()


def upper_first(text):
    """
    Converts the first character of string to upper case.

    Args:
        text (str): String passed in by the user.

    Returns:
        str: String in which the first character is converted to upper case.

    Example:

        >>> upper_first('fred')
        'Fred'
        >>> upper_first('foo bar')
        'Foo bar'
        >>> upper_first('1foobar')
        '1foobar'
        >>> upper_first(';foobar')
        ';foobar'

    .. versionadded:: 4.0.0
    """
    return text[:1].upper() + text[1:]


def unquote(text, quote_char='"'):
    """
    Unquote `text` by removing `quote_char` if `text` begins and ends with it.

    Args:
        text (str): String to unquote.

    Returns:
        str: Unquoted string.

    Example:

        >>> unquote('"abc"')
        'abc'
        >>> unquote('"abc"', '#')
        '"abc"'
        >>> unquote('#abc', '#')
        '#abc'
        >>> unquote('#abc#', '#')
        'abc'

    .. versionadded:: 3.0.0
    """
    text = pyd.to_string(text)
    inner = text[1:-1]

    if text == "{0}{1}{0}".format(quote_char, inner):
        text = inner

    return text


def url(*paths, **params):
    """
    Combines a series of URL paths into a single URL. Optionally, pass in keyword arguments to
    append query parameters.

    Args:
        paths (str): URL paths to combine.

    Keyword Args:
        params (str, optional): Query parameters.

    Returns:
        str: URL string.

    Example:

        >>> link = url('a', 'b', ['c', 'd'], '/', q='X', y='Z')
        >>> path, params = link.split('?')
        >>> path == 'a/b/c/d/'
        True
        >>> set(params.split('&')) == set(['q=X', 'y=Z'])
        True

    .. versionadded:: 2.2.0
    """
    paths = pyd.chain(paths).flatten_deep().map(pyd.to_string).value()
    paths_list = []
    params_list = flatten_url_params(params)

    for path in paths:
        scheme, netloc, path, query, fragment = urlsplit(path)
        query = parse_qsl(query)
        params_list += query
        paths_list.append(urlunsplit((scheme, netloc, path, "", fragment)))

    path = delimitedpathjoin("/", *paths_list)
    scheme, netloc, path, query, fragment = urlsplit(path)
    query = urlencode(params_list)

    return urlunsplit((scheme, netloc, path, query, fragment))


def words(text, pattern=None):
    """
    Return list of words contained in `text`.

    Args:
        text (str): String to split.
        pattern (str, optional): Custom pattern to split words on. Defaults to ``None``.

    Returns:
        list: List of words.

    Example:

        >>> words('a b, c; d-e')
        ['a', 'b', 'c', 'd', 'e']
        >>> words('fred, barney, & pebbles', '/[^, ]+/g')
        ['fred', 'barney', '&', 'pebbles']

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.2.0
        Added `pattern` argument.

    .. versionchanged:: 3.2.0
        Improved matching for one character words.
    """
    return reg_exp_js_match(text, pattern or RE_WORDS)


#
# Utility functions not a part of main API
#


def js_to_py_re_find(reg_exp):
    """Return Python regular expression matching function based on Javascript style regexp."""
    pattern, options = reg_exp[1:].rsplit("/", 1)
    flags = re.I if "i" in options else 0

    def find(text):
        if "g" in options:
            results = re.findall(pattern, text, flags=flags)
        else:
            results = re.search(pattern, text, flags=flags)

            if results:
                results = [results.group()]
            else:
                results = []

        return results

    return find


def js_to_py_reg_exp_replace(reg_exp):
    """Return Python regular expression substitution function based on Javascript style regexp."""
    pattern, options = reg_exp[1:].rsplit("/", 1)
    count = 0 if "g" in options else 1
    ignore_case = "i" in options

    def _replace(text, repl):
        return reg_exp_replace(text, pattern, repl, ignore_case=ignore_case, count=count)

    return _replace


def delimitedpathjoin(delimiter, *paths):
    """
    Join delimited path using specified delimiter.

    >>> assert delimitedpathjoin('.', '') == ''
    >>> assert delimitedpathjoin('.', '.') == '.'
    >>> assert delimitedpathjoin('.', ['', '.a']) == '.a'
    >>> assert delimitedpathjoin('.', ['a', '.']) == 'a.'
    >>> assert delimitedpathjoin('.', ['', '.a', '', '', 'b']) == '.a.b'
    >>> ret = '.a.b.c.d.e.'
    >>> assert delimitedpathjoin('.', ['.a.', 'b.', '.c', 'd', 'e.']) == ret
    >>> assert delimitedpathjoin('.', ['a', 'b', 'c']) == 'a.b.c'
    >>> ret = 'a.b.c.d.e.f'
    >>> assert delimitedpathjoin('.', ['a.b', '.c.d.', '.e.f']) == ret
    >>> ret = '.a.b.c.1.'
    >>> assert delimitedpathjoin('.', '.', 'a', 'b', 'c', 1, '.') == ret
    >>> assert delimitedpathjoin('.', []) == ''
    """
    paths = [pyd.to_string(path) for path in pyd.flatten_deep(paths) if path]

    if len(paths) == 1:
        # Special case where there's no need to join anything. Doing this because if
        # path==[delimiter], then an extra delimiter would be added if the else clause ran instead.
        path = paths[0]
    else:
        leading = delimiter if paths and paths[0].startswith(delimiter) else ""
        trailing = delimiter if paths and paths[-1].endswith(delimiter) else ""
        middle = delimiter.join([path.strip(delimiter) for path in paths if path.strip(delimiter)])
        path = "".join([leading, middle, trailing])

    return path


def flatten_url_params(params):
    """
    Flatten URL params into list of tuples. If any param value is a list or tuple, then map each
    value to the param key.

    >>> params = [('a', 1), ('a', [2, 3])]
    >>> assert flatten_url_params(params) == [('a', 1), ('a', 2), ('a', 3)]
    >>> params = {'a': [1, 2, 3]}
    >>> assert flatten_url_params(params) == [('a', 1), ('a', 2), ('a', 3)]
    """
    if isinstance(params, dict):
        params = list(iteritems(params))

    flattened = []
    for param, value in params:
        if isinstance(value, (list, tuple)):
            flattened += zip([param] * len(value), value)
        else:
            flattened.append((param, value))

    return flattened
