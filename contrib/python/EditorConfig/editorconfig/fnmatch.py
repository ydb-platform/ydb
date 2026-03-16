"""Filename matching with shell patterns.

fnmatch(FILENAME, PATTERN) matches according to the local convention.
fnmatchcase(FILENAME, PATTERN) always takes case in account.

The functions operate by translating the pattern into a regular
expression.  They cache the compiled regular expressions for speed.

The function translate(PATTERN) returns a regular expression
corresponding to PATTERN.  (It does not compile it.)

Based on code from fnmatch.py file distributed with Python 2.6.

Licensed under PSF License (see LICENSE.PSF file).

Changes to original fnmatch module:
- translate function supports ``*`` and ``**`` similarly to fnmatch C library
"""

import os
import re
from re import Pattern


__all__ = ["fnmatch", "fnmatchcase", "translate"]

_cache = {}

LEFT_BRACE = re.compile(
    r"""

    (?<! \\ )           # Not preceded by "\"

    \{                  # "{"

    """, re.VERBOSE
)

RIGHT_BRACE = re.compile(
    r"""

    (?<! \\ )           # Not preceded by "\"

    \}                  # "}"

    """, re.VERBOSE
)

NUMERIC_RANGE = re.compile(
    r"""
    (               # Capture a number
        [+-] ?      # Zero or one "+" or "-" characters
        \d +        # One or more digits
    )

    \.\.            # ".."

    (               # Capture a number
        [+-] ?      # Zero or one "+" or "-" characters
        \d +        # One or more digits
    )
    """, re.VERBOSE
)


def fnmatch(name: str, pat: str) -> bool:
    """Test whether FILENAME matches PATTERN.

    Patterns are Unix shell style:

    - ``*``             matches everything except path separator
    - ``**``            matches everything
    - ``?``             matches any single character
    - ``[seq]``         matches any character in seq
    - ``[!seq]``        matches any char not in seq
    - ``{s1,s2,s3}``    matches any of the strings given (separated by commas)

    An initial period in FILENAME is not special.
    Both FILENAME and PATTERN are first case-normalized
    if the operating system requires it.
    If you don't want this, use fnmatchcase(FILENAME, PATTERN).
    """

    name = os.path.normpath(name).replace(os.sep, "/")
    return fnmatchcase(name, pat)


def cached_translate(pat: str) -> tuple[Pattern[str], list[tuple[int, int]]]:
    if not pat in _cache:
        res, num_groups = translate(pat)
        regex = re.compile(res)
        _cache[pat] = regex, num_groups
    return _cache[pat]


def fnmatchcase(name: str, pat: str) -> bool:
    """Test whether FILENAME matches PATTERN, including case.

    This is a version of fnmatch() which doesn't case-normalize
    its arguments.
    """

    regex, num_groups = cached_translate(pat)
    match = regex.match(name)
    if not match:
        return False
    pattern_matched = True
    for (num, (min_num, max_num)) in zip(match.groups(), num_groups):
        if num[0] == '0' or not (min_num <= int(num) <= max_num):
            pattern_matched = False
            break
    return pattern_matched


def translate(pat: str, nested: bool = False) -> tuple[str, list[tuple[int, int]]]:
    """Translate a shell PATTERN to a regular expression.

    There is no way to quote meta-characters.
    """

    index, length = 0, len(pat)  # Current index and length of pattern
    brace_level = 0
    in_brackets = False
    result = ''
    is_escaped = False
    matching_braces = (len(LEFT_BRACE.findall(pat)) ==
                       len(RIGHT_BRACE.findall(pat)))
    numeric_groups = []
    while index < length:
        current_char = pat[index]
        index += 1
        if current_char == '*':
            pos = index
            if pos < length and pat[pos] == '*':
                result += '.*'
            else:
                result += '[^/]*'
        elif current_char == '?':
            result += '[^/]'
        elif current_char == '[':
            if in_brackets:
                result += '\\['
            else:
                pos = index
                has_slash = False
                while pos < length and pat[pos] != ']':
                    if pat[pos] == '/' and pat[pos-1] != '\\':
                        has_slash = True
                        break
                    pos += 1
                if has_slash:
                    result += '\\[' + pat[index:(pos + 1)]
                    index = pos + 1
                else:
                    if index < length and pat[index] in '!^':
                        index += 1
                        result += '[^'
                    else:
                        result += '['
                    in_brackets = True
        elif current_char == '-':
            if in_brackets:
                result += current_char
            else:
                result += '\\' + current_char
        elif current_char == ']':
            if in_brackets and pat[index-2] == '\\':
                result += '\\]'
            else:
                result += current_char
                in_brackets = False
        elif current_char == '{':
            pos = index
            has_comma = False
            while pos < length and (pat[pos] != '}' or is_escaped):
                if pat[pos] == ',' and not is_escaped:
                    has_comma = True
                    break
                is_escaped = pat[pos] == '\\' and not is_escaped
                pos += 1
            if not has_comma and pos < length:
                num_range = NUMERIC_RANGE.match(pat[index:pos])
                if num_range:
                    numeric_groups.append((int(num_range.group(1)), int(num_range.group(2))))
                    result += r"([+-]?\d+)"
                else:
                    inner_result, inner_groups = translate(pat[index:pos],
                                                           nested=True)
                    result += '\\{%s\\}' % (inner_result,)
                    numeric_groups += inner_groups
                index = pos + 1
            elif matching_braces:
                result += '(?:'
                brace_level += 1
            else:
                result += '\\{'
        elif current_char == ',':
            if brace_level > 0 and not is_escaped:
                result += '|'
            else:
                result += '\\,'
        elif current_char == '}':
            if brace_level > 0 and not is_escaped:
                result += ')'
                brace_level -= 1
            else:
                result += '\\}'
        elif current_char == '/':
            if pat[index:(index + 3)] == "**/":
                result += "(?:/|/.*/)"
                index += 3
            else:
                result += '/'
        elif current_char != '\\':
            result += re.escape(current_char)
        if current_char == '\\':
            if is_escaped:
                result += re.escape(current_char)
            is_escaped = not is_escaped
        else:
            is_escaped = False
    if not nested:
        result = r'(?s)%s\Z' % result
    return result, numeric_groups
