#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
This module defines common definitions and helper functions for regex subpackage.
"""
from collections.abc import Iterable, Iterator
from typing import Union

CHARACTER_CLASS_ESCAPED: set[int] = {ord(c) for c in r'-|.^?*+{}()[]\\'}
"""Code Points of escaped chars in a character class."""

CodePoint = Union[int, tuple[int, int]]


class RegexError(Exception):
    """
    Error in a regular expression or in a character class specification.
    This exception is derived from `Exception` base class and is raised
    only by the regex subpackage.
    """


def code_point_order(cp: CodePoint) -> int:
    """Ordering function for code points."""
    return cp if isinstance(cp, int) else cp[0]


def code_point_reverse_order(cp: CodePoint) -> int:
    """Reverse ordering function for code points."""
    return cp if isinstance(cp, int) else cp[1] - 1


def iter_code_points(codepoints: Iterable[CodePoint], reverse: bool = False) \
        -> Iterator[CodePoint]:
    """
    Iterates a code points sequence. Three or more consecutive code points are merged in a range.

    :param codepoints: an iterable with code points and code point ranges.
    :param reverse: if `True` reverses the order of the sequence.
    :return: yields code points or code point ranges.
    """
    start_cp = end_cp = 0
    if reverse:
        codepoints = sorted(codepoints, key=code_point_reverse_order, reverse=True)
    else:
        codepoints = sorted(codepoints, key=code_point_order)

    for cp in codepoints:
        if isinstance(cp, int):
            cp0 = cp
            cp1 = cp + 1
        else:
            cp0, cp1 = cp

        if not end_cp:
            start_cp = cp0
            end_cp = cp1
            continue
        elif reverse:
            if start_cp <= cp1:
                if start_cp > cp0:
                    start_cp = cp0
                continue
        elif end_cp >= cp0:
            if end_cp < cp1:
                end_cp = cp1
            continue

        if end_cp > start_cp + 1:
            yield start_cp, end_cp
        else:
            yield start_cp

        start_cp = cp0
        end_cp = cp1
    else:
        if end_cp:
            if end_cp > start_cp + 1:
                yield start_cp, end_cp
            else:
                yield start_cp


def code_point_repr(cp: CodePoint) -> str:
    """
    Returns the string representation of a code point.

    :param cp: an integer or a tuple with at least two integers. \
    Values must be in interval [0, sys.maxunicode].
    """
    if isinstance(cp, int):
        if cp in CHARACTER_CLASS_ESCAPED:
            return r'\%s' % chr(cp)
        return chr(cp)

    if cp[0] in CHARACTER_CLASS_ESCAPED:
        start_char = r'\%s' % chr(cp[0])
    else:
        start_char = chr(cp[0])

    end_cp = cp[1] - 1  # Character ranges include the right bound
    if end_cp in CHARACTER_CLASS_ESCAPED:
        end_char = r'\%s' % chr(end_cp)
    else:
        end_char = chr(end_cp)

    if end_cp > cp[0] + 1:
        return '%s-%s' % (start_char, end_char)
    else:
        return start_char + end_char


def iterparse_character_subset(s: str, expand_ranges: bool = False) -> Iterator[CodePoint]:
    """
    Parses a regex character subset, generating a sequence of code points
    and code points ranges. An unescaped hyphen (-) that is not at the
    start or at the end is interpreted as range specifier.

    :param s: a string representing the character subset.
    :param expand_ranges: if set to `True` then expands character ranges.
    :return: yields integers or couples of integers.
    """
    escaped = False
    on_range = False
    char = ''
    length = len(s)
    subset_index_iterator = iter(range(len(s)))
    for k in subset_index_iterator:
        if k == 0:
            char = s[0]
            if char == '\\':
                escaped = True
            elif char in r'[]' and length > 1:
                raise RegexError("bad character %r at position 0" % char)
            elif expand_ranges:
                yield ord(char)
            elif length <= 2 or s[1] != '-':
                yield ord(char)
        elif s[k] == '-':
            if escaped or (k == length - 1):
                char = s[k]
                yield ord(char)
                escaped = False
            elif on_range:
                char = s[k]
                yield ord(char)
                on_range = False
            else:
                # Parse character range
                on_range = True
                k = next(subset_index_iterator)
                end_char = s[k]
                if end_char == '\\' and (k < length - 1):
                    if s[k + 1] in r'-|.^?*+{}()[]':
                        k = next(subset_index_iterator)
                        end_char = s[k]
                    elif s[k + 1] in r'sSdDiIcCwWpP':
                        msg = "bad character range '%s-\\%s' at position %d: %r"
                        raise RegexError(msg % (char, s[k + 1], k - 2, s))

                if ord(char) > ord(end_char):
                    msg = "bad character range '%s-%s' at position %d: %r"
                    raise RegexError(msg % (char, end_char, k - 2, s))
                elif expand_ranges:
                    yield from range(ord(char) + 1, ord(end_char) + 1)
                else:
                    yield ord(char), ord(end_char) + 1

        elif s[k] in r'|.^?*+{}()':
            if escaped:
                escaped = False
            on_range = False
            char = s[k]
            yield ord(char)
        elif s[k] in r'[]':
            if not escaped and length > 1:
                raise RegexError("bad character %r at position %d" % (s[k], k))
            escaped = on_range = False
            char = s[k]
            if k >= length - 2 or s[k + 1] != '-':
                yield ord(char)
        elif s[k] == '\\':
            if escaped:
                escaped = on_range = False
                char = '\\'
                yield ord(char)
            else:
                escaped = True
        else:
            if escaped:
                escaped = False
                yield ord('\\')
            on_range = False
            char = s[k]
            if k >= length - 2 or s[k + 1] != '-':
                yield ord(char)
    if escaped:
        yield ord('\\')
