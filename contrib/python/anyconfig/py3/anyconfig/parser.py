#
# Copyright (C) 2011 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Misc simple parsers."""
from __future__ import annotations

import re
import typing
import warnings

if typing.TYPE_CHECKING:
    import collections.abc


INT_PATTERN: re.Pattern = re.compile(r"^(\d|([1-9]\d+))$")
FLOAT_PATTERN: re.Pattern = re.compile(r"^\d+[\.]\d+$")
BOOL_TRUE_PATTERN: re.Pattern = re.compile(r"^true$", re.IGNORECASE)
BOOL_FALSE_PATTERN: re.Pattern = re.compile(r"^false$", re.IGNORECASE)
STR_PATTERN: re.Pattern = re.compile(r"^['\"](.*)['\"]$")

PrimitiveT = typing.Union[str, int, float, bool]
PrimitivesT = list[PrimitiveT]


def parse_single(  # noqa: PLR0911
    str_: str | None,
) -> PrimitiveT:
    """Parse an expression gives a primitive value."""
    if str_ is None:
        return ""

    str_ = str_.strip()

    if not str_:
        return ""

    if BOOL_TRUE_PATTERN.match(str_) is not None:
        return True

    if BOOL_FALSE_PATTERN.match(str_) is not None:
        return False

    if INT_PATTERN.match(str_) is not None:
        return int(str_)

    if FLOAT_PATTERN.match(str_) is not None:
        return float(str_)

    if STR_PATTERN.match(str_) is not None:
        return str_[1:-1]

    return str_


def parse_list(str_: str, sep: str = ",") -> PrimitivesT:
    """Parse an expression gives a list of values.

    An expression ``str_`` might contain a list of str-es separated with
    ``sep``, represents a list of primitive values.
    """
    return [parse_single(x) for x in str_.split(sep) if x]


AttrValsT = tuple[str, typing.Union[PrimitivesT, PrimitiveT]]


def attr_val_itr(
    str_: str, avs_sep: str = ":", vs_sep: str = ",", as_sep: str = ";",
) -> collections.abc.Iterator[AttrValsT]:
    """Parse a list of atrribute and value pairs.

    This is a helper function for parse_attrlist_0.

    :param str_: String represents a list of pairs of attribute and value
    :param avs_sep: char to separate attribute and values
    :param vs_sep: char to separate values
    :param as_sep: char to separate attributes
    """
    for rel in parse_list(str_, as_sep):
        rel = typing.cast("str", rel)
        if avs_sep not in rel or rel.endswith(avs_sep):
            continue

        (_attr, _values, *_rest) = parse_list(rel, avs_sep)

        if _rest:
            warnings.warn(
                f"Extra strings {_rest!s} in {rel!s}"
                f"It should be in the form of attr{avs_sep}value.",
                stacklevel=2,
            )

        _attr = typing.cast("str", _attr)

        if vs_sep in str(_values):
            yield (_attr, parse_list(typing.cast("str", _values), vs_sep))
        elif _values:
            yield (_attr, typing.cast("PrimitiveT", _values))


def parse_attrlist_0(
    str_: str, avs_sep: str = ":", vs_sep: str = ",", as_sep: str = ";",
) -> list[AttrValsT]:
    """Parse a list of atrribute and value pairs.

    This is a helper function for parse_attrlist.

    The expressions to parse should be in the form of
    [ATTR1:VAL0,VAL1,...;ATTR2:VAL0,VAL2,..].

    :param str_: input string
    :param avs_sep:  char to separate attribute and values
    :param vs_sep:  char to separate values
    :param as_sep:  char to separate attributes

    :return:
        a list of tuples of (key, value | [value])
            where key = (Int | String | ...),
            value = (Int | Bool | String | ...) | [Int | Bool | String | ...]
    """
    return list(attr_val_itr(str_, avs_sep, vs_sep, as_sep))


AttrValsDictT = dict[str, typing.Union[PrimitivesT, PrimitiveT]]


def parse_attrlist(str_: str, avs_sep: str = ":", vs_sep: str = ",",
                   as_sep: str = ";") -> AttrValsDictT:
    """Parse a list of atrribute and value pairs.

    The expressions to parse should be in the form of
    [ATTR1:VAL0,VAL1,...;ATTR2:VAL0,VAL2,..].

    :param str_: input string
    :param avs_sep:  char to separate attribute and values
    :param vs_sep:  char to separate values
    :param as_sep:  char to separate attributes
    """
    return dict(parse_attrlist_0(str_, avs_sep, vs_sep, as_sep))


def parse(
    str_: typing.Optional[str], lsep: str = ",", avsep: str = ":",
    vssep: str = ",", avssep: str = ";",
) -> PrimitiveT | PrimitivesT | AttrValsDictT:
    """Very simple generic parser."""
    if str_ is None or not str_:
        return parse_single(str_)

    if avsep in str_:
        return parse_attrlist(str_, avsep, vssep, avssep)
    if lsep in str_:
        return parse_list(str_, lsep)

    return parse_single(str_)
