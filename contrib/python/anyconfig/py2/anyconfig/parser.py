#
# Copyright (C) 2011 - 2013 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
"""Misc parsers
"""
from __future__ import absolute_import

import re


INT_PATTERN = re.compile(r"^(\d|([1-9]\d+))$")
BOOL_PATTERN = re.compile(r"^(true|false)$", re.I)
STR_PATTERN = re.compile(r"^['\"](.*)['\"]$")


def parse_single(str_):
    """
    Very simple parser to parse expressions represent some single values.

    :param str_: a string to parse
    :return: Int | Bool | String

    >>> parse_single(None)
    ''
    >>> parse_single("0")
    0
    >>> parse_single("123")
    123
    >>> parse_single("True")
    True
    >>> parse_single("a string")
    'a string'
    >>> parse_single('"a string"')
    'a string'
    >>> parse_single("'a string'")
    'a string'
    >>> parse_single("0.1")
    '0.1'
    >>> parse_single("    a string contains extra whitespaces     ")
    'a string contains extra whitespaces'
    """
    if str_ is None:
        return ''

    str_ = str_.strip()

    if not str_:
        return ''

    if BOOL_PATTERN.match(str_) is not None:
        return bool(str_)

    if INT_PATTERN.match(str_) is not None:
        return int(str_)

    if STR_PATTERN.match(str_) is not None:
        return str_[1:-1]

    return str_


def parse_list(str_, sep=","):
    """
    Simple parser to parse expressions reprensent some list values.

    :param str_: a string to parse
    :param sep: Char to separate items of list
    :return: [Int | Bool | String]

    >>> parse_list("")
    []
    >>> parse_list("1")
    [1]
    >>> parse_list("a,b")
    ['a', 'b']
    >>> parse_list("1,2")
    [1, 2]
    >>> parse_list("a,b,")
    ['a', 'b']
    """
    return [parse_single(x) for x in str_.split(sep) if x]


def attr_val_itr(str_, avs_sep=":", vs_sep=",", as_sep=";"):
    """
    Atrribute and value pair parser.

    :param str_: String represents a list of pairs of attribute and value
    :param avs_sep: char to separate attribute and values
    :param vs_sep: char to separate values
    :param as_sep: char to separate attributes
    """
    for rel in parse_list(str_, as_sep):
        if avs_sep not in rel or rel.endswith(avs_sep):
            continue

        (_attr, _values) = parse_list(rel, avs_sep)

        if vs_sep in str(_values):
            _values = parse_list(_values, vs_sep)

        if _values:
            yield (_attr, _values)


def parse_attrlist_0(str_, avs_sep=":", vs_sep=",", as_sep=";"):
    """
    Simple parser to parse expressions in the form of
    [ATTR1:VAL0,VAL1,...;ATTR2:VAL0,VAL2,..].

    :param str_: input string
    :param avs_sep:  char to separate attribute and values
    :param vs_sep:  char to separate values
    :param as_sep:  char to separate attributes

    :return:
        a list of tuples of (key, value | [value])
            where key = (Int | String | ...),
            value = (Int | Bool | String | ...) | [Int | Bool | String | ...]

    >>> parse_attrlist_0("a:1")
    [('a', 1)]
    >>> parse_attrlist_0("a:1;b:xyz")
    [('a', 1), ('b', 'xyz')]
    >>> parse_attrlist_0("requires:bash,zsh")
    [('requires', ['bash', 'zsh'])]
    >>> parse_attrlist_0("obsoletes:sysdata;conflicts:sysdata-old")
    [('obsoletes', 'sysdata'), ('conflicts', 'sysdata-old')]
    """
    return [(a, vs) for a, vs in attr_val_itr(str_, avs_sep, vs_sep, as_sep)]


def parse_attrlist(str_, avs_sep=":", vs_sep=",", as_sep=";"):
    """
    Simple parser to parse expressions in the form of
    [ATTR1:VAL0,VAL1,...;ATTR2:VAL0,VAL2,..].

    :param str_: input string
    :param avs_sep:  char to separate attribute and values
    :param vs_sep:  char to separate values
    :param as_sep:  char to separate attributes

    >>> parse_attrlist("requires:bash,zsh")
    {'requires': ['bash', 'zsh']}
    """
    return dict(parse_attrlist_0(str_, avs_sep, vs_sep, as_sep))


def parse(str_, lsep=",", avsep=":", vssep=",", avssep=";"):
    """Generic parser"""
    if avsep in str_:
        return parse_attrlist(str_, avsep, vssep, avssep)
    if lsep in str_:
        return parse_list(str_, lsep)

    return parse_single(str_)

# vim:sw=4:ts=4:et:
