from __future__ import annotations

import ast
import functools
import re
from collections.abc import Generator
from collections.abc import Iterable
from re import Match
from re import Pattern
from typing import Optional

from tokenize_rt import Offset
from tokenize_rt import Token
from tokenize_rt import tokens_to_src

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._string_helpers import curly_escape
from pyupgrade._token_helpers import KEYWORDS
from pyupgrade._token_helpers import remove_brace
from pyupgrade._token_helpers import victims

PercentFormatPart = tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    str,
]
PercentFormat = tuple[str, Optional[PercentFormatPart]]

MAPPING_KEY_RE = re.compile(r'\(([^()]*)\)')
CONVERSION_FLAG_RE = re.compile('[#0+ -]*')
WIDTH_RE = re.compile(r'(?:\*|\d*)')
PRECISION_RE = re.compile(r'(?:\.(?:\*|\d*))?')
LENGTH_RE = re.compile('[hlL]?')


def _must_match(regex: Pattern[str], string: str, pos: int) -> Match[str]:
    match = regex.match(string, pos)
    assert match is not None
    return match


def _parse_percent_format(s: str) -> tuple[PercentFormat, ...]:
    def _parse_inner() -> Generator[PercentFormat]:
        string_start = 0
        string_end = 0
        in_fmt = False

        i = 0
        while i < len(s):
            if not in_fmt:
                try:
                    i = s.index('%', i)
                except ValueError:  # no more % fields!
                    yield s[string_start:], None
                    return
                else:
                    string_end = i
                    i += 1
                    in_fmt = True
            else:
                key_match = MAPPING_KEY_RE.match(s, i)
                if key_match:
                    key: str | None = key_match.group(1)
                    i = key_match.end()
                else:
                    key = None

                conversion_flag_match = _must_match(CONVERSION_FLAG_RE, s, i)
                conversion_flag = conversion_flag_match.group() or None
                i = conversion_flag_match.end()

                width_match = _must_match(WIDTH_RE, s, i)
                width = width_match.group() or None
                i = width_match.end()

                precision_match = _must_match(PRECISION_RE, s, i)
                precision = precision_match.group() or None
                i = precision_match.end()

                # length modifier is ignored
                i = _must_match(LENGTH_RE, s, i).end()

                try:
                    conversion = s[i]
                except IndexError:
                    raise ValueError('end-of-string while parsing format')
                i += 1

                fmt = (key, conversion_flag, width, precision, conversion)
                yield s[string_start:string_end], fmt

                in_fmt = False
                string_start = i

        if in_fmt:
            raise ValueError('end-of-string while parsing format')

    return tuple(_parse_inner())


def _simplify_conversion_flag(flag: str) -> str:
    parts: list[str] = []
    for c in flag:
        if c in parts:
            continue
        c = c.replace('-', '<')
        parts.append(c)
        if c == '<' and '0' in parts:
            parts.remove('0')
        elif c == '+' and ' ' in parts:
            parts.remove(' ')
    return ''.join(parts)


def _percent_to_format(s: str) -> str:
    def _handle_part(part: PercentFormat) -> str:
        s, fmt = part
        s = curly_escape(s)

        if fmt is None:
            return s
        else:
            key, conversion_flag, width, precision, conversion = fmt
            if conversion == '%':
                return s + '%'
            parts = [s, '{']
            if conversion == 's':
                conversion = ''
            if key:
                parts.append(key)
            if conversion in {'r', 'a'}:
                converter = f'!{conversion}'
                conversion = ''
            else:
                converter = ''
            if any((conversion_flag, width, precision, conversion)):
                parts.append(':')
            if conversion_flag:
                parts.append(_simplify_conversion_flag(conversion_flag))
            parts.extend(x for x in (width, precision, conversion) if x)
            parts.extend(converter)
            parts.append('}')
            return ''.join(parts)

    return ''.join(_handle_part(part) for part in _parse_percent_format(s))


def _fix_percent_format_tuple(
        i: int,
        tokens: list[Token],
        *,
        node_right: ast.Tuple,
) -> None:
    # TODO: this is overly timid
    paren = i + 4
    if tokens_to_src(tokens[i + 1:paren + 1]) != ' % (':
        return

    fmt_victims = victims(tokens, paren, node_right, gen=False)
    fmt_victims.ends.pop()

    for index in reversed(fmt_victims.starts + fmt_victims.ends):
        remove_brace(tokens, index)

    newsrc = _percent_to_format(tokens[i].src)
    tokens[i] = tokens[i]._replace(src=newsrc)
    tokens[i + 1:paren] = [Token('Format', '.format'), Token('OP', '(')]


def _fix_percent_format_dict(
        i: int,
        tokens: list[Token],
        *,
        node_right: ast.Dict,
) -> None:
    seen_keys: set[str] = set()
    keys = {}

    for k in node_right.keys:
        # not a string key
        if not isinstance(k, ast.Constant) or not isinstance(k.value, str):
            return
        # duplicate key
        elif k.value in seen_keys:
            return
        # not an identifier
        elif not k.value.isidentifier():
            return
        # a keyword
        elif k.value in KEYWORDS:
            return
        seen_keys.add(k.value)
        keys[ast_to_offset(k)] = k.value

    # TODO: this is overly timid
    brace = i + 4
    if tokens_to_src(tokens[i + 1:brace + 1]) != ' % {':
        return

    fmt_victims = victims(tokens, brace, node_right, gen=False)
    brace_end = fmt_victims.ends[-1]

    key_indices = []
    for j, token in enumerate(tokens[brace:brace_end], brace):
        key = keys.pop(token.offset, None)
        if key is None:
            continue
        # we found the key, but the string didn't match (implicit join?)
        elif ast.literal_eval(token.src) != key:
            return
        # the map uses some strange syntax that's not `'key': value`
        elif tokens[j + 1].src != ':' or tokens[j + 2].src != ' ':
            return
        else:
            key_indices.append((j, key))
    assert not keys, keys

    tokens[brace_end] = tokens[brace_end]._replace(src=')')
    for key_index, s in reversed(key_indices):
        tokens[key_index:key_index + 3] = [Token('CODE', f'{s}=')]
    newsrc = _percent_to_format(tokens[i].src)
    tokens[i] = tokens[i]._replace(src=newsrc)
    tokens[i + 1:brace + 1] = [Token('CODE', '.format'), Token('OP', '(')]


@register(ast.BinOp)
def visit_BinOp(
        state: State,
        node: ast.BinOp,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            not state.settings.keep_percent_format and
            isinstance(node.op, ast.Mod) and
            isinstance(node.left, ast.Constant) and
            isinstance(node.left.value, str)
    ):
        try:
            parsed = _parse_percent_format(node.left.value)
        except ValueError:
            pass
        else:
            for _, fmt in parsed:
                if not fmt:
                    continue
                key, conversion_flag, width, precision, conversion = fmt
                # timid: these require out-of-order parameter consumption
                if width == '*' or precision == '.*':
                    break
                # these conversions require modification of parameters
                if conversion in {'d', 'i', 'u', 'c'}:
                    break
                # timid: py2: %#o formats different from {:#o} (--py3?)
                if '#' in (conversion_flag or '') and conversion == 'o':
                    break
                # no equivalent in format
                if key == '':
                    break
                # timid: py2: conversion is subject to modifiers (--py3?)
                nontrivial_fmt = any((conversion_flag, width, precision))
                if conversion == '%' and nontrivial_fmt:
                    break
                # no equivalent in format
                if conversion in {'a', 'r'} and nontrivial_fmt:
                    break
                # %s with None and width is not supported
                if width and conversion == 's':
                    break
                # all dict substitutions must be named
                if isinstance(node.right, ast.Dict) and not key:
                    break
            else:
                if isinstance(node.right, ast.Tuple):
                    func = functools.partial(
                        _fix_percent_format_tuple,
                        node_right=node.right,
                    )
                    yield ast_to_offset(node), func
                elif isinstance(node.right, ast.Dict):
                    func = functools.partial(
                        _fix_percent_format_dict,
                        node_right=node.right,
                    )
                    yield ast_to_offset(node), func
