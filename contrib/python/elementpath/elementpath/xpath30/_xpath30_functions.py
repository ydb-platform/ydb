#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
XPath 3.0 implementation - part 3 (functions)
"""
import decimal
import os
import re
import codecs
import math
import zoneinfo
from collections.abc import Iterator
from copy import copy
from itertools import zip_longest
from typing import cast, Any, Optional, Union, NoReturn
from urllib.parse import urlsplit
from urllib.request import urlopen
from urllib.error import URLError

import elementpath.aliases as ta

from elementpath.exceptions import ElementPathError
from elementpath.tdop import MultiLabel
from elementpath.helpers import Patterns, is_xml_codepoint, node_position
from elementpath.namespaces import get_expanded_name, split_expanded_name, \
    XPATH_FUNCTIONS_NAMESPACE
from elementpath.datatypes import NumericProxy, QName, Date, DateTime, Time, AnyURI
from elementpath.sequences import xlist
from elementpath.sequence_types import is_sequence_type, match_sequence_type
from elementpath.etree import defuse_xml
from elementpath.xpath_nodes import XPathNode, ElementNode, NamespaceNode, \
    DocumentNode, EtreeElementNode, SchemaElementNode
from elementpath.tree_builders import get_node_tree
from elementpath.xpath_tokens import XPathToken, ValueToken, XPathFunction
from elementpath.serialization import get_serialization_params, serialize_to_xml, \
    serialize_to_json
from elementpath.xpath_context import XPathContext, XPathSchemaContext
from elementpath.regex import translate_pattern, RegexError

from ._xpath30_operators import XPath30Parser
from .xpath30_helpers import UNICODE_DIGIT_PATTERN, DECIMAL_DIGIT_PATTERN, \
    MODIFIER_PATTERN, decimal_to_string, int_to_roman, int_to_alphabetic, \
    format_digits, int_to_words, parse_datetime_picture, parse_datetime_marker, \
    ordinal_suffix

FORMAT_INTEGER_TOKENS = {'A', 'a', 'i', 'I', 'w', 'W', 'Ww'}

DECL_PARAM_PATTERN = re.compile(r'([^\d\W][\w.\-\u00B7\u0300-\u036F\u203F\u2040]*)\s*=\s*')
EXPONENT_PIC = re.compile(r'\d[eE]\d')

register = XPath30Parser.register
method = XPath30Parser.method
function = XPath30Parser.function


###
# 'inline function' expression or 'function test'

class _InlineFunction(XPathFunction):

    symbol = lookup_name = 'function'
    lbp = 90
    rbp = 90
    label: Union[str, MultiLabel] = MultiLabel('inline function', 'function test')

    body: Optional[XPathToken] = None
    "Body of anonymous inline function."

    variables: Optional[dict[str, Any]] = None
    "In-scope variables linked by let and for expressions and arguments."

    varnames: Optional[list[str]] = None
    "Inline function arguments varnames."

    def __str__(self) -> str:
        return str(self.label)

    @property
    def source(self) -> str:
        if self.label == 'function test':
            if len(self.sequence_types) == 1 and self.sequence_types[0] == '*':
                return 'function(*)'
            else:
                return 'function(%s) as %s' % (
                    ', '.join(self.sequence_types[:-1]), self.sequence_types[-1]
                )

        arguments = []
        return_type = ''
        for var, sq in zip_longest(self, self.sequence_types):
            if var is None:
                if sq != 'item()*':
                    return_type = f' as {sq}'
            elif sq is None or sq == 'item()*':
                arguments.append(var.source)
            else:
                arguments.append(f'{var.source} as {sq}')

        return '%s(%s)%s {%s}' % (
            self.symbol,
            ', '.join(arguments),
            return_type,
            getattr(self.body, 'source', '')
        )

    def __call__(self, *args: ta.FunctionArgType,
                 context: Optional[XPathContext] = None) -> Any:

        def get_argument(v: Any) -> Any:
            if isinstance(v, XPathToken) and not isinstance(v, XPathFunction):
                v = v.evaluate(context)

            if isinstance(v, XPathFunction) and sequence_type.startswith('function('):
                if not v.match_function_test(sequence_type, as_argument=True):
                    msg = "argument {!r}: {} does not match sequence type {}"
                    raise self.error('XPTY0004', msg.format(varname, v, sequence_type))

            elif not match_sequence_type(v, sequence_type, self.parser):
                _v = self.cast_to_primitive_type(v, sequence_type)
                if not match_sequence_type(_v, sequence_type, self.parser):
                    msg = "argument '${}': {} does not match sequence type {}"
                    raise self.error('XPTY0004', msg.format(varname, v, sequence_type))
                return _v
            return v

        sequence_type: str
        self.check_arguments_number(len(args))

        context = copy(context)
        if self.variables and context is not None:
            context.variables.update(self.variables)

        if self.varnames is None:
            self.varnames = []

        assert self.body is not None
        if self.label == 'inline partial function':
            k = 0
            for varname, sequence_type, tk in zip(self.varnames, self.sequence_types, self):
                if context is None:
                    raise self.missing_context()

                if tk.symbol != '?' or tk:
                    context.variables[varname] = tk.evaluate(context)
                else:
                    context.variables[varname] = get_argument(args[k])
                    k += 1

            result = self.body.evaluate(context)
        else:
            if context is None:
                raise self.missing_context()
            elif not args and self:
                if isinstance(context.item, DocumentNode):
                    if isinstance(context.root, DocumentNode):
                        context.item = context.root.getroot()
                    elif context.root is not None:
                        context.item = context.root

                args = cast(tuple[ta.FunctionArgType], (context.item,))

            partial_function = False
            if self.variables is None:
                self.variables = {}

            for varname, sequence_type, value in zip(self.varnames, self.sequence_types, args):
                if isinstance(value, XPathToken) and value.symbol == '?':
                    partial_function = True
                else:
                    context.variables[varname] = get_argument(value)

            if partial_function:
                self.to_partial_function()
                return self

            result = self.body.evaluate(context)

        return self.validated_result(result)

    def nud(self) -> Union[XPathFunction, XPathToken]:  # type: ignore[override]
        def append_sequence_type(tk: XPathToken) -> None:
            if tk.symbol == '(' and len(tk) == 1:
                tk = tk[0]

            sequence_type = tk.source
            next_symbol = self.parser.next_token.symbol
            if sequence_type != 'empty-sequence()' and next_symbol in ('*', '+', '?'):
                self.parser.advance()
                sequence_type += next_symbol
                tk.occurrence = next_symbol

            if not is_sequence_type(sequence_type, self.parser):
                if 'xs:NMTOKENS' in sequence_type \
                        or 'xs:ENTITIES' in sequence_type \
                        or 'xs:IDREFS' in sequence_type:
                    msg = "a list type cannot be used in a function signature"
                    raise self.error('XPST0051', msg)
                raise self.error('XPST0003', "a sequence type expected")

            assert isinstance(self.sequence_types, list)
            self.sequence_types.append(sequence_type)

        if self.parser.next_token.symbol != '(':
            return self.as_name()
        self.parser.advance('(')
        self.sequence_types = []

        if self.parser.next_token.symbol in ('$', ')'):
            self.label = 'inline function'
            self.varnames = []

            while self.parser.next_token.symbol != ')':
                self.parser.next_token.expected('$')
                variable = self.parser.expression(5)
                varname = variable[0].value
                assert isinstance(varname, str)
                if varname in self.varnames:
                    raise self.error('XQST0039')

                self.append(variable)
                self.varnames.append(varname)

                if self.parser.next_token.symbol == 'as':
                    self.parser.advance('as')
                    token = self.parser.expression(90)
                    append_sequence_type(token)
                else:
                    self.sequence_types.append('item()*')

                self.parser.next_token.expected(')', ',')
                if self.parser.next_token.symbol == ',':
                    self.parser.advance()
                    self.parser.next_token.unexpected(')')

            self.parser.advance(')')

        elif self.parser.next_token.symbol == '*':
            self.label = 'function test'
            self.append(self.parser.advance('*'))
            self.sequence_types.append('*')
            self.parser.advance(')')
            return self
        else:
            self.label = 'function test'
            while True:
                token = self.parser.parse_sequence_type()
                append_sequence_type(token)
                self.append(token)
                if self.parser.next_token.symbol != ',':
                    break
                self.parser.advance(',')
            self.parser.advance(')')

        # Add function return sequence type
        if self.parser.next_token.symbol != 'as':
            self.sequence_types.append('item()*')
        else:
            self.parser.advance('as')
            if self.parser.next_token.label not in ('kind test', 'sequence type', 'function test'):
                self.parser.expected_next('(name)', ':')

            token = self.parser.expression(rbp=90)
            append_sequence_type(token)

        if self.label == 'inline function':
            if self.parser.next_token.symbol != '{' and not self:
                self.label = 'function test'
            else:
                self.parser.advance('{')
                if self.parser.next_token.symbol != '}':
                    self.body = self.parser.expression()
                elif self.parser.version >= '3.1':
                    self.body = ValueToken(self.parser, value=[])
                else:
                    raise self.wrong_syntax("inline function has an empty body")
                self.parser.advance('}')

        return self

    def evaluate(self, context: ta.ContextType = None) -> ta.ValueType:
        if context is None:
            raise self.missing_context()
        elif self.label.endswith('function'):
            self.variables = context.variables.copy()  # like a closure
            return self

        # A function test
        if not isinstance(context.item, XPathFunction):
            return []
        elif self.source == 'function(*)':
            return context.item
        elif context.item.match_function_test(self.sequence_types):
            return context.item
        else:
            return []

    def to_partial_function(self) -> None:
        assert self.label != 'function test', "an effective inline function required"

        nargs = len([tk and not tk for tk in self._items if tk.symbol == '?'])
        assert nargs, "a partial function requires at least a placeholder token"

        self._name = None  # noqa
        self.label = 'inline partial function'
        self.nargs = nargs


XPath30Parser.symbol_table['function'] = _InlineFunction


###
# Mathematical functions
@method(function('pi', prefix='math', nargs=0, sequence_types=('xs:double',)))
def evaluate__pi(self: XPathFunction, context: ta.ContextType = None) -> float:
    return math.pi


@method(function('exp', prefix='math', nargs=1, sequence_types=('xs:double?', 'xs:double?')))
def evaluate__exp(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    return math.exp(arg)


@method(function('exp10', prefix='math', nargs=1, sequence_types=('xs:double?', 'xs:double?')))
def evaluate__exp10(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    return float(10 ** arg)


@method(function('log', prefix='math', nargs=1, sequence_types=('xs:double?', 'xs:double?')))
def evaluate__log(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType | None = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    return float('-inf') if not arg else math.nan if arg <= -1 else math.log(arg)


@method(function('log10', prefix='math', nargs=1, sequence_types=('xs:double?', 'xs:double?')))
def evaluate__log10(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType | None = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    return float('-inf') if not arg else math.nan if arg <= -1 else math.log10(arg)


@method(function('pow', prefix='math', nargs=2,
                 sequence_types=('xs:double?', 'xs:numeric', 'xs:double?')))
def evaluate__pow(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    if self.context is not None:
        context = self.context

    x = self.get_argument(context, cls=NumericProxy)
    y = self.get_argument(context, index=1, required=True, cls=NumericProxy)
    if x is None:
        return []
    elif not x and y < 0:
        return math.copysign(float('inf'), x) if (y % 2) == 1 else float('inf')

    try:
        return float(x ** y)
    except TypeError:
        return math.nan


@method(function('sqrt', prefix='math', nargs=1,
                 sequence_types=('xs:double?', 'xs:double?')))
def evaluate__sqrt(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType | None = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    elif arg < 0:
        return math.nan
    return math.sqrt(arg)


@method(function('sin', prefix='math', nargs=1,
                 sequence_types=('xs:double?', 'xs:double?')))
def evaluate__sin(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType | None = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    elif math.isinf(arg):
        return math.nan
    return math.sin(arg)


@method(function('cos', prefix='math', nargs=1,
                 sequence_types=('xs:double?', 'xs:double?')))
def evaluate__cos(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType | None = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    elif math.isinf(arg):
        return math.nan
    return math.cos(arg)


@method(function('tan', prefix='math', nargs=1,
                 sequence_types=('xs:double?', 'xs:double?')))
def evaluate__tan(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType | None = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    elif math.isinf(arg):
        return math.nan
    return math.tan(arg)


@method(function('asin', prefix='math', nargs=1,
                 sequence_types=('xs:double?', 'xs:double?')))
def evaluate__asin(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType | None = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    elif arg < -1 or arg > 1:
        return math.nan
    return math.asin(arg)


@method(function('acos', prefix='math', nargs=1,
                 sequence_types=('xs:double?', 'xs:double?')))
def evaluate__acos(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType | None = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    elif arg < -1 or arg > 1:
        return math.nan
    return math.acos(arg)


@method(function('atan', prefix='math', nargs=1,
                 sequence_types=('xs:double?', 'xs:double?')))
def evaluate__atan(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    arg: ta.NumericType | None = self.get_argument(self.context or context, cls=NumericProxy)
    if arg is None:
        return []
    return math.atan(arg)


@method(function('atan2', prefix='math', nargs=2,
                 sequence_types=('xs:double', 'xs:double', 'xs:double')))
def evaluate__atan2(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[float]:
    if self.context is not None:
        context = self.context

    x = self.get_argument(context, cls=NumericProxy)
    y = self.get_argument(context, index=1, required=True, cls=NumericProxy)
    return math.atan2(x, y)


###
# Formatting functions
@method(function('format-integer', nargs=(2, 3),
                 sequence_types=('xs:integer?', 'xs:string', 'xs:string?', 'xs:string')))
def evaluate__format_integer(self: XPathFunction, context: ta.ContextType = None) -> str:
    if self.context is not None:
        context = self.context

    value = self.get_argument(context, cls=NumericProxy)
    picture = self.get_argument(context, index=1, required=True, cls=str)
    lang = self.get_argument(context, index=2, cls=str)
    if value is None:
        return ''

    if ';' not in picture:
        fmt_token, fmt_modifier = picture, ''
    else:
        fmt_token, fmt_modifier = picture.rsplit(';', 1)

    if MODIFIER_PATTERN.match(fmt_modifier) is None:
        raise self.error('FODF1310')

    if not fmt_token:
        raise self.error('FODF1310')
    elif fmt_token in FORMAT_INTEGER_TOKENS:
        if fmt_token == 'a':
            result = int_to_alphabetic(value, lang)
        elif fmt_token == 'A':
            result = int_to_alphabetic(value, lang).upper()
        elif fmt_token == 'i':
            result = int_to_roman(value).lower()
        elif fmt_token == 'I':
            result = int_to_roman(value)
        elif fmt_token == 'w':
            return int_to_words(value, lang, fmt_modifier)
        elif fmt_token == 'W':
            return int_to_words(value, lang, fmt_modifier).upper()
        else:
            return int_to_words(value, lang, fmt_modifier).title()

    else:
        if UNICODE_DIGIT_PATTERN.search(fmt_token) is None:
            if any(not x.isalpha() and not x.isdigit() for x in fmt_token):
                result = str(value)  # fallback for invalid pictures
            else:
                base_char = '1'
                for base_char in fmt_token:
                    if base_char.isalpha():
                        break
                if base_char.islower():
                    result = int_to_alphabetic(value, base_char)
                else:
                    result = int_to_alphabetic(value, base_char.lower()).upper()

        elif DECIMAL_DIGIT_PATTERN.search(fmt_token) is None or ',,' in fmt_token:
            msg = 'picture argument has an invalid primary format token'
            raise self.error('FODF1310', msg)
        else:
            digits = UNICODE_DIGIT_PATTERN.findall(fmt_token)
            cp = ord(digits[0])
            if any((ord(ch) - cp) > 10 for ch in digits[1:]):
                msg = "picture argument mixes digits from different digit families"
                raise self.error('FODF1310', msg)
            elif fmt_token[0].isdigit():
                if '#' in fmt_token:
                    msg = 'picture argument has an invalid primary format token'
                    raise self.error('FODF1310', msg)
            elif fmt_token[0] != '#':
                raise self.error('FODF1310', "invalid grouping in picture argument")

            if digits[0].isdigit():
                cp = ord(digits[0])
                while chr(cp - 1).isdigit():
                    cp -= 1
                digits_family = ''.join(chr(cp + k) for k in range(10))
            else:
                raise ValueError()

            if value < 0:
                result = '-' + format_digits(str(abs(value)), fmt_token, digits_family)
            else:
                result = format_digits(str(abs(value)), fmt_token, digits_family)

    if fmt_modifier.startswith('o'):
        return f'{result}{ordinal_suffix(value)}'
    return result


@method(function('format-number', nargs=(2, 3),
                 sequence_types=('xs:numeric?', 'xs:string', 'xs:string?', 'xs:string')))
def evaluate__format_number(self: XPathFunction, context: ta.ContextType = None) -> str:
    if self.context is not None:
        context = self.context

    value = self.get_argument(context, cls=NumericProxy)
    picture = self.get_argument(context, index=1, required=True, cls=str)
    decimal_format_name = self.get_argument(context, index=2, cls=str)

    # Check and adapt decimal format name
    if decimal_format_name is not None:
        decimal_format_name = decimal_format_name.strip()
        if decimal_format_name.startswith('Q{'):
            if decimal_format_name.startswith('Q{}'):
                decimal_format_name = decimal_format_name[3:]
            else:
                decimal_format_name = decimal_format_name[1:]
        elif ':' in decimal_format_name:
            try:
                decimal_format_name = get_expanded_name(
                    name=decimal_format_name,
                    namespaces=self.parser.namespaces
                )
            except (KeyError, ValueError):
                raise self.error('FODF1280') from None

    try:
        decimal_format = self.parser.decimal_formats[decimal_format_name]
    except KeyError:
        raise self.error('FODF1280') from None

    pattern_separator = decimal_format['pattern-separator']
    sub_pictures = picture.split(pattern_separator)
    if len(sub_pictures) > 2:
        raise self.error('FODF1310')

    decimal_separator = decimal_format['decimal-separator']
    if any(p.count(decimal_separator) > 1 for p in sub_pictures):
        raise self.error('FODF1310')

    percent_sign = decimal_format['percent']
    per_mille_sign = decimal_format['per-mille']
    if any(p.count(percent_sign) + p.count(per_mille_sign) > 1 for p in sub_pictures):
        raise self.error('FODF1310')

    zero_digit = decimal_format['zero-digit']
    optional_digit = decimal_format['digit']
    digits_family = ''.join(chr(cp + ord(zero_digit)) for cp in range(10))
    if any(optional_digit not in p and all(x not in p for x in digits_family)
           for p in sub_pictures):
        raise self.error('FODF1310')

    grouping_separator = decimal_format['grouping-separator']
    adjacent_pattern = re.compile(r'[\\%s\\%s]{2}' % (grouping_separator, decimal_separator))
    if any(adjacent_pattern.search(p) for p in sub_pictures):
        raise self.error('FODF1310')

    if any(x.endswith(grouping_separator)
           for s in sub_pictures for x in s.split(decimal_separator)):
        raise self.error('FODF1310')

    active_characters = digits_family + ''.join([
        decimal_separator, grouping_separator, pattern_separator, optional_digit
    ])

    exponent_pattern = None
    exponent_separator = 'e'
    if self.parser.version > '3.0':
        # Check optional exponent spec correctness in each sub-picture
        exponent_separator = decimal_format['exponent-separator']
        _pattern = re.compile(r'(?<=[{0}]){1}[{0}]'.format(
            re.escape(active_characters), exponent_separator
        ))
        for p in sub_pictures:
            for match in _pattern.finditer(p):
                if percent_sign in p or per_mille_sign in p:
                    raise self.error('FODF1310')
                elif any(c not in digits_family for c in p[match.span()[1]-1:]):
                    # detailed check to consider suffix
                    has_suffix = False
                    for ch in p[match.span()[1]-1:]:
                        if ch in digits_family:
                            if has_suffix:
                                raise self.error('FODF1310')
                        elif ch in active_characters:
                            raise self.error('FODF1310')
                        else:
                            has_suffix = True

                exponent_pattern = _pattern

    if exponent_pattern is None:
        if any(EXPONENT_PIC.search(s) for s in sub_pictures):
            raise self.error('FODF1310')

    if value is None or math.isnan(value):
        return f"{decimal_format['NaN']}"
    elif isinstance(value, float):
        value = decimal.Decimal.from_float(value)
    elif not isinstance(value, decimal.Decimal):
        value = decimal.Decimal(value)

    minus_sign = decimal_format['minus-sign']

    prefix: str = ''
    if value >= 0:
        subpic = sub_pictures[0]
    else:
        subpic = sub_pictures[-1]
        if len(sub_pictures) == 1:
            prefix = minus_sign

    for k, ch in enumerate(subpic):
        if ch in active_characters:
            prefix += subpic[:k]
            subpic = subpic[k:]
            break
    else:
        prefix += subpic
        subpic = ''

    if not subpic:
        suffix = ''
    elif subpic[-1] == percent_sign:
        suffix = percent_sign
        subpic = subpic[:-1]

        exponent = value.as_tuple().exponent
        if isinstance(exponent, int) and exponent < 0:
            value *= 100
        else:
            value = decimal.Decimal(int(value) * 100)

    elif subpic[-1] == per_mille_sign:
        suffix = per_mille_sign
        subpic = subpic[:-1]

        exponent = value.as_tuple().exponent
        if isinstance(exponent, int) and exponent < 0:
            value *= 1000
        else:
            value = decimal.Decimal(int(value) * 1000)

    else:
        for k, ch in enumerate(reversed(subpic)):
            if ch in active_characters:
                idx = len(subpic) - k
                suffix = subpic[idx:]
                subpic = subpic[:idx]
                break
        else:
            suffix = subpic
            subpic = ''

    exp_fmt = None
    if exponent_pattern is not None:
        exp_match = exponent_pattern.search(subpic)
        if exp_match is not None:
            exp_fmt = subpic[exp_match.span()[0]+1:]
            subpic = subpic[:exp_match.span()[0]]

    fmt_tokens = subpic.split(decimal_separator)
    if all(not fmt for fmt in fmt_tokens):
        raise self.error('FODF1310')

    if math.isinf(value):
        return f"{prefix}{decimal_format['infinity']}{suffix}"

    # Calculate the exponent value if it's in the sub-picture
    exp_value = 0
    if exp_fmt and value:
        num_digits = 0
        for ch in fmt_tokens[0]:
            if ch in digits_family:
                num_digits += 1

        if abs(value) > 1:
            v = abs(value)
            while v > 10 ** num_digits:
                exp_value += 1
                v /= 10

            # modify empty fractional part to store a digit
            if not num_digits:
                if len(fmt_tokens) == 1:
                    fmt_tokens.append(zero_digit)
                elif not fmt_tokens[-1]:
                    fmt_tokens[-1] = zero_digit

        elif len(fmt_tokens) > 1 and fmt_tokens[-1] and value >= 0:
            v = abs(value) * 10
            while v < 10 ** num_digits:
                exp_value -= 1
                v *= 10
        else:
            v = abs(value) * 10
            while v < 10:
                exp_value -= 1
                v *= 10

        if exp_value:
            value = value * decimal.Decimal(10) ** -exp_value

    # round the value by fractional part
    if len(fmt_tokens) == 1 or not fmt_tokens[-1]:
        exp = decimal.Decimal('1')
    else:
        k = -1
        for ch in fmt_tokens[-1]:
            if ch in digits_family or ch == optional_digit:
                k += 1
        exp = decimal.Decimal('.' + '0' * k + '1')

    try:
        if value > 0:
            value = value.quantize(exp, rounding='ROUND_HALF_UP')
        else:
            value = value.quantize(exp, rounding='ROUND_HALF_DOWN')
    except decimal.InvalidOperation:
        pass  # number too large, don't round elementpath..

    chunks = decimal_to_string(value).lstrip('-').split('.')
    kwargs = {
        'digits_family': digits_family,
        'optional_digit': optional_digit,
        'grouping_separator': grouping_separator,
    }
    result = format_digits(chunks[0], fmt_tokens[0], **kwargs)

    if len(fmt_tokens) > 1 and fmt_tokens[-1]:
        has_optional_digit = False
        for ch in fmt_tokens[-1]:
            if ch == optional_digit:
                has_optional_digit = True
            elif ch.isdigit() and has_optional_digit:
                raise self.error('FODF1310')

        if len(chunks) == 1:
            chunks.append(zero_digit)

        decimal_part = format_digits(chunks[1], fmt_tokens[-1], **kwargs)

        for ch in reversed(fmt_tokens[-1]):
            if ch == optional_digit:
                if decimal_part and decimal_part[-1] == zero_digit:
                    decimal_part = decimal_part[:-1]
            else:
                if not decimal_part:
                    decimal_part = zero_digit
                break

        if decimal_part:
            result += decimal_separator + decimal_part

            if not fmt_tokens[0] and result.startswith(zero_digit):
                result = result.lstrip(zero_digit)

    if exp_fmt:
        exp_digits = format_digits(str(abs(exp_value)), exp_fmt, **kwargs)
        if exp_value >= 0:
            result += f'{exponent_separator}{exp_digits}'
        else:
            result += f'{exponent_separator}-{exp_digits}'

    return prefix + result + suffix


function('format-dateTime', nargs=(2, 5),
         sequence_types=('xs:dateTime?', 'xs:string', 'xs:string?',
                         'xs:string?', 'xs:string?', 'xs:string?'))
function('format-date', nargs=(2, 5),
         sequence_types=('xs:date?', 'xs:string', 'xs:string?',
                         'xs:string?', 'xs:string?', 'xs:string?'))
function('format-time', nargs=(2, 5),
         sequence_types=('xs:time?', 'xs:string', 'xs:string?',
                         'xs:string?', 'xs:string?', 'xs:string?'))


@method('format-dateTime')
@method('format-date')
@method('format-time')
def evaluate__format_date_time(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[str]:
    cls: type[Union[DateTime, Date, Time]]
    if self.symbol == 'format-dateTime':
        cls = DateTime
        invalid_markers = ''
    elif self.symbol == 'format-date':
        cls = Date
        invalid_markers = 'HhPmsf'
    else:
        cls = Time
        invalid_markers = 'YMDdFWwCE'

    if self.context is not None:
        context = self.context

    value = self.get_argument(context, cls=cls)
    picture = self.get_argument(context, index=1, required=True, cls=str)
    if len(self) not in [2, 5]:
        raise self.error('XPST0017')
    language = self.get_argument(context, index=2, cls=str)
    calendar = self.get_argument(context, index=3, cls=str)
    place = self.get_argument(context, index=4, cls=str)

    if value is None:
        return ''

    try:
        literals, markers = parse_datetime_picture(picture)
    except ElementPathError as err:
        err.token = self
        raise

    if invalid_markers:
        for mrk in markers:
            if mrk[1] in invalid_markers:
                msg = 'Invalid date formatting component {!r}'.format(mrk)
                raise self.error('FOFD1350', msg)

    result = []
    if language not in ('en', 'it', None):
        language = 'en'
        result.append('[Language: en')

    if calendar is not None:
        if calendar.startswith('Q{}'):
            calendar = calendar[3:]

        if calendar not in ('AD', 'ISO', 'OS'):
            if context is None or calendar != context.default_calendar:
                if QName.is_valid(calendar):
                    if ':' not in calendar:
                        msg = f'unknown calendar in no namespace {calendar!r}'
                        raise self.error('FOFD1340', msg)

                    try:
                        _ = get_expanded_name(calendar, self.parser.namespaces)
                    except (KeyError, ValueError) as err:
                        raise self.error('FOFD1340', str(err)) from None

                elif Patterns.extended_qname.search(calendar) is None:
                    raise self.error('FOFD1340', f'Invalid calendar argument {calendar!r}')
            else:
                result.append('[' if not result else ', ')
                result.append('Calendar: AD')

    if place is not None and zoneinfo is not None:
        try:
            zone = zoneinfo.ZoneInfo(place.strip())
        except zoneinfo.ZoneInfoNotFoundError:
            if not isinstance(context, XPathSchemaContext):
                raise self.error('FOFD1340', f'Invalid place argument {place!r}')
        else:
            value = value.astimezone(zone)

    if result:
        result.append(']')

    for k in range(len(markers)):
        result.append(literals[k])
        try:
            result.append(parse_datetime_marker(markers[k], value, language))
        except ElementPathError as err:
            err.token = self
            raise

    result.append(literals[-1])
    return ''.join(result)


###
# String functions that use regular expressions
@method(function('analyze-string', nargs=(2, 3),
                 sequence_types=('xs:string?', 'xs:string', 'xs:string',
                                 'element(fn:analyze-string-result)')))
def evaluate__analyze_string(self: XPathFunction, context: ta.ContextType = None) \
        -> ElementNode:
    if self.context is not None:
        context = self.context

    input_string = self.get_argument(context, default='', cls=str)
    pattern = self.get_argument(context, 1, required=True, cls=str)
    flags = 0
    if len(self) > 2:
        for c in self.get_argument(context, 2, required=True, cls=str):
            if c in 'smix':
                flags |= getattr(re, c.upper())
            elif c == 'q' and self.parser.version > '2':
                pattern = re.escape(pattern)
            else:
                raise self.error('FORX0001', "Invalid regular expression flag %r" % c)

    try:
        python_pattern = translate_pattern(pattern, flags, self.parser.xsd_version)
        compiled_pattern = re.compile(python_pattern, flags=flags)
    except (re.error, RegexError) as err:
        msg = "Invalid regular expression: {}"
        raise self.error('FORX0002', msg.format(str(err))) from None
    except OverflowError as err:
        raise self.error('FORX0002', err) from None

    if compiled_pattern.match('') is not None:
        raise self.error('FORX0003', "pattern matches a zero-length string")

    if context is None:
        raise self.missing_context()

    level = 0
    escaped = False
    char_class = False
    group_levels = [0]
    for s in compiled_pattern.pattern:
        if escaped:
            escaped = False
        elif s == '\\':
            escaped = True
        elif char_class:
            if s == ']':
                char_class = False
        elif s == '[':
            char_class = True
        elif s == '(':
            group_levels.append(level)
            level += 1
        elif s == ')':
            level -= 1

    lines = ['<analyze-string-result xmlns="{}">'.format(XPATH_FUNCTIONS_NAMESPACE)]
    k = 0

    while k < len(input_string):
        match = compiled_pattern.search(input_string, k)
        if match is None:
            lines.append('<non-match>{}</non-match>'.format(input_string[k:]))
            break
        elif not match.groups():
            start, stop = match.span()
            if start > k:
                lines.append('<non-match>{}</non-match>'.format(input_string[k:start]))
            lines.append('<match>{}</match>'.format(input_string[start:stop]))
            k = stop
        else:
            start, stop = match.span()
            if start > k:
                lines.append('<non-match>{}</non-match>'.format(input_string[k:start]))
                k = start

            match_items = []
            group_tmpl = '<group nr="{}">{}'
            empty_group_tmpl = '<group nr="{}"/>'
            unclosed_groups = 0

            for idx in range(1, compiled_pattern.groups + 1):
                _start, _stop = match.span(idx)
                if _start < 0:
                    continue
                elif _start > k:
                    if unclosed_groups:
                        for _ in range(unclosed_groups):
                            match_items.append('</group>')
                        unclosed_groups = 0

                    match_items.append(input_string[k:_start])

                if _start == _stop:
                    if group_levels[idx] <= group_levels[idx - 1]:
                        for _ in range(unclosed_groups):
                            match_items.append('</group>')
                        unclosed_groups = 0
                    match_items.append(empty_group_tmpl.format(idx))
                    k = _stop
                elif idx == compiled_pattern.groups:
                    k = _stop
                    match_items.append(group_tmpl.format(idx, input_string[_start:k]))
                    match_items.append('</group>')
                else:
                    next_start = match.span(idx + 1)[0]
                    if next_start < 0 or _stop < next_start or _stop == next_start \
                            and group_levels[idx + 1] <= group_levels[idx]:
                        k = _stop
                        match_items.append(group_tmpl.format(idx, input_string[_start:k]))
                        match_items.append('</group>')
                    else:
                        k = next_start
                        match_items.append(group_tmpl.format(idx, input_string[_start:k]))
                        unclosed_groups += 1

            for _ in range(unclosed_groups):
                match_items.append('</group>')

            match_items.append(input_string[k:stop])
            k = stop
            lines.append('<match>{}</match>'.format(''.join(match_items)))

    lines.append('</analyze-string-result>')
    if self.parser.defuse_xml:
        root = context.etree.XML(defuse_xml(''.join(lines)))
    else:
        root = context.etree.XML(''.join(lines))

    return cast(ElementNode, get_node_tree(root=root, namespaces=self.parser.namespaces))


###
# Functions and operators on nodes
@method(function('path', nargs=(0, 1), sequence_types=('node()?', 'xs:string?')))
def evaluate__path(self: XPathFunction, context: ta.ContextType = None) -> ta.OneOrEmpty[str]:
    if self.context is not None:
        context = self.context
    elif context is None:
        raise self.missing_context()

    if not self:
        item = context.item
    else:
        item = self.get_argument(context)
        if item is None:
            return []

    if not isinstance(item, XPathNode):
        return []
    elif context.root is None or (root_node := item.root_node) is not context.root:
        # The context has no root or the root is not the root of the item node
        return []
    elif not isinstance(root_node, (DocumentNode, SchemaElementNode)):
        # It's a fragment: add fn:root() to select the root position
        path = item.path[len(root_node.path):]
        return f"Q{{{XPATH_FUNCTIONS_NAMESPACE}}}root(){path}"
    else:
        return item.path or []


@method(function('has-children', nargs=(0, 1), sequence_types=('node()?', 'xs:boolean')))
def evaluate__has_children(self: XPathFunction, context: ta.ContextType = None) -> bool:
    if self.context is not None:
        context = self.context
    elif context is None:
        raise self.missing_context()

    if not self:
        item = context.item
        if not isinstance(item, XPathNode):
            raise self.error('XPTY0004', 'context item must be a node')
    else:
        item = self.get_argument(context)
        if item is None:
            return False
        elif not isinstance(item, XPathNode):
            raise self.error('XPTY0004', 'argument must be a node')

    return isinstance(item, DocumentNode) or \
        isinstance(item, EtreeElementNode) and (len(item.value) > 0 or item.value.text is not None)


@method(function('innermost', nargs=1, sequence_types=('node()*', 'node()*')))
def select__innermost(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[XPathNode]:
    if context is None:
        raise self.missing_context()

    nodes = [e for e in self[0].select(context)]
    if any(not isinstance(x, XPathNode) for x in nodes):
        raise self.error('XPTY0004', 'argument must contain only nodes')

    ancestors = {x for context.item in nodes for x in context.iter_ancestors(axis='ancestor')}
    results = {x for x in nodes if x not in ancestors}
    yield from cast(list[XPathNode], sorted(results, key=node_position))


@method(function('outermost', nargs=1, sequence_types=('node()*', 'node()*')))
def select__outermost(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[XPathNode]:
    if context is None:
        raise self.missing_context()

    nodes: Union[list[ta.ItemType], set[ta.ItemType]]
    nodes = [e for e in self[0].select(context)]
    if any(not isinstance(x, XPathNode) for x in nodes):
        raise self.error('XPTY0004', 'argument must contain only nodes')

    results = set()
    if len(nodes) > 10:
        nodes = set(nodes)

    for item in nodes:
        context.item = item
        ancestors = {x for x in context.iter_ancestors(axis='ancestor')}
        if any(x in nodes for x in ancestors):
            continue
        results.add(item)

    yield from cast(list[XPathNode], sorted(results, key=node_position))


##
# Functions and operators on sequences
@method(function('head', nargs=1, sequence_types=('item()*', 'item()?')))
def evaluate__head(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[ta.ItemType]:
    for item in self[0].select(self.context or context):
        return item
    else:
        return []


@method(function('tail', nargs=1, sequence_types=('item()*', 'item()*')))
def select__tail(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    for k, item in enumerate(self[0].select(self.context or context)):
        if k:
            yield item


@method(function('generate-id', nargs=(0, 1), sequence_types=('node()?', 'xs:string')))
def evaluate__generate_id(self: XPathFunction, context: ta.ContextType = None) -> str:
    arg: ta.NumericType | None
    arg = self.get_argument(self.context or context, default_to_context=True)
    if arg is None:
        return ''
    elif not isinstance(arg, XPathNode):
        if self:
            raise self.error('XPTY0004', "argument is not a node")
        raise self.error('XPTY0004', "context item is not a node")
    else:
        return f'ID{id(arg)}'


@method(function('uri-collection', nargs=(0, 1),
                 sequence_types=('xs:string?', 'xs:anyURI*')))
def evaluate__uri_collection(self: XPathFunction, context: ta.ContextType = None) \
        -> list[AnyURI] | list[NoReturn]:
    if self.context is not None:
        context = self.context

    uri = self.get_argument(context)
    if context is None:
        raise self.missing_context()
    elif isinstance(context, XPathSchemaContext):
        return []
    elif not self or uri is None:
        if context.default_resource_collection is None:
            raise self.error('FODC0002', 'no default resource collection has been defined')
        resource_collection = [AnyURI(context.default_resource_collection)]
    else:
        try:
            AnyURI(uri)
        except ValueError:
            raise self.error('FODC0004', 'invalid argument to fn:uri-collection') from None

        if not context.resource_collections:
            resource_collection = []
        else:
            uri = self.get_absolute_uri(uri)
            try:
                resource_collection = [AnyURI(x) for x in context.resource_collections[uri]]
            except (KeyError, TypeError):
                url_parts = urlsplit(uri)
                if url_parts.scheme in ('', 'file') and \
                        not url_parts.path.startswith(':') and url_parts.path.endswith('/'):
                    raise self.error('FODC0003', 'collection URI is a directory')
                raise self.error('FODC0002', '{!r} collection not found'.format(uri)) from None

    if not match_sequence_type(resource_collection, 'xs:anyURI*', self.parser):
        raise self.error('XPDY0050', "type does not match sequence type xs:anyURI*")

    return resource_collection


@method(function('unparsed-text', nargs=(1, 2),
                 sequence_types=('xs:string?', 'xs:string', 'xs:string?')))
@method(function('unparsed-text-lines', nargs=(1, 2),
                 sequence_types=('xs:string?', 'xs:string', 'xs:string*')))
def evaluate__unparsed_text(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[str]:
    if self.context is not None:
        context = self.context

    href: Optional[str] = self.get_argument(context, cls=str)
    if href is None:
        return []
    elif urlsplit(href).fragment:
        raise self.error('FOUT1170')

    encoding: str
    if len(self) > 1:
        encoding = self.get_argument(context, index=1, required=True, cls=str)
    else:
        encoding = 'UTF-8'

    try:
        uri = self.get_absolute_uri(href)
    except ValueError:
        raise self.error('FOUT1170') from None

    try:
        codecs.lookup(encoding)
    except LookupError:
        raise self.error('FOUT1190') from None

    if context is not None and uri in context.text_resources:
        text = context.text_resources[uri]
    else:
        try:
            with urlopen(uri) as rp:
                stream_reader = codecs.getreader(encoding)(rp)
                text = stream_reader.read()
        except URLError as err:
            raise self.error('FOUT1170', err) from None
        except ValueError as err:
            if len(self) > 1:
                raise self.error('FOUT1190', err) from None

            try:
                with urlopen(uri) as rp:
                    stream_reader = codecs.getreader('UTF-16')(rp)
                    text = stream_reader.read()
            except URLError as err:
                raise self.error('FOUT1170', err) from None
            except ValueError as err:
                raise self.error('FOUT1190', err) from None

        if context is not None:
            context.text_resources[uri] = text

    if not all(is_xml_codepoint(ord(s)) for s in text):
        raise self.error('FOUT1190')

    text = text.lstrip('\ufeff')

    if self.symbol == 'unparsed-text-lines':
        lines = Patterns.xml_newlines.split(text)
        return lines[:-1] if lines[-1] == '' else lines

    return text


@method(function('unparsed-text-available', nargs=(1, 2),
                 sequence_types=('xs:string?', 'xs:string', 'xs:boolean')))
def evaluate__unparsed_text_available(self: XPathFunction, context: ta.ContextType = None) \
        -> bool:
    if self.context is not None:
        context = self.context

    href = self.get_argument(context, cls=str)
    if href is None:
        return False
    elif urlsplit(href).fragment:
        return False

    if len(self) > 1:
        encoding = self.get_argument(context, index=1, required=True, cls=str)
    else:
        encoding = 'UTF-8'

    try:
        uri = self.get_absolute_uri(href)
    except ValueError:
        return False

    try:
        codecs.lookup(encoding)
    except LookupError:
        return False

    try:
        with urlopen(uri) as rp:
            stream_reader = codecs.getreader(encoding)(rp)
            for line in stream_reader:
                if any(not is_xml_codepoint(ord(s)) for s in line):
                    return False
    except URLError:
        return False
    except ValueError:
        if len(self) > 1:
            return False
    else:
        return True

    # Fallback auto-detection with utf-16
    try:
        with urlopen(uri) as rp:
            stream_reader = codecs.getreader('UTF-16')(rp)
            for line in stream_reader:
                if any(not is_xml_codepoint(ord(s)) for s in line):
                    return False
    except (ValueError, URLError):
        return False
    else:
        return True


@method(function('environment-variable', nargs=1,
                 sequence_types=('xs:string', 'xs:string?')))
def evaluate__environment_variable(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[str]:
    if self.context is not None:
        context = self.context

    name: str = self.get_argument(context, required=True, cls=str)
    if context is None:
        raise self.missing_context()
    elif not context.allow_environment:
        return []
    else:
        value = os.environ.get(name)
        return value if value is not None else []


@method(function('available-environment-variables', nargs=0,
                 sequence_types=('xs:string*',)))
def evaluate__available_env_vars(self: XPathFunction, context: ta.ContextType = None) \
        -> list[str] | list[NoReturn]:
    if self.context is not None:
        context = self.context
    elif context is None:
        raise self.missing_context()

    if not context.allow_environment:
        return []
    else:
        return xlist(os.environ)


###
# Parsing and serializing
@method(function('parse-xml', nargs=1,
                 sequence_types=('xs:string?', 'document-node(element(*))?')))
def evaluate__parse_xml(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[DocumentNode]:
    # TODO: resolve relative entity references with static base URI
    if self.context is not None:
        context = self.context

    arg: Optional[str] = self.get_argument(context, cls=str)
    if arg is None:
        return []
    elif context is None:
        raise self.missing_context()

    etree = context.etree
    try:
        if self.parser.defuse_xml:
            root = etree.XML(defuse_xml(arg.encode('utf-8')))
        else:
            root = etree.XML(arg.encode('utf-8'))
    except etree.ParseError:
        raise self.error('FODC0006')
    else:
        return cast(DocumentNode, get_node_tree(etree.ElementTree(root), self.parser.namespaces))


@method(function('parse-xml-fragment', nargs=1,
                 sequence_types=('xs:string?', 'document-node()?')))
def evaluate__parse_xml_fragment(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[DocumentNode]:
    if self.context is not None:
        context = self.context

    arg: Optional[str] = self.get_argument(context, cls=str)
    if arg is None or isinstance(context, XPathSchemaContext):
        return []
    elif context is None:
        raise self.missing_context()

    # Wrap argument in a fake document because an
    # XML document can have only one root element
    if arg.startswith('<?xml '):
        xml_declaration, _, arg = arg[6:].partition('?>')
        xml_params = DECL_PARAM_PATTERN.findall(xml_declaration)
        if 'encoding' not in xml_params:
            raise self.error('FODC0006', "'encoding' argument is mandatory")

        for param in xml_params:
            if param not in ('version', 'encoding'):
                msg = f'unexpected parameter {param!r} in XML declaration'
                raise self.error('FODC0006', msg)

    if arg.lstrip().startswith('<!DOCTYPE'):
        raise self.error('FODC0006', "<!DOCTYPE is not allowed")

    etree = context.etree
    try:
        if self.parser.defuse_xml:
            root = etree.XML(defuse_xml(arg))
        else:
            root = etree.XML(arg)
    except etree.ParseError as err:
        # A not parsable fragment: try to parse including XML data in a dummy element.
        try:
            dummy_element_node = get_node_tree(
                root=etree.XML(f'<document>{arg}</document>'),
                namespaces=self.parser.namespaces
            )
        except etree.ParseError:
            raise self.error('FODC0006', str(err)) from None
        else:
            assert isinstance(dummy_element_node, ElementNode)
            return dummy_element_node.get_document_node(replace=True)
    else:
        return cast(DocumentNode, get_node_tree(
            root=etree.ElementTree(root),
            namespaces=self.parser.namespaces
        ))


@method(function('serialize', nargs=(1, 2), sequence_types=(
        'item()*', 'element(output:serialization-parameters)?', 'xs:string')))
def evaluate__serialize(self: XPathFunction, context: ta.ContextType = None) -> str:
    # TODO full implementation of serialization with
    #  https://www.w3.org/TR/xpath-functions-30/#xslt-xquery-serialization-30
    if self.context is not None:
        context = self.context

    params = self.get_argument(context, index=1) if len(self) == 2 else None
    kwargs = get_serialization_params(params, token=self)

    if context is None:
        raise self.missing_context()
    elif isinstance(context, XPathSchemaContext):
        return ''  # not applicable to schemas

    method_ = kwargs.get('method', 'xml')
    if method_ in ('xml', 'html', 'text'):
        etree_module = context.etree
        if context.namespaces:
            for pfx, uri in context.namespaces.items():
                etree_module.register_namespace(pfx, uri)
        else:
            for pfx, uri in self.parser.namespaces.items():
                etree_module.register_namespace(pfx, uri)

        return serialize_to_xml(self[0].select(context), etree_module, **kwargs)

    elif method_ == 'json':
        return serialize_to_json(self[0].select(context), token=self, **kwargs)
    else:
        return ''


###
# Higher-order functions

@method(function('function-lookup', nargs=2,
                 sequence_types=('xs:QName', 'xs:integer', 'function(*)?')))
def evaluate__function_lookup(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[XPathFunction]:
    if self.context is not None:
        context = self.context

    qname: QName = self.get_argument(context, cls=QName, required=True)
    arity: int = self.get_argument(context, index=1, cls=int, required=True)
    if qname.namespace == '':
        return []

    try:
        cls = self.parser.symbol_table[qname.expanded_name]
    except KeyError:
        try:
            cls = self.parser.symbol_table[qname.local_name]
        except KeyError:
            return []

    assert issubclass(cls, XPathFunction)
    try:
        func = cls(self.parser, nargs=arity)
    except TypeError:
        return []

    func.namespace = qname.namespace
    func.context = copy(context)
    return func


@method(function('function-name', nargs=1, sequence_types=('function(*)', 'xs:QName?')))
def evaluate__function_name(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[QName]:
    if self.context is not None:
        context = self.context

    if isinstance(self[0], XPathFunction):
        func = self[0]
    else:
        func = self.get_argument(context)

    if not isinstance(func, XPathFunction):
        raise self.error('XPTY0004', "argument is not a function")
    else:
        name = func.qname
        return [] if name is None else name


@method(function('function-arity', nargs=1, sequence_types=('function(*)', 'xs:integer')))
def evaluate__function_arity(self: XPathFunction, context: ta.ContextType = None) -> int:
    if isinstance(self[0], XPathFunction):
        return self[0].arity

    func: XPathFunction
    func = self.get_argument(self.context or context, cls=XPathFunction, required=True)
    return func.arity


@method(function('for-each', nargs=2,
                 sequence_types=('item()*', 'function(item()) as item()*', 'item()*')))
def select__for_each(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    if self.context is not None:
        context = self.context

    func = self[1][1] if self[1].symbol == ':' else self[1]
    if not isinstance(func, XPathFunction):
        func = self.get_argument(context, index=1, cls=XPathFunction, required=True)
    assert isinstance(func, XPathFunction)

    for item in self[0].select(context):
        result = func(item, context=context)
        if isinstance(result, list):
            yield from result
        else:
            yield result


@method(function('filter', nargs=2,
                 sequence_types=('item()*', 'function(item()) as xs:boolean', 'item()*')))
def select__filter(self: XPathFunction, context: ta.ContextType = None)\
        -> Iterator[ta.ItemType]:
    func = self[1][1] if self[1].symbol == ':' else self[1]
    if not isinstance(func, XPathFunction):
        func = self.get_argument(context, index=1, cls=XPathFunction, required=True)
    assert isinstance(func, XPathFunction)

    if func.nargs == 0:
        raise self.error('XPTY0004', f'invalid number of arguments {func.nargs}')

    for item in self[0].select(context):
        cond = func(item, context=context)
        if not isinstance(cond, bool):
            raise self.error('XPTY0004', 'a single boolean value required')
        if cond:
            yield item


@method(function('fold-left', nargs=3,
                 sequence_types=('item()*', 'item()*',
                                 'function(item()*, item()) as item()*', 'item()*')))
def select__fold_left(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    func = self[2][1] if self[2].symbol == ':' else self[2]
    if not isinstance(func, XPathFunction):
        func = self.get_argument(context, index=2, cls=XPathFunction, required=True)
    assert isinstance(func, XPathFunction)

    if func.arity != 2:
        raise self.error('XPTY0004', "function arity must be 2")

    zero = self.get_argument(context, index=1)

    result = zero
    for item in self[0].select(context):
        result = func(result, item, context=context)

    if isinstance(result, list):
        yield from result
    else:
        yield result


@method(function('fold-right', nargs=3,
                 sequence_types=('item()*', 'item()*',
                                 'function(item()*, item()) as item()*', 'item()*')))
def select__fold_right(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    func = self[2][1] if self[2].symbol == ':' else self[2]
    if not isinstance(func, XPathFunction):
        func = self.get_argument(context, index=2, cls=XPathFunction, required=True)
    assert isinstance(func, XPathFunction)

    if func.arity != 2:
        raise self.error('XPTY0004', "function arity must be 2")

    zero = self.get_argument(context, index=1)

    result = zero
    sequence = [x for x in self[0].select(context)]

    for item in reversed(sequence):
        result = func(item, result, context=context)

    if isinstance(result, list):
        yield from result
    else:
        yield result


@method(function('for-each-pair', nargs=3,
                 sequence_types=('item()*', 'item()*',
                                 'function(item(), item()) as item()*', 'item()*')))
def select__for_each_pair(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    func = self[2][1] if self[2].symbol == ':' else self[2]
    if not isinstance(func, XPathFunction):
        func = self.get_argument(context, index=2, cls=XPathFunction, required=True)

    if not isinstance(func, XPathFunction):
        raise self.error('XPTY0004', "invalid type for 3rd argument {!r}".format(func))
    elif func.arity != 2:
        raise self.error('XPTY0004', "function arity of 3rd argument must be 2")

    for item1, item2 in zip(self[0].select(context), self[1].select(context)):
        result = func(item1, item2, context=context)
        if isinstance(result, list):
            yield from result
        else:
            yield result


@method(function('namespace-node', nargs=0, label='kind test'))
def select__namespace_node_kind_test(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[NamespaceNode]:
    if context is None:
        raise self.missing_context()
    elif isinstance(context.item, NamespaceNode):
        yield context.item
    elif isinstance(context, XPathSchemaContext):
        return  # deprecated for XP20+ and not needed for schema analysis
    elif isinstance(context.item, ElementNode):
        elem = context.item
        for context.item in elem.namespace_nodes:
            yield context.item  # noqa


###
# Redefined or extended functions
XPath30Parser.unregister('data')
XPath30Parser.unregister('document-uri')
XPath30Parser.unregister('nilled')
XPath30Parser.unregister('node-name')
XPath30Parser.unregister('string-join')
XPath30Parser.unregister('round')


@method(function('data', nargs=(0, 1), sequence_types=('item()*', 'xs:anyAtomicType*')))
def select__data(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ta.AtomicType]:
    if self.context is not None:
        context = self.context

    if self:
        yield from self[0].atomization(context)
    elif context is None:
        raise self.missing_context()
    else:
        yield from self.atomize_item(context.item)


@method(function('document-uri', nargs=(0, 1), sequence_types=('node()?', 'xs:anyURI?')))
def evaluate__document_uri(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[AnyURI]:
    if self.context is not None:
        context = self.context
    elif context is None:
        raise self.missing_context()

    arg: ta.NumericType | None = self.get_argument(context, default_to_context=True)
    if isinstance(arg, DocumentNode):
        uri = arg.document_uri
        if uri is not None:
            return AnyURI(uri)
        elif isinstance(context.root, DocumentNode):
            if context.documents:
                for uri, doc in context.documents.items():
                    if doc and doc.document is context.root.document:
                        return AnyURI(uri)
    return []


@method(function('nilled', nargs=(0, 1), sequence_types=('node()?', 'xs:boolean?')))
def evaluate__nilled(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[bool]:
    arg: ta.NumericType | None = self.get_argument(self.context or context, default_to_context=True)
    if arg is None:
        return []
    elif isinstance(arg, XPathNode):
        result = arg.nilled
        return result if result is not None else []
    else:
        raise self.error('XPTY0004', 'an XPath node required')


@method(function('node-name', nargs=(0, 1), sequence_types=('node()?', 'xs:QName?')))
def evaluate__node_name(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[QName]:
    arg: ta.NumericType | None
    arg = self.get_argument(self.context or context, default_to_context=True)
    if arg is None:
        return []
    elif isinstance(arg, XPathNode):
        name = arg.name
        if name is None:
            return []
        elif name.startswith('{'):
            # name is a QName in extended format
            namespace, local_name = split_expanded_name(name)
            if not namespace:
                return QName('', local_name)

            for pfx, uri in self.parser.namespaces.items():
                if uri == namespace:
                    if not pfx:
                        return QName(uri, local_name)
                    return QName(uri, '{}:{}'.format(pfx, local_name))
            raise self.error('FONS0004', 'no prefix found for namespace {}'.format(namespace))
        else:
            # name is a local name
            return QName(self.parser.namespaces.get('', ''), name)
    else:
        raise self.error('XPTY0004', 'an XPath node required')


@method(function('string-join', nargs=(1, 2),
                 sequence_types=('xs:string*', 'xs:string', 'xs:string')))
def evaluate__string_join(self: XPathFunction, context: ta.ContextType = None) -> str:
    if self.context is not None:
        context = self.context

    items = [
        self.validated_value(s, cls=str, promote=AnyURI, index=k)
        for k, s in enumerate(self[0].atomization(context))
    ]

    if len(self) == 1:
        return ''.join(items)
    separator: str = self.get_argument(context, 1, required=True, cls=str)
    return separator.join(items)


@method(function('round', nargs=(1, 2),
                 sequence_types=('xs:numeric?', 'xs:integer', 'xs:numeric?')))
def evaluate__round(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneNumericOrEmpty:
    if self.context is not None:
        context = self.context

    arg: ta.NumericType | None = self.get_argument(context)
    if arg is None:
        return []
    elif isinstance(arg, XPathNode) or self.parser.compatibility_mode:
        arg = self.number_value(arg)

    if isinstance(arg, float) and (math.isnan(arg) or math.isinf(arg)):
        return arg

    precision: int = self.get_argument(context, index=1, default=0, cls=int)
    try:
        if precision < 0:
            return type(arg)(round(arg, precision))  # type: ignore[call-overload, arg-type]

        number = decimal.Decimal(arg)
        exponent = decimal.Decimal('1') / 10 ** precision
        if number > 0:
            return type(arg)(number.quantize(exponent, rounding='ROUND_HALF_UP'))
        else:
            return type(arg)(number.quantize(exponent, rounding='ROUND_HALF_DOWN'))
    except TypeError as err:
        if isinstance(context, XPathSchemaContext):
            return []
        raise self.error('FORG0006', err) from None
    except decimal.InvalidOperation:
        if isinstance(arg, str):
            if isinstance(context, XPathSchemaContext):
                return []
            raise self.error('XPTY0004') from None
        return round(arg)
    except decimal.DecimalException as err:
        if isinstance(context, XPathSchemaContext):
            return []
        raise self.error('FOCA0002', err) from None
