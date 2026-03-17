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
XPath 3.1 implementation - part 2 (operators and constructors)
"""
from collections.abc import Iterator, Iterable
from typing import cast, Union

import elementpath.aliases as ta

from elementpath.sequence_types import is_sequence_type, match_sequence_type
from elementpath.xpath_tokens import XPathToken, ProxyToken, XPathFunction, \
    XPathMap, XPathArray
from elementpath.sequences import xlist

from .xpath31_parser import XPath31Parser

__all__ = ['XPath31Parser']

register = XPath31Parser.register
method = XPath31Parser.method
function = XPath31Parser.function

register('map', bp=90, label=('kind test', 'map'), bases=(XPathFunction,),
         pattern=r'(?<!\$)\bmap(?=\s*(?:\(\:.*\:\))?\s*(?=\(|\{)(?!\:))')


@method('map')
def nud__map_sequence_type_or_constructor(self: XPathFunction) \
        -> Union[XPathToken, XPathMap, XPathArray]:
    if self.parser.next_token.symbol == '{':
        self.parser.token = XPathMap(self.parser).nud()
        return self.parser.token
    elif self.parser.next_token.symbol != '(':
        return self.as_name()

    self.label = 'kind test'

    self.parser.advance('(')
    if self.parser.next_token.label not in ('kind test', 'sequence type', 'function test'):
        self.parser.expected_next('(name)', ':', '*', message='a QName or a wildcard expected')
    self[:] = self.parser.expression(45),
    self.parser.parse_occurrence(self[0])

    if self[0].symbol != '*':
        self.parser.advance(',')
        if self.parser.next_token.label not in ('kind test', 'sequence type', 'function test'):
            self.parser.expected_next('(name)', ':', '*', message='a QName or a wildcard expected')
        self.append(self.parser.expression(45))
        self.parser.parse_occurrence(self[-1])

    self.parser.advance(')')
    return self


register('array', bp=90, label=('kind test', 'array'), bases=(XPathFunction,),
         pattern=r'(?<!\$)\barray(?=\s*(?:\(\:.*\:\))?\s*(?=\(|\{)(?!\:))')


@method('array')
def nud__sequence_type_or_curly_array_constructor(self: XPathFunction) -> XPathToken:
    if self.parser.next_token.symbol == '{':
        self.parser.token = XPathArray(self.parser).nud()
        return self.parser.token
    elif self.parser.next_token.symbol != '(':
        return self.as_name()

    self.label = 'kind test'
    self.parser.advance('(')
    if self.parser.next_token.label not in ('kind test', 'function test'):
        self.parser.expected_next('(name)', ':', '*', 'item')
    self[:] = self.parser.expression(45),
    if self[0].symbol != '*':
        self.parser.parse_occurrence(self[0])
    self.parser.advance(')')
    self.parser.parse_occurrence(self)
    return self


@method('map')
@method('array')
def select__map_or_array_kind_test(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[Union[XPathMap, XPathArray]]:
    if context is None:
        raise self.missing_context()

    for item in context.iter_children_or_self():
        if match_sequence_type(item, self.source, self.parser):
            yield cast(Union[XPathMap, XPathArray], item)


###
# Square array constructor (pushed lazy)
@method('[')
def nud__square_array_constructor(self: XPathToken) -> XPathToken:
    if self.parser.version < '3.1':
        raise self.wrong_syntax()

    # Constructs an XPathArray token and returns it instead of the predicate
    token = XPathArray(self.parser)
    token.symbol = '['
    if token.parser.next_token.symbol not in (']', '(end)'):
        while True:
            token.append(self.parser.expression(5))
            if token.parser.next_token.symbol != ',':
                break
            token.parser.advance()

    token.parser.advance(']')
    return token


class LookupOperatorToken(XPathToken):
    """
    Question mark symbol is used for XP31+ lookup operator and also for
    placeholder in XP30+ partial functions and for optional occurrences.
    """
    symbol = lookup_name = '?'
    lbp = 85
    rbp = 85

    def __init__(self, parser: ta.XPathParserType, value: ta.AtomicType | None = None) -> None:
        super().__init__(parser, value)
        if self.parser.token.symbol in ('(', ','):
            # It's a placeholder symbol or a unary lookup operator
            # in a list of function arguments.
            self.lbp = self.rbp = 0

    @property
    def source(self) -> str:
        if not self:
            return '?'
        elif len(self) == 1:
            return f'?{self[0].source}'
        else:
            return f'{self[0].source}?{self[1].source}'

    def nud(self) -> 'LookupOperatorToken':
        try:
            self.parser.expected_next('(name)', '(integer)', '(', '*')
        except SyntaxError:
            if self.lbp:
                raise
            return self  # a placeholder/unary lookup token
        else:
            self[:] = self.parser.expression(85),
            return self

    def led(self, left: XPathToken) -> Union['LookupOperatorToken', XPathToken]:
        try:
            self.parser.expected_next('(name)', '(integer)', '(', '*')
        except SyntaxError:
            if isinstance(left.value, str) and is_sequence_type(left.value, self.parser):
                self.lbp = self.rbp = 0
                left.occurrence = '?'
                return left
            raise
        else:
            self[:] = left, self.parser.expression(85)
            return self

    def evaluate(self, context: ta.ContextType = None) -> ta.OneOrMore[ta.ItemType]:
        if not self:
            return self.symbol  # a placeholder token
        return xlist(self.select(context))

    def select(self, context: ta.ContextType = None) -> Iterator[ta.ItemType]:

        # flatten sequences, don't flatten arrays.
        def flatten(v: ta.ValueType) -> Iterator[ta.ItemType]:
            if isinstance(v, list):
                yield from v
            else:
                yield v

        if not self:
            if isinstance(self.value, list):
                yield from self.value
            else:
                yield self.value
            return

        items: Iterable[ta.ItemType]
        if len(self) == 1:
            # unary lookup operator (used in predicates)
            if context is None:
                raise self.missing_context()
            items = (context.item,)
        else:
            items = self[0].select(context)

        for item in items:
            symbol = self[-1].symbol
            if isinstance(item, XPathMap):
                if symbol == '*':
                    for value in item.values(context):
                        yield from flatten(value)

                elif symbol in ('(name)', '(integer)'):
                    yield from flatten(
                        item(cast(Union[str, int], self[-1].value), context=context)
                    )
                elif symbol == '(':
                    for obj in self[-1].select(context):
                        yield from flatten(item(self.data_value(obj), context=context))

            elif isinstance(item, XPathArray):
                if symbol == '*':
                    for value in item.items(context):
                        yield from flatten(value)
                elif symbol == '(name)':
                    raise self.error('XPTY0004')
                elif symbol == '(integer)':
                    yield from flatten(item(cast(int, self[-1].value), context=context))
                elif symbol == '(':
                    for value in self[-1].select(context):
                        yield from flatten(item(self.data_value(value), context=context))

            elif item == ():
                continue
            else:
                raise self.error('XPTY0004')


XPath31Parser.symbol_table['?'] = LookupOperatorToken


@method('=>', bp=67)
def led__arrow_operator(self: XPathToken, left: XPathToken) -> XPathToken:
    next_token = self.parser.next_token
    if next_token.symbol == '$':
        self[:] = left, self.parser.expression(80)
    elif isinstance(next_token, ProxyToken):
        self.parser.parse_arguments = False
        self[:] = left, next_token.nud()
        self.parser.parse_arguments = True
        self.parser.advance()
    elif isinstance(next_token, XPathFunction):
        self[:] = left, next_token
        if next_token.label == 'kind test':
            raise next_token.wrong_syntax()
        self.parser.advance()  # Skip static evaluation of function arguments
    else:
        next_token.expected('(name)', ':', 'Q{', '(')
        self.parser.parse_arguments = False
        self[:] = left, self.parser.expression(80)
        self.parser.parse_arguments = True

    right = self.parser.expression(67)
    right.expected('(')
    self.append(right)
    return self


@method('=>')
def evaluate__arrow_operator(self: XPathToken, context: ta.ContextType = None) \
        -> ta.ValueType:
    tokens = [self[0]]
    if self[2]:
        tokens.extend(self[2][0].get_argument_tokens())
    func = self[1].get_function(context, arity=len(tokens))
    arguments = [tk.evaluate(context) for tk in tokens]
    return func(*arguments, context=context)
