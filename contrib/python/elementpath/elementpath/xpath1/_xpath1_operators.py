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
XPath 1.0 implementation - part 2 (operators and expressions)
"""
import math
import decimal
import operator
from collections.abc import Iterator
from copy import copy
from typing import cast

import elementpath.aliases as ta

from elementpath.datatypes import AbstractDateTime, ArithmeticProxy, Duration, NumericProxy
from elementpath.xpath_nodes import XPathNode, ElementNode, DocumentNode

from elementpath.exceptions import ElementPathTypeError
from elementpath.helpers import node_position
from elementpath.xpath_context import XPathSchemaContext
from elementpath.xpath_tokens import XPathToken, NameToken, VariableToken, \
    ContextItemToken, AsteriskToken, ParentShortcutToken

from .xpath1_parser import XPath1Parser

__all__ = ['XPath1Parser']

OPERATORS_MAP = {
    '=': operator.eq,
    '!=': operator.ne,
    '>': operator.gt,
    '>=': operator.ge,
    '<': operator.lt,
    '<=': operator.le,
}

register = XPath1Parser.register
nullary = XPath1Parser.nullary
infix = XPath1Parser.infix
method = XPath1Parser.method

XPath1Parser.symbol_table['$'] = VariableToken
XPath1Parser.symbol_table['*'] = AsteriskToken
XPath1Parser.symbol_table['.'] = ContextItemToken
XPath1Parser.symbol_table['..'] = ParentShortcutToken


###
# Logical Operators
@method(infix('or', bp=20))
def evaluate__or_operator(self: XPathToken, context: ta.ContextType = None) -> bool:
    return self.boolean_value(self[0].select(copy(context))) or \
        self.boolean_value(self[1].select(copy(context)))


@method(infix('and', bp=25))
def evaluate__and_operator(self: XPathToken, context: ta.ContextType = None) -> bool:
    return self.boolean_value(self[0].select(copy(context))) and \
        self.boolean_value(self[1].select(copy(context)))


###
# Comparison operators
@method('=', bp=30)
@method('!=', bp=30)
@method('<', bp=30)
@method('>', bp=30)
@method('<=', bp=30)
@method('>=', bp=30)
def led__comparison_operators(self: XPathToken, left: XPathToken) -> XPathToken:
    if left.symbol in OPERATORS_MAP:
        raise self.wrong_syntax()
    self[:] = left, self.parser.expression(rbp=30)
    return self


@method('=')
@method('!=')
@method('<')
@method('>')
@method('<=')
@method('>=')
def evaluate__comparison_operators(self: XPathToken, context: ta.ContextType = None) -> bool:
    op = OPERATORS_MAP[self.symbol]
    try:
        return any(op(x1, x2) for x1, x2 in self.iter_comparison_data(context))
    except (TypeError, ValueError) as err:
        if isinstance(context, XPathSchemaContext):
            return False
        elif isinstance(err, ElementPathTypeError):
            raise
        elif isinstance(err, TypeError):
            raise self.error('XPTY0004', err) from None
        else:
            raise self.error('FORG0001', err) from None


###
# Numerical operators
@method(infix('+', bp=40))
def evaluate__plus_operator(self: XPathToken, context: ta.ContextType = None) \
        -> ta.OneArithmeticOrEmpty:
    if len(self) == 1:
        arg: ta.NumericType = self.get_argument(context, cls=NumericProxy)
        return [] if arg is None else +arg
    else:
        op1: ta.ArithmeticType | None
        op2: ta.ArithmeticType
        op1, op2 = self.get_operands(context, cls=ArithmeticProxy)
        if op1 is None:
            return []

        try:
            return op1 + op2  # type:ignore[operator, return-value]
        except (TypeError, OverflowError) as err:
            if isinstance(context, XPathSchemaContext):
                return []
            elif isinstance(err, TypeError):
                raise self.error('XPTY0004', err) from None
            elif isinstance(op1, AbstractDateTime):
                raise self.error('FODT0001', err) from None
            elif isinstance(op1, Duration):
                raise self.error('FODT0002', err) from None
            else:
                raise self.error('FOAR0002', err) from None


@method(infix('-', bp=40))
def evaluate__minus_operator(self: XPathToken, context: ta.ContextType = None) \
        -> ta.OneArithmeticOrEmpty:
    if len(self) == 1:
        arg: ta.NumericType = self.get_argument(context, cls=NumericProxy)
        return [] if arg is None else -arg
    else:
        op1: ta.ArithmeticType | None
        op2: ta.ArithmeticType
        op1, op2 = self.get_operands(context, cls=ArithmeticProxy)
        if op1 is None:
            return []

        try:
            return op1 - op2  # type:ignore[operator, return-value]
        except (TypeError, OverflowError) as err:
            if isinstance(context, XPathSchemaContext):
                return []
            elif isinstance(err, TypeError):
                raise self.error('XPTY0004', err) from None
            elif isinstance(op1, AbstractDateTime):
                raise self.error('FODT0001', err) from None
            elif isinstance(op1, Duration):
                raise self.error('FODT0002', err) from None
            else:
                raise self.error('FOAR0002', err) from None


@method('+')
@method('-')
def nud__plus_minus_operators(self: XPathToken) -> XPathToken:
    self[:] = self.parser.expression(rbp=70),
    return self


@method(infix('div', bp=45))
def evaluate__div_operator(self: XPathToken, context: ta.ContextType = None) \
        -> int | float | decimal.Decimal | ta.AnyItemsOrEmpty:
    dividend: ta.ArithmeticType | None
    divisor: ta.ArithmeticType
    dividend, divisor = self.get_operands(context, cls=ArithmeticProxy)
    if dividend is None:
        return []
    elif divisor != 0:
        try:
            if isinstance(dividend, int) and isinstance(divisor, int):
                return decimal.Decimal(dividend) / decimal.Decimal(divisor)
            return dividend / divisor  # type:ignore[operator]
        except TypeError as err:
            raise self.error('XPTY0004', err) from None
        except ValueError as err:
            raise self.error('FOCA0005', err) from None
        except OverflowError as err:
            raise self.error('FOAR0002', err) from None
        except (ZeroDivisionError, decimal.DivisionByZero):
            raise self.error('FOAR0001') from None

    elif isinstance(dividend, AbstractDateTime):
        raise self.error('FODT0001')
    elif isinstance(dividend, Duration):
        raise self.error('FODT0002')
    elif not self.parser.compatibility_mode and \
            isinstance(dividend, (int, decimal.Decimal)) and \
            isinstance(divisor, (int, decimal.Decimal)):
        raise self.error('FOAR0001')
    elif dividend == 0:
        return math.nan
    elif dividend > 0:
        return float('-inf') if str(divisor).startswith('-') else float('inf')
    else:
        return float('inf') if str(divisor).startswith('-') else float('-inf')


@method(infix('mod', bp=45))
def evaluate__mod_operator(self: XPathToken, context: ta.ContextType = None) \
        -> ta.OneArithmeticOrEmpty:
    op1: ta.NumericType | None
    op2: ta.NumericType | None
    op1, op2 = self.get_operands(context, cls=NumericProxy)
    if op1 is None:
        return []
    elif op2 is None:
        raise self.error('XPTY0004', '2nd operand is an empty sequence')
    elif op2 == 0 and isinstance(op2, float):
        return math.nan
    elif math.isinf(op2) and not math.isinf(op1) and op1 != 0:
        return op1 if self.parser.version != '1.0' else math.nan

    try:
        if isinstance(op1, int) and isinstance(op2, int):
            return op1 % op2 if op1 * op2 >= 0 else -(abs(op1) % op2)
        return op1 % op2  # type: ignore[operator]
    except TypeError as err:
        raise self.error('FORG0006', err) from None
    except (ZeroDivisionError, decimal.InvalidOperation):
        raise self.error('FOAR0001') from None


# Resolve the intrinsic ambiguity of some infix operators
@method('or')
@method('and')
@method('div')
@method('mod')
def nud__disambiguation_of_infix_operators(self: XPathToken) -> NameToken:
    return self.as_name()


###
# Union expressions
@method('|', bp=50)
def led__union_operator(self: XPathToken, left: XPathToken) -> XPathToken:
    if left.symbol in ('|', 'union'):
        left.concatenated = True
    self[:] = left, self.parser.expression(rbp=50)
    return self


@method('|')
def select__union_operator(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[XPathNode]:
    if context is None:
        raise self.missing_context()

    results = {item for k in range(2) for item in self[k].select(copy(context))}
    if any(not isinstance(x, XPathNode) for x in results):
        raise self.error('XPTY0004', 'only XPath nodes are allowed')
    elif self.concatenated:
        yield from cast(set[XPathNode], results)
    else:
        yield from cast(list[XPathNode], sorted(results, key=node_position))


###
# Path expressions
@method('//', bp=75)
def nud__descendant_path(self: XPathToken) -> XPathToken:
    if self.parser.next_token.label not in self.parser.PATH_STEP_LABELS:
        self.parser.expected_next(*self.parser.PATH_STEP_SYMBOLS)

    self[:] = self.parser.expression(75),
    return self


@method('/', bp=75)
def nud__child_path(self: XPathToken) -> XPathToken:
    if self.parser.next_token.label not in self.parser.PATH_STEP_LABELS:
        try:
            self.parser.expected_next(*self.parser.PATH_STEP_SYMBOLS)
        except SyntaxError:
            return self

    self[:] = self.parser.expression(75),
    return self


@method('//')
@method('/')
def led__child_or_descendant_path(self: XPathToken, left: XPathToken) -> XPathToken:
    if left.symbol in ('/', '//', ':', '[', '$'):
        pass
    elif left.label not in self.parser.PATH_STEP_LABELS and \
            left.symbol not in self.parser.PATH_STEP_SYMBOLS:
        raise self.wrong_syntax()

    if self.parser.next_token.label not in self.parser.PATH_STEP_LABELS:
        self.parser.expected_next(*self.parser.PATH_STEP_SYMBOLS)

    self[:] = left, self.parser.expression(75)
    return self


@method('/')
def select__child_path(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    """
    Child path expression. Selects child:: axis as default (when bind to '*' or '(name)').
    """
    if context is None:
        raise self.missing_context()
    elif not self:
        if isinstance(context.root, DocumentNode):
            yield context.root
    elif len(self) == 1:
        if isinstance(context.document, DocumentNode):
            context.item = context.document
        elif context.root is None or isinstance(context.root.parent, ElementNode):
            return  # No root or a rooted subtree -> document root produce []
        else:
            context.item = context.root  # A fragment or a schema node
        yield from self[0].select(context)
    else:
        items: set[ta.ItemType] = set()
        for _ in self[0].select_with_focus(context):
            if not isinstance(context.item, XPathNode):
                msg = f"Intermediate step contains an atomic value {context.item!r}"
                raise self.error('XPTY0019', msg)

            for result in self[1].select(context):
                if not isinstance(result, XPathNode):
                    yield result
                elif result in items:
                    pass
                elif isinstance(result, ElementNode):
                    if result.value not in items:
                        items.add(result)
                        yield result
                else:
                    items.add(result)
                    yield result


@method('//')
def select__descendant_path(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    """Operator '//' is a short equivalent to /descendant-or-self::node()/"""
    if context is None:
        raise self.missing_context()
    elif len(self) == 2:
        items: set[ta.ItemType] = set()
        for _ in self[0].select_with_focus(context):
            if not isinstance(context.item, XPathNode):
                raise self.error('XPTY0019')

            for _ in context.iter_descendants():
                for result in self[1].select(context):
                    if not isinstance(result, XPathNode):
                        yield result
                    elif result in items:
                        pass
                    elif isinstance(result, ElementNode):
                        if result.value not in items:
                            items.add(result)
                            yield result
                    else:
                        items.add(result)
                        yield result

    else:
        if isinstance(context.document, DocumentNode):
            context.item = context.document
        elif context.root is None or isinstance(context.root.parent, ElementNode):
            return  # No root or a rooted subtree -> document root produce []
        else:
            context.item = context.root  # A fragment or a schema node

        items = set()
        for _ in context.iter_descendants():
            for result in self[0].select(context):
                if not isinstance(result, XPathNode):
                    items.add(result)
                elif result in items:
                    pass
                elif isinstance(result, ElementNode):
                    if result.value not in items:
                        items.add(result)
                else:
                    items.add(result)

        yield from sorted(items, key=node_position)


###
# Predicate filters
@method('[', bp=80)
def led__predicate(self: XPathToken, left: XPathToken) -> XPathToken:
    self[:] = left, self.parser.expression()
    self.parser.advance(']')
    return self


@method('[')
def select__predicate(self: XPathToken, context: ta.ContextType = None) -> Iterator[ta.ItemType]:
    if context is None:
        raise self.missing_context()

    for _ in self[0].select_with_focus(context):
        if (self[1].label in ('axis', 'kind test') or self[1].symbol == '..') \
                and not isinstance(context.item, XPathNode):
            raise self.error('XPTY0020')

        predicate = list(self[1].select(copy(context)))

        if len(predicate) == 1 and isinstance(predicate[0], NumericProxy):
            if context.position == predicate[0]:
                yield context.item
        elif self.boolean_value(predicate):
            yield context.item


###
# Parenthesized expressions
@method('(', bp=100)
def nud__parenthesized_expr(self: XPathToken) -> XPathToken:
    self[:] = self.parser.expression(),
    self.parser.advance(')')
    return self


@method('(')
def evaluate__parenthesized_expr(self: XPathToken, context: ta.ContextType = None) \
        -> ta.ValueType:
    return self[0].evaluate(context)


@method('(')
def select__parenthesized_expr(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    return self[0].select(context)

# XPath 1.0 definitions continue into module xpath1_functions
