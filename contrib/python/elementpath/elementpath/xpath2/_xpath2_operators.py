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
XPath 2.0 implementation - part 2 (operators, expressions and multi-role tokens)
"""
import math
import operator
from copy import copy
from collections.abc import Iterator
from decimal import Decimal, DivisionByZero
from typing import cast, NoReturn

import elementpath.aliases as ta

from elementpath.protocols import XsdAttributeProtocol
from elementpath.exceptions import ElementPathError
from elementpath.namespaces import XSD_NAMESPACE, XSD_NOTATION, XSD_ANY_ATOMIC_TYPE, XSD_UNTYPED
from elementpath.helpers import numeric_equal, numeric_not_equal, \
    node_position, get_double
from elementpath.namespaces import XSD_ERROR, get_namespace, get_expanded_name
from elementpath.datatypes import UntypedAtomic, QName, AnyURI, \
    Duration, Integer, DoubleProxy10
from elementpath.xpath_nodes import ElementNode, DocumentNode, XPathNode, AttributeNode
from elementpath.sequences import xlist
from elementpath.sequence_types import is_instance
from elementpath.xpath_context import XPathSchemaContext
from elementpath.xpath_tokens import XPathToken, XPathFunction, XPathConstructor

from .xpath2_parser import XPath2Parser

__all__ = ['XPath2Parser']

COMPARISON_OPERATORS = frozenset(('eq', 'ne', 'lt', 'le', 'gt', 'ge'))

register = XPath2Parser.register
infix = XPath2Parser.infix
method = XPath2Parser.method
function = XPath2Parser.function


@method('then')
@method('as')
@method('of')
@method('else')
@method('in')
@method('return')
@method('satisfies')
def nud__auxiliary_symbols(self: XPathToken) -> XPathToken:
    return self.as_name()


###
# Node sequence composition
XPath2Parser.duplicate('|', 'union')


@method(infix('intersect', bp=55))
@method(infix('except', bp=55))
def select__intersect_and_except_operators(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[XPathNode]:
    if context is None:
        raise self.missing_context()

    s1, s2 = set(self[0].select(copy(context))), set(self[1].select(copy(context)))
    if any(not isinstance(x, XPathNode) for x in s1) \
            or any(not isinstance(x, XPathNode) for x in s2):
        raise self.error('XPTY0004', 'only XPath nodes are allowed')

    if self.symbol == 'except':
        yield from cast(list[XPathNode], sorted(s1 - s2, key=node_position))
    else:
        yield from cast(list[XPathNode], sorted(s1 & s2, key=node_position))


###
# 'if' expression
@method('if', bp=20, label='expression')
def nud__if_expression(self: XPathToken) -> XPathToken:
    if self.parser.next_token.symbol != '(':
        return self.as_name()

    self.parser.advance('(')
    self[:] = self.parser.expression(5),
    self.parser.advance(')')
    self.parser.advance('then')
    self[1:] = self.parser.expression(5),
    self.parser.advance('else')
    self[2:] = self.parser.expression(5),
    return self


@method('if')
def evaluate__if_expression(self: XPathToken, context: ta.ContextType = None) \
        -> ta.ValueType:
    if self.boolean_value(self[0].select(copy(context))):
        if isinstance(context, XPathSchemaContext):
            self[2].evaluate(context)
        return self[1].evaluate(context)
    else:
        if isinstance(context, XPathSchemaContext):
            self[1].evaluate(context)
        return self[2].evaluate(context)


@method('if')
def select__if_expression(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    if self.boolean_value(self[0].select(copy(context))):
        if isinstance(context, XPathSchemaContext):
            self[2].evaluate(context)
        yield from self[1].select(context)
    else:
        if isinstance(context, XPathSchemaContext):
            self[1].evaluate(context)
        yield from self[2].select(context)


###
# Quantified expressions
@method('some', bp=20, label='expression')
@method('every', bp=20, label='expression')
def nud__quantified_expressions(self: XPathToken) -> XPathToken:
    del self[:]
    if self.parser.next_token.symbol != '$':
        return self.as_name()

    while True:
        self.parser.next_token.expected('$')
        variable = self.parser.expression(5)
        self.append(variable)
        self.parser.advance('in')
        expr = self.parser.expression(5)
        self.append(expr)
        for tk in filter(lambda x: x.symbol == '$', expr.iter()):
            if tk[0].value == variable[0].value:
                raise tk.error('XPST0008', 'loop variable in its range expression')

        if self.parser.next_token.symbol != ',':
            break
        self.parser.advance()

    self.parser.advance('satisfies')
    self.append(self.parser.expression(5))
    return self


@method('some')
@method('every')
def evaluate__quantified_expressions(self: XPathToken, context: ta.ContextType = None) -> bool:
    if context is None:
        raise self.missing_context()

    context = copy(context)
    context.variables = context.variables.copy()

    some = self.symbol == 'some'
    varnames = [cast(str, self[k][0].value) for k in range(0, len(self) - 1, 2)]
    selectors = [self[k].select for k in range(1, len(self) - 1, 2)]

    for results in context.iter_product(selectors, varnames):
        context.variables.update(x for x in zip(varnames, results))
        if self.boolean_value(self[-1].select(copy(context))):
            if some:
                return True
        elif not some:
            return False

    return not some


###
# 'for' expressions
@method('for', bp=20, label='expression')
def nud__for_expression(self: XPathToken) -> XPathToken:
    del self[:]
    if self.parser.next_token.symbol != '$':
        return self.as_name()

    while True:
        self.parser.next_token.expected('$')
        variable = self.parser.expression(5)
        self.append(variable)
        self.parser.advance('in')
        expr = self.parser.expression(5)
        self.append(expr)
        for tk in filter(lambda x: x.symbol == '$', expr.iter()):
            if tk[0].value == variable[0].value:
                raise tk.error('XPST0008', 'loop variable in its range expression')

        if self.parser.next_token.symbol != ',':
            break
        self.parser.advance()

    self.parser.advance('return')
    self.append(self.parser.expression(5))
    return self


@method('for')
def select__for_expression(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    if context is None:
        raise self.missing_context()

    context = copy(context)
    context.variables = context.variables.copy()

    varnames = [cast(str, self[k][0].value) for k in range(0, len(self) - 1, 2)]
    selectors = [self[k].select for k in range(1, len(self) - 1, 2)]

    for results in copy(context).iter_product(selectors, varnames):
        context.variables.update(x for x in zip(varnames, results))
        yield from self[-1].select(copy(context))


###
# Sequence type based
@method('instance', bp=60, label='expression')
@method('treat', bp=61, label='expression')
def led__sequence_type_based_expressions(self: XPathToken, left: XPathToken) -> XPathToken:
    self.parser.advance('of' if self.symbol == 'instance' else 'as')
    self[:] = left, self.parser.parse_sequence_type()
    return self


@method('instance')
def evaluate__instance_expression(self: XPathToken, context: ta.ContextType = None) -> bool:
    occurs = self[1].occurrence
    position = None

    if self[1].symbol == 'empty-sequence':
        for _ in self[0].select(context):
            return False
        return True
    elif self[1].label in ('kind test', 'sequence type', 'function test'):
        if context is None:
            raise self.missing_context()

        for position, context.item in enumerate(self[0].select(context)):
            if context.axis is None:
                context.axis = 'self'

            result = self[1].evaluate(context)
            if isinstance(result, list) and not result:
                return occurs in ('*', '?') or \
                    isinstance(context.item, XPathFunction) and \
                    context.item.name == XSD_ERROR
            elif position and occurs in ('', '?'):
                return False
        else:
            return position is not None or occurs in ('*', '?')
    else:
        type_name = self[1].source.rstrip('*+?')
        try:
            qname = get_expanded_name(type_name, self.parser.namespaces)
        except KeyError as err:
            raise self.error('XPST0081', "namespace prefix {} not found".format(err))

        for position, item in enumerate(self[0].select(context)):
            try:
                if not is_instance(item, qname, self.parser):
                    return False
            except TypeError as err:
                raise self.error('XPST0051', err) from None
            except KeyError:
                msg = f"atomic type {type_name!r} not found in in-scope schema types"
                raise self.error('XPST0051', msg) from None
            else:
                if position and occurs in ('', '?'):
                    return False
        else:
            return position is not None or occurs in ('*', '?')


@method('treat')
def evaluate__treat_expression(self: XPathToken, context: ta.ContextType = None) \
        -> ta.ValueType:
    occurs = self[1].occurrence
    position = None
    castable_expr: xlist[ta.ItemType] = xlist()
    if self[1].symbol == 'empty-sequence':
        for _ in self[0].select(context):
            raise self.error('XPDY0050')
    elif self[1].label in ('kind test', 'sequence type', 'function test'):
        for position, item in enumerate(self[0].select(context)):
            result = self[1].evaluate(context)
            if not result and isinstance(result, list):
                raise self.error('XPDY0050')
            elif position and occurs in ('', '?'):
                raise self.error('XPDY0050', "more than one item in sequence")
            castable_expr.append(item)
        else:
            if position is None and occurs not in ('*', '?'):
                raise self.error('XPDY0050', "the sequence cannot be empty")
    else:
        type_name = self[1].source.rstrip('*+?')
        try:
            qname = get_expanded_name(type_name, self.parser.namespaces)
        except KeyError as err:
            raise self.error('XPST0081', 'prefix {} not found'.format(str(err)))

        if not qname.startswith('{') and not QName.is_valid(qname):
            raise self.error('XPST0003')

        for position, item in enumerate(self[0].select(context)):
            try:
                if not is_instance(item, qname, self.parser):
                    msg = f"item {item!r} is not of type {type_name!r}"
                    raise self.error('XPDY0050', msg)
            except KeyError:
                msg = f"atomic type {type_name!r} not found in in-scope schema types"
                raise self.error('XPST0051', msg) from None
            else:
                if position and occurs in ('', '?'):
                    raise self.error('XPDY0050', "more than one item in sequence")
                castable_expr.append(item)
        else:
            if position is None and occurs not in ('*', '?'):
                raise self.error('XPDY0050', "the sequence cannot be empty")

    return castable_expr


###
# Simple type based expressions

@method('castable', bp=62, label='expression')
@method('cast', bp=63, label='expression')
def led__cast_expressions(self: XPathToken, left: XPathToken) -> XPathToken:
    self.parser.advance('as')
    self.parser.expected_next('(name)', ':', 'Q{', message='an EQName expected')
    self[:] = left, self.parser.expression(rbp=85)
    if self.parser.next_token.symbol == '?':
        self[1].occurrence = '?'
        self.parser.advance()
    return self


@method('castable')
@method('cast')
def evaluate__cast_expressions(self: XPathToken, context: ta.ContextType = None) \
        -> ta.OneAtomicOrEmpty:
    type_name = self[1].source.rstrip('+*?')
    try:
        atomic_type = get_expanded_name(type_name, self.parser.namespaces)
    except KeyError as err:
        raise self.error('XPST0081', 'prefix {} not found'.format(str(err)))

    if atomic_type in (XSD_NOTATION, XSD_ANY_ATOMIC_TYPE):
        raise self.error('XPST0080')

    namespace = get_namespace(atomic_type)
    if namespace != XSD_NAMESPACE and \
            (self.parser.schema is None or self.parser.schema.get_type(atomic_type) is None):
        msg = f"atomic type {atomic_type!r} not found in the in-scope schema types"
        raise self.error('XPST0051', msg)

    result = [res for res in self[0].select(context)]
    if len(result) > 1:
        if self.symbol != 'cast':
            return False
        raise self.error('XPTY0004', "more than one value in expression")
    elif not result:
        if self[1].occurrence == '?':
            return [] if self.symbol == 'cast' else True
        elif self.symbol != 'cast':
            return False
        else:
            raise self.error('XPTY0004', "an atomic value is required")

    arg = self.data_value(result[0])
    value: ta.OneAtomicOrEmpty
    try:
        if namespace != XSD_NAMESPACE:
            if self.parser.schema is not None:
                value = self.parser.schema.cast_as(self.string_value(arg), atomic_type)
            else:
                value = []
        else:
            local_name = atomic_type.split('}')[1]
            try:
                token_class = cast(type[XPathConstructor], self.parser.symbol_table[local_name])
            except KeyError:
                msg = f"atomic type {type_name!r} not found in the in-scope schema types"
                raise self.error('XPST0051', msg)
            else:
                if token_class.label != 'constructor function':
                    msg = f"token {type_name!r} is not a constructor"
                    raise self.error('XPST0051', msg)

            if local_name == 'QName':
                if isinstance(arg, QName):
                    pass
                elif self.parser.version < '3.0' and self[0].symbol != '(string)':
                    raise self.error('XPTY0004', "Non literal string to QName cast")

            token = token_class(self.parser)
            value = token.cast(arg)

    except ElementPathError:
        if self.symbol != 'cast':
            return False
        elif isinstance(context, XPathSchemaContext):
            return UntypedAtomic('1')
        raise
    except (TypeError, ValueError) as err:
        if self.symbol != 'cast':
            return False
        elif isinstance(context, XPathSchemaContext):
            return UntypedAtomic('1')
        elif isinstance(arg, (UntypedAtomic, str)):
            raise self.error('FORG0001', err) from None
        raise self.error('XPTY0004', err) from None
    else:
        return value if self.symbol == 'cast' else True


###
# Comma operator - concatenate items or sequences
@method(infix(',', bp=5))
def evaluate__comma_operator(self: XPathToken, context: ta.ContextType = None) \
        -> list[ta.ItemType]:
    results: list[ta.ItemType] = []
    for op in self:
        result = op.evaluate(context)
        if isinstance(result, list):
            results.extend(result)
        elif result is not None:
            results.append(result)
    return results


@method(',')
def select__comma_operator(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    for op in self:
        yield from op.select(copy(context))


###
# Parenthesized expression: XPath 2.0 admits the empty case ().
@method(register('(', lbp=80, rpb=80, label='expression'))
def nud__parenthesized_expression(self: XPathToken) -> XPathToken:
    if self.parser.next_token.symbol != ')':
        self[:] = self.parser.expression(),
    self.parser.advance(')')
    return self


@method('(')
def led__parenthesized_expression(self: XPathToken, left: XPathToken) -> XPathToken:
    if left.symbol == '(name)':
        if left.value in self.parser.RESERVED_FUNCTION_NAMES:
            msg = f"{left.value!r} is not allowed as function name"
            raise left.error('XPST0003', msg)
        else:
            raise left.error('XPST0017', 'unknown function {!r}'.format(left.value))

    elif left.symbol == ':' and left[1].symbol == '(name)':
        if left[1].namespace == XSD_NAMESPACE:
            msg = 'unknown constructor function {!r}'.format(left[1].value)
            raise left[1].error('XPST0017', msg)
        raise left.error('XPST0017', 'unknown function {!r}'.format(left.value))

    if self.parser.next_token.symbol != ')':
        self[:] = left, self.parser.expression()
    else:
        self[:] = left,
    self.parser.advance(')')
    return self


@method('(')
def evaluate__parenthesized_expression(self: XPathToken, context: ta.ContextType = None) \
        -> ta.ValueType:
    return self[0].evaluate(context) if self else []


@method('(')
def select__parenthesized_expression(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    return self[0].select(context) if self else iter(())


###
# Value comparison operators (eq, ne, lt, le, gt, and ge)
#
# Ref: https://www.w3.org/TR/xpath20/#id-value-comparisons
#
@method('eq', bp=30)
@method('ne', bp=30)
@method('lt', bp=30)
@method('gt', bp=30)
@method('le', bp=30)
@method('ge', bp=30)
def led__value_comparison_operators(self: XPathToken, left: XPathToken) -> XPathToken:
    if left.symbol in COMPARISON_OPERATORS:
        raise self.wrong_syntax()
    self[:] = left, self.parser.expression(rbp=30)
    return self


@method('eq')
@method('ne')
@method('lt')
@method('gt')
@method('le')
@method('ge')
def evaluate__value_comparison_operators(self: XPathToken, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[bool]:
    operands = [self[0].get_atomized_operand(context), self[1].get_atomized_operand(context)]

    if any(x is None for x in operands):
        return []
    elif any(isinstance(x, XPathFunction) for x in operands):
        raise self.error('FOTY0013', "cannot compare a function item")
    elif all(isinstance(x, DoubleProxy10) for x in operands):
        # Special case of two <class 'float'> values: use custom operators
        if self.symbol == 'eq':
            return numeric_equal(*cast(list[float], operands))
        elif self.symbol == 'ne':
            return numeric_not_equal(*cast(list[float], operands))
        elif numeric_equal(*cast(list[float], operands)):
            return self.symbol in ('le', 'ge')

    cls0, cls1 = type(operands[0]), type(operands[1])
    if cls0 is cls1 and cls0 is not Duration:
        pass
    elif all(isinstance(x, float) for x in operands):
        pass
    elif any(isinstance(x, bool) for x in operands):
        msg = "cannot apply {} between {!r} and {!r}".format(self, *operands)
        raise self.error('XPTY0004', msg)
    elif all(isinstance(x, (int, Decimal)) for x in operands):
        pass
    elif all(isinstance(x, (str, UntypedAtomic, AnyURI)) for x in operands):
        pass
    elif all(isinstance(x, (str, UntypedAtomic, QName)) for x in operands):
        pass
    elif all(isinstance(x, (float, Decimal, int)) for x in operands):
        if isinstance(operands[0], float):
            operands[1] = get_double(cast(ta.NumericType, operands[1]), self.parser.xsd_version)
        else:
            operands[0] = get_double(cast(ta.NumericType, operands[0]), self.parser.xsd_version)
    elif all(isinstance(x, Duration) for x in operands) and self.symbol in ('eq', 'ne'):
        pass
    elif (issubclass(cls0, cls1) or issubclass(cls1, cls0)) and not issubclass(cls0, Duration):
        pass
    else:
        msg = "cannot apply {} between {!r} and {!r}".format(self, *operands)
        raise self.error('XPTY0004', msg)

    try:
        return cast(bool, getattr(operator, self.symbol)(*operands))
    except TypeError as err:
        raise self.error('XPTY0004', err) from None


###
# Node comparison
@method('is', bp=30)
def led__node_comparison(self: XPathToken, left: XPathToken) -> XPathToken:
    if left.symbol == 'is':
        raise self.wrong_syntax()
    self[:] = left, self.parser.expression(rbp=30)
    return self


@method('is')
@method(infix('<<', bp=30))
@method(infix('>>', bp=30))
def evaluate__node_comparison(self: XPathToken, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[bool]:
    symbol = self.symbol

    left = [x for x in self[0].select(context)]
    if not left:
        return []
    elif len(left) > 1 or not isinstance(left[0], XPathNode):
        raise self[0].error('XPTY0004', f"left operand of {symbol!r} must be a single node")

    right = [x for x in self[1].select(context)]
    if not right:
        return []
    elif len(right) > 1 or not isinstance(right[0], XPathNode):
        raise self[0].error('XPTY0004', "right operand of %r must be a single node" % symbol)

    if symbol == 'is':
        return left[0] is right[0]
    else:
        if left[0] is right[0] or context is None:
            return False

        documents = [context.root]
        documents.extend(v for v in context.variables.values() if isinstance(v, DocumentNode))

        for root in documents:
            if root is not None:
                for item in root.iter_document():  # pragma: no cover
                    if left[0] is item:
                        return True if symbol == '<<' else False
                    elif right[0] is item:
                        return False if symbol == '<<' else True
        else:
            raise self.error('FOCA0002', "operands are not nodes of the XML tree!")


###
# Range expression
@method('to', bp=35)
def led__range_expression(self: XPathToken, left: XPathToken) -> XPathToken:
    if left.symbol == 'to':
        raise self.wrong_syntax()
    self[:] = left, self.parser.expression(rbp=35)
    return self


@method('to')
def evaluate__range_expression(self: XPathToken, context: ta.ContextType = None) -> list[int]:
    start, stop = self.get_operands(context, cls=Integer)
    try:
        return xlist(range(start, stop + 1))
    except TypeError:
        return []


@method('to')
def select__range_expression(self: XPathToken, context: ta.ContextType = None) -> Iterator[int]:
    yield from cast(list[int] | list[NoReturn], self.evaluate(context))


###
# Numerical operators
@method(infix('idiv', bp=45))
def evaluate__idiv_operator(self: XPathToken, context: ta.ContextType = None) -> int:
    op1, op2 = self.get_operands(context)
    if op1 is None or op2 is None:
        raise self.error('XPST0005')

    try:
        if math.isinf(op1):
            raise self.error('FOAR0001' if op2 == 0 else 'FOAR0002')
        elif math.isnan(op1) or math.isnan(op2):
            raise self.error('FOAR0002')
    except TypeError as err:
        if isinstance(context, XPathSchemaContext):
            return 1
        raise self.error('XPTY0004', err) from None

    try:
        result = op1 // op2
    except (ZeroDivisionError, DivisionByZero):
        if isinstance(context, XPathSchemaContext):
            return 1
        raise self.error('FOAR0001') from None
    else:
        if result >= 0 or isinstance(op1, Decimal) or \
                isinstance(op2, Decimal) or abs(op1) == abs(op2):
            return int(result)
        else:
            return int(result) + 1


# Resolve the intrinsic ambiguity of some infix operators
@method('union')
@method('intersect')
@method('except')
@method('eq')
@method('ne')
@method('lt')
@method('gt')
@method('le')
@method('ge')
@method('is')
@method('to')
@method('idiv')
@method('instance')
@method('treat')
@method('castable')
@method('cast')
def nud__disambiguation_of_infix_operators(self: XPathToken) -> XPathToken:
    return self.as_name()


###
# Kind tests (sequence types that can appear also in XPath expressions)
@method(function('document-node', nargs=(0, 1), label='kind test'))
def select__document_node_kind_test(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[DocumentNode]:
    if context is None:
        raise self.missing_context()
    elif not self:
        for item in context.iter_children_or_self():
            if isinstance(item, DocumentNode):
                yield item
    else:
        elements = [e for e in self[0].select(copy(context)) if isinstance(e, ElementNode)]
        if isinstance(context.item, DocumentNode):
            if len(elements) == 1:
                yield context.item


@method('document-node')
def nud__document_node_kind_test(self: XPathFunction) -> XPathFunction:
    self.parser.advance('(')
    if self.parser.next_token.symbol in ('element', 'schema-element'):
        self[0:] = self.parser.expression(5),
        if self.parser.next_token.symbol == ',':
            msg = 'Too many arguments: expected at most 1 argument'
            raise self.error('XPST0017', msg)
    elif self.parser.next_token.symbol != ')':
        raise self.error('XPST0003', 'element or schema-element kind test expected')
    self.parser.advance(')')
    return self


@method(function('element', nargs=(0, 2), label='kind test'))
def select__element_kind_test(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ElementNode]:
    if context is None:
        raise self.missing_context()
    elif not self:
        for item in context.iter_children_or_self():
            if isinstance(item, ElementNode):
                yield item
    else:
        for item in self[0].select(context):
            if len(self) == 1:
                yield cast(ElementNode, item)  # Already selected by sequence type test
            elif isinstance(item, ElementNode):
                type_annotation = self[1].name
                if item.nilled:
                    if self[1].occurrence in ('*', '?'):
                        yield item
                elif item.type_name == type_annotation:
                    if type_annotation != XSD_UNTYPED:
                        yield item
                    elif self[0].symbol != '*':
                        yield item
                elif is_instance(item.typed_value, type_annotation, self.parser):
                    yield item


@method('element')
def nud__element_kind_test(self: XPathFunction) -> XPathFunction:
    self.parser.advance('(')
    if self.parser.next_token.symbol != ')':
        self.parser.expected_next('(name)', ':', '*', message='a QName or a wildcard expected')
        self[0:] = self.parser.expression(5),
        if self.parser.next_token.symbol == ',':
            self.parser.advance(',')
            self.parser.expected_next('(name)', ':', message='a QName expected')
            self[1:] = self.parser.expression(80),
            if self.parser.next_token.symbol in ('*', '+', '?'):
                self[1].occurrence = self.parser.next_token.symbol
                self.parser.advance()

    self.parser.advance(')')
    return self


@method(function('schema-attribute', nargs=1, label='kind test'))
def select__schema_attribute_kind_test(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[AttributeNode]:
    if context is None:
        raise self.missing_context()

    attribute_name = self[0].source
    qname = get_expanded_name(attribute_name, self.parser.namespaces)

    for _ in context.iter_children_or_self():
        if self.parser.schema is None:
            break

        if self.parser.schema.get_attribute(qname) is None:
            raise self.error('XPST0008', "attribute %r not found in schema" % attribute_name)

        if isinstance(context.item, AttributeNode) and context.item.match_name(qname):
            yield context.item
            return

    if not isinstance(context, XPathSchemaContext):
        raise self.error('XPST0008', 'schema attribute %r not found' % attribute_name)


@method(function('schema-element', nargs=1, label='kind test'))
def select__schema_element_kind_test(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ElementNode]:
    if context is None:
        raise self.missing_context()

    element_name = self[0].source
    qname = get_expanded_name(element_name, self.parser.namespaces)

    if self.parser.schema is not None:
        for _ in context.iter_children_or_self():
            if self.parser.schema.get_element(qname) is None:
                raise self.error('XPST0008', "element %r not found in schema" % element_name)

            if isinstance(context.item, ElementNode) and context.item.name == qname:
                yield context.item
                return

    if not isinstance(context, XPathSchemaContext):
        raise self.error('XPST0008', 'schema element %r not found' % element_name)


@method('schema-attribute')
@method('schema-element')
def nud__schema_node_kind_test(self: XPathFunction) -> XPathFunction:
    self.parser.advance('(')
    self.parser.expected_next('(name)', ':', 'Q{', message='a QName expected')
    self[0:] = self.parser.expression(5),
    self.parser.advance(')')
    return self


###
# Multi role-tokens definition: in XPath 2.0 the 'attribute' keyword is used both for
# attribute:: axis and attribute() node type function.
#
# First the XPath1 token class has to be removed from the XPath2 symbol table. Then the
# symbol has to be registered usually with the same binding power (bp --> lbp, rbp), a
# multi-value label (using a tuple of values) and a custom pattern. Finally a custom nud
# or led method is required.
XPath2Parser.unregister('attribute')
XPath2Parser.register(
    'attribute', lbp=90, rbp=90, label=('kind test', 'axis'),
    pattern=r'\battribute(?=\s*\:\:|\s*\(\:.*\:\)\s*\:\:|\s*\(|\s*\(\:.*\:\)\()'
)


@method('attribute')
def nud__attribute_kind_test_or_axis(self: XPathToken) -> XPathToken:
    if self.parser.next_token.symbol == '::':
        self.label = 'axis'
        self.parser.advance('::')
        self.parser.expected_next(
            '(name)', '*', 'text', 'node', 'document-node', 'comment', 'processing-instruction',
            'attribute', 'schema-attribute', 'element', 'schema-element', 'namespace-node'
        )
        self[:] = self.parser.expression(rbp=90),
    else:
        self.label = 'kind test'
        self.parser.advance('(')
        if self.parser.next_token.symbol != ')':
            self.parser.next_token.expected('(name)', '*', ':')
            self[:] = self.parser.expression(5),

            if self.parser.next_token.symbol == ',':
                self.parser.advance(',')
                self.parser.next_token.expected('(name)', ':')
                self[1:] = self.parser.expression(5),

        self.parser.advance(')')

        if self.namespace:
            msg = f"{self.value!r} is not allowed as function name"
            raise self.error('XPST0003', msg)

    return self


@method('attribute')
def select__attribute_kind_test_or_axis(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[ta.AtomicType | AttributeNode | XsdAttributeProtocol]:
    if context is None:
        raise self.missing_context()
    elif self.label == 'axis':
        for _ in context.iter_attributes():
            yield from cast(Iterator[AttributeNode], self[0].select(context))
    elif not self:
        for attribute in context.iter_attributes():
            yield attribute
    else:
        name = self[0].value
        assert isinstance(name, str)

        if self.parser.schema is not None and len(self) == 2:
            assert isinstance(self[1].value, str)
            type_name = get_expanded_name(self[1].value, namespaces=self.parser.namespaces)
        else:
            type_name = None

        for attribute in context.iter_attributes():
            if attribute.match_name(name):
                if isinstance(context, XPathSchemaContext):
                    continue

                if type_name == XSD_UNTYPED == attribute.type_name:
                    if name != '*':
                        yield attribute
                elif not type_name or attribute.type_name == type_name or \
                        is_instance(attribute.typed_value, type_name, self.parser):
                    yield attribute


# XPath 2.0 definitions continue into module xpath2_functions
