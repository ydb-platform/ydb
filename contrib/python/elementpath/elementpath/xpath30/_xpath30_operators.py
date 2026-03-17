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
XPath 3.0 implementation - part 2 (symbols, operators and expressions)
"""
from copy import copy
from collections.abc import Iterator
from typing import Any, cast, Union

import elementpath.aliases as ta
import elementpath.namespaces as ns
from elementpath.datatypes import QName
from elementpath.xpath_tokens import XPathToken, ValueToken, XPathFunction, \
    XPathMap, XPathArray

from .xpath30_parser import XPath30Parser

__all__ = ['XPath30Parser']

register = XPath30Parser.register
infix = XPath30Parser.infix
method = XPath30Parser.method

register(':=')

###
# Placeholder symbol (used also for optional occurrence)

XPath30Parser.unregister('?')
register('?', bases=(ValueToken,))


@method('?')
def nud__placeholder_symbol(self: ValueToken) -> ValueToken:
    return self


@method('?')
def evaluate__placeholder_symbol(self: ValueToken, context: ta.ContextType = None) -> ValueToken:
    return self


###
# Braced/expanded QName(s)

XPath30Parser.duplicate('{', 'Q{', pattern=r'Q\{')
XPath30Parser.unregister('{')
XPath30Parser.unregister('}')
register('{')
register('}', bp=100)


XPath30Parser.unregister('(')


@method(register('(', lbp=80, rpb=80, label='expression'))
def nud__parenthesized_expression(self: XPathToken) -> XPathToken:
    if self.parser.next_token.symbol != ')':
        self[:] = self.parser.expression(),
    self.parser.advance(')')
    return self


@method('(')
def led__parenthesized_expression(self: XPathToken, left: XPathToken) -> XPathToken:
    if left.symbol in ('(name)', 'Q{'):
        if left.value in self.parser.RESERVED_FUNCTION_NAMES:
            msg = f"{left.value!r} is not allowed as function name"
            raise left.error('XPST0003', msg)
        else:
            raise left.error('XPST0017', 'unknown function {!r}'.format(left.value))

    elif left.symbol == ':' and left[1].symbol == '(name)':
        if left[1].namespace == ns.XSD_NAMESPACE:
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
        -> ta.ValueType | XPathToken:
    if not self:
        return []

    value = self[0].evaluate(context)

    if isinstance(value, list) and len(value) == 1:
        value = value[0]

    if len(self) > 1:
        if isinstance(value, XPathFunction):
            func: XPathFunction
            func = value
            tokens = self[1].get_argument_tokens()

            if any(x.symbol == '?' and not x for x in tokens):
                func.check_arguments_number(len(tokens))
                func = copy(func)
                func[:] = tokens
                func.to_partial_function()
                return func

            arguments: list[ta.ValueType] = [tk.evaluate(context) for tk in tokens]

            if func.label == 'partial function' and func[0].symbol == '?' and len(func[0]):
                if context is None:
                    raise self.missing_context()
                return func(context.item, *arguments, context=context)

            return func(*arguments, context=context)

        elif self[0].symbol == '(':
            if not isinstance(value, list):
                return value
            elif any(not isinstance(x, XPathFunction) for x in value):
                return value

        if isinstance(value, XPathToken) and value.symbol == '?':
            return value

        raise self.error('XPTY0004', f'an XPath function expected, not {type(value)!r}')

    if isinstance(value, (XPathMap, XPathArray)) or \
            not isinstance(value, XPathFunction) or self[0].span[0] > self.span[0]:
        return value
    else:
        return value(context=context)


@method(infix('||', bp=32))
def evaluate__union_operator(self: XPathToken, context: ta.ContextType = None) -> str:

    return self.string_value(self.get_argument(context)) + \
        self.string_value(self.get_argument(context, index=1))


@method(infix('!', bp=72))
def select__simple_map_operator(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    if context is None:
        raise self.missing_context()

    for context.item in self[0].select_with_focus(context):
        for result in self[1].select(context):
            yield result


###
# 'let' expressions

@method('let', bp=20, label='expression')
def nud__let_expression(self: XPathToken) -> XPathToken:
    del self[:]
    if self.parser.next_token.symbol != '$':
        return self.as_name()

    while True:
        self.parser.next_token.expected('$')
        variable = self.parser.expression(5)
        self.append(variable)
        self.parser.advance(':=')
        expr = self.parser.expression(5)
        self.append(expr)
        if self.parser.next_token.symbol != ',':
            break
        self.parser.advance()

    self.parser.advance('return')
    self.append(self.parser.expression(5))
    return self


@method('let')
def select__let_expression(self: XPathToken, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    if context is None:
        raise self.missing_context()

    context = copy(context)
    context.variables = context.variables.copy()

    for k in range(0, len(self) - 1, 2):
        varname = cast(str, self[k][0].value)
        value = self[k+1].evaluate(context)
        context.variables[varname] = value

    yield from self[-1].select(context)


@method('#', bp=90)
def led__function_reference(self: XPathToken, left: XPathToken) -> XPathToken:
    if not left.label.endswith('function'):
        left.expected(':', '(name)', 'Q{')

    self[:] = left, self.parser.expression(rbp=90)
    self[1].expected('(integer)')
    return self


@method('#')
def evaluate__function_reference(self: XPathToken, context: ta.ContextType = None) -> XPathFunction:
    token_class: type[Union[XPathFunction, XPathToken]]
    namespace: Any
    name: Any

    arity = self[1].value
    assert arity is None or isinstance(arity, int)

    if isinstance(self[0], XPathFunction):
        token_class = self[0].__class__
        namespace = self[0].namespace
        name = self[0].name
        if isinstance(name, QName):
            qname = name
        else:
            qname = QName(None, f'anonymous {self[0].label}'.replace(' ', '-'))
    else:
        if self[0].symbol == ':':
            namespace = self[0][1].namespace
            name = self[0].value
        elif self[0].symbol == 'Q{':
            namespace = self[0][0].value
            name = self[0][1].value
        elif self[0].value not in self.parser.RESERVED_FUNCTION_NAMES:
            namespace = ns.XPATH_FUNCTIONS_NAMESPACE
            name = self[0].value
        else:
            msg = f"{self[0].value!r} is not allowed as function name"
            raise self.error('XPST0003', msg)

        assert isinstance(name, str)
        assert isinstance(namespace, str) or namespace is None
        qname = QName(namespace, name)
        namespace = qname.namespace
        local_name = qname.local_name

        # Generic rule for XSD constructor functions
        if namespace == ns.XSD_NAMESPACE and arity != 1:
            raise self.error('XPST0017', f"unknown function {qname.qname}#{arity}")

        # Special checks for multirole tokens
        if namespace == ns.XPATH_FUNCTIONS_NAMESPACE and \
                local_name in ('QName', 'dateTime') and arity == 1:
            raise self.error('XPST0017', f"unknown function {qname.qname}#{arity}")

        try:
            token_class = self.parser.symbol_table[qname.expanded_name]
        except KeyError:
            try:
                token_class = self.parser.symbol_table[local_name]
            except KeyError:
                msg = f"unknown function {qname.qname}#{arity}"
                raise self.error('XPST0017', msg) from None

        if token_class.symbol == 'function' or not token_class.label.endswith('function'):
            raise self.error('XPST0003')
        assert issubclass(token_class, XPathFunction)

    try:
        func = token_class(self.parser, nargs=arity)
    except TypeError:
        msg = f"unknown function {qname.qname}#{arity}"
        raise self.error('XPST0017', msg) from None
    else:
        if func.namespace is None:
            func.namespace = namespace
        elif func.namespace != namespace:
            raise self.error('XPST0017', f"unknown function {qname.qname}#{arity}")
        func.context = copy(context)
        return func
