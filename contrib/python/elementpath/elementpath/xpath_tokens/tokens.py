#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""A collection of additional and special token classes."""
import math
from collections.abc import Iterator
from decimal import Decimal
from typing import Literal, cast, NoReturn

import elementpath.aliases as ta

from elementpath.namespaces import XSD_NAMESPACE, XPATH_FUNCTIONS_NAMESPACE, XMLNS_NAMESPACE
from elementpath.namespaces import get_expanded_name
from elementpath.datatypes import AnyAtomicType, AnyURI, UntypedAtomic, ArithmeticProxy, \
    YearMonthDuration, DayTimeDuration, Duration, AbstractDateTime
from elementpath.helpers import collapse_white_spaces
from elementpath.sequences import xlist
from elementpath.xpath_nodes import AttributeNode, ElementNode
from elementpath.xpath_context import XPathSchemaContext
from elementpath.decoder import get_atomic_sequence

from .base import XPathToken


class ValueToken(XPathToken):
    """
    A dummy token for encapsulating a value.
    """
    symbol = '(value)'
    value: AnyAtomicType

    @property
    def source(self) -> str:
        return str(self.value)

    def evaluate(self, context: ta.ContextType = None) -> AnyAtomicType:
        return self.value

    def select(self, context: ta.ContextType = None) -> Iterator[AnyAtomicType]:
        if isinstance(self.value, list):
            yield from self.value
        else:
            yield self.value


###
# Token classes for names
class ProxyToken(XPathToken):
    """
    A token class for resolving collisions between other tokens that have
    the same symbol but are in different namespaces. It also resolves
    collisions of functions with names.
    """
    symbol = '(proxy)'

    def nud(self) -> 'XPathToken':
        if self.parser.next_token.symbol not in ('(', '#'):
            # Not a function call or reference, returns a name.
            return self.as_name()

        lookup_name = f'{{{self.namespace or XPATH_FUNCTIONS_NAMESPACE}}}{self.value}'
        try:
            token = self.parser.symbol_table[lookup_name](self.parser)
        except KeyError:
            if self.namespace == XSD_NAMESPACE:
                msg = f'unknown constructor function {self.symbol!r}'
            else:
                msg = f'unknown function {self.symbol!r}'
            raise self.error('XPST0017', msg) from None
        else:
            if self.parser.next_token.symbol == '#':
                return token

            res = token.nud()
            return res


###
# Name related tokens for matching elements and attributes
class NameToken(XPathToken):
    """
    The special '(name)' token for matching attributes or element nodes.
    For XPath its value is an unprefixed name.
    """
    symbol = lookup_name = '(name)'
    label = 'name'
    bp = 10
    value: str

    def __str__(self) -> str:
        return f'{self.value!r} name'

    def nud(self) -> XPathToken:
        if self.parser.next_token.symbol == '::':
            msg = "axis '%s::' not found" % self.value
            if self.parser.compatibility_mode:
                raise self.error('XPST0010', msg)
            raise self.error('XPST0003', msg)
        elif self.parser.next_token.symbol == '(':
            if self.parser.version >= '2.0':
                pass  # XP30+ has led() for '(' operator that can check this
            elif self.namespace == XSD_NAMESPACE:
                raise self.error('XPST0017', 'unknown constructor function {!r}'.format(self.value))
            elif self.namespace or self.value not in self.parser.RESERVED_FUNCTION_NAMES:
                raise self.error('XPST0017', 'unknown function {!r}'.format(self.value))
            else:
                msg = f"{self.value!r} is not allowed as function name"
                raise self.error('XPST0003', msg)

        return self

    def evaluate(self, context: ta.ContextType = None) -> list[AttributeNode | ElementNode]:
        return xlist(self.select(context))

    def select(self, context: ta.ContextType = None) -> Iterator[AttributeNode | ElementNode]:
        if context is None:
            raise self.missing_context()
        yield from context.iter_matching_nodes(self.value, self.parser.default_namespace)


class PrefixedNameToken(XPathToken):
    """Colon symbol for expressing prefixed qualified names or wildcards."""
    symbol = lookup_name = ':'
    lbp = 95
    rbp = 95
    value: str

    def __init__(self, parser: ta.XPathParserType, value: Literal[':'] = ':') -> None:
        super().__init__(parser, value)

        # Change bind powers if it cannot be a namespace related token
        if self.is_spaced():
            self.lbp = self.rbp = 0
        elif self.parser.token.symbol not in ('*', '(name)', 'array'):
            self.lbp = self.rbp = 0

    def __str__(self) -> str:
        if len(self) < 2:
            return 'unparsed prefixed reference'
        elif self[1].label.endswith('function'):
            return f"{self.value!r} {self[1].label}"
        elif '*' in self.value:
            return f"{self.value!r} prefixed wildcard"
        else:
            return f"{self.value!r} prefixed name"

    @property
    def source(self) -> str:
        return ':'.join(tk.source for tk in self) + self.occurrence

    def led(self, left: XPathToken) -> XPathToken:
        version = self.parser.version
        if self.is_spaced():
            if version <= '3.0':
                raise self.wrong_syntax("a QName cannot contains spaces before or after ':'")
            return left

        if version == '1.0':
            left.expected('(name)')
        elif version <= '3.0':
            left.expected('(name)', '*')
        elif left.symbol not in ('(name)', '*'):
            return left

        if not self.parser.next_token.label.endswith('function'):
            self.parser.expected_next('(name)', '*')

        if isinstance(left, NameToken):
            try:
                namespace = self.parser.namespaces[left.value]
            except KeyError:
                self.parser.advance()  # Assure there isn't a following incomplete comment
                self[:] = left, self.parser.token
                msg = "prefix {!r} is not declared".format(left.value)
                # raise self.error('FONS0004', msg) from None  FIXME?? XP30+??
                raise self.error('XPST0081', msg) from None
            else:
                self.parser.next_token.bind_namespace(namespace)
        elif self.parser.next_token.symbol != '(name)':
            raise self.wrong_syntax()
        else:
            self.parser.next_token.bind_namespace('*')

        self[:] = left, self.parser.expression(95)

        self.name = self[1].name
        if self[1].label.endswith('function'):
            self.value = f'{self[0].value}:{self[1].symbol}'
        else:
            self.value = f'{self[0].value}:{self[1].value}'
        return self

    def evaluate(self, context: ta.ContextType = None) -> ta.ValueType:
        if self[1].label.endswith('function'):
            return self[1].evaluate(context)
        return xlist(self.select(context))

    def select(self, context: ta.ContextType = None) -> Iterator[ta.ItemType]:
        if self[1].label.endswith('function'):
            value = self[1].evaluate(context)
            if isinstance(value, list):
                yield from value
            elif value is not None:
                yield value
            return

        if context is None:
            raise self.missing_context()

        yield from context.iter_matching_nodes(self.name)


class BracedNameToken(XPathToken):
    """Braced expanded name for expressing namespace related names."""

    symbol = lookup_name = '{'
    label = 'expanded name'
    bp = lbp = rbp = 95
    value: str

    def nud(self) -> XPathToken:
        if self.parser.strict and self.symbol == '{':
            raise self.wrong_syntax("not allowed symbol if parser has strict=True")

        self.parser.next_token.unexpected('{')
        if self.parser.next_token.symbol == '}':
            namespace = ''
        else:
            value = self.parser.next_token.value
            assert isinstance(value, str)
            namespace = value + self.parser.advance_until('}')
            namespace = collapse_white_spaces(namespace)

        try:
            AnyURI(namespace)
        except ValueError as err:
            msg = f"invalid URI in an EQName: {str(err)}"
            raise self.error('XQST0046', msg) from None

        if namespace == XMLNS_NAMESPACE:
            msg = f"cannot use the URI {XMLNS_NAMESPACE!r}!r in an EQName"
            raise self.error('XQST0070', msg)

        self.parser.advance()
        if not self.parser.next_token.label.endswith('function'):
            self.parser.expected_next('(name)', '*')
        self.parser.next_token.bind_namespace(namespace)

        cls: type[XPathToken] = self.parser.symbol_table['(string)']
        self[:] = cls(self.parser, namespace), self.parser.expression(90)

        if self[0].value:
            self.name = self[1].name = self.value = f'{{{self[0].value}}}{self[1].value}'
        elif self[1].value == '*':
            self.name = self[1].name = self.value = '{}*'
        else:
            self.name = self[1].name = self.value = cast(str, self[1].value)
        return self

    def evaluate(self, context: ta.ContextType = None) -> ta.ValueType:
        if self[1].label.endswith('function'):
            return self[1].evaluate(context)
        return xlist(self.select(context))

    def select(self, context: ta.ContextType = None) -> Iterator[ta.ItemType]:
        if self[1].label.endswith('function'):
            result = self[1].evaluate(context)
            if isinstance(result, list):
                yield from result
            else:
                yield result
            return
        elif context is None:
            raise self.missing_context()

        if isinstance(self.value, str):
            yield from context.iter_matching_nodes(self.name)


class VariableToken(XPathToken):
    """A token for parsing a variable reference."""

    symbol = lookup_name = '$'
    label = 'variable'
    bp = lbp = rbp = 90
    value: str

    def __str__(self) -> str:
        if not self._items:
            return 'unparsed variable reference'
        return f'${self[0].value} variable'

    def nud(self: XPathToken) -> XPathToken:
        self.parser.expected_next('(name)', 'Q{')
        self[:] = self.parser.expression(rbp=90),
        self.value = self[0].value
        return self

    def evaluate(self: XPathToken, context: ta.ContextType = None) -> ta.ValueType:
        if context is None:
            raise self.missing_context()

        assert isinstance(self.value, str)
        varname: str = self.value
        if varname.startswith('{'):
            expanded_name = varname
        else:
            try:
                expanded_name = get_expanded_name(self.value, self.parser.namespaces)
            except KeyError as err:
                raise self.error('XPST0081', "namespace prefix {} not found".format(err))

        try:
            value = context.variables[varname]
        except KeyError:
            if expanded_name in context.variables:
                value = context.variables[expanded_name]
            elif isinstance(context, XPathSchemaContext):
                try:
                    st = cast(dict[str, str], self.parser.variable_types)[varname]
                except KeyError:
                    return []

                if st[-1] in ('*', '+', '?'):
                    st, occurs = st[:-1], st[-1]
                else:
                    occurs = ''

                if self.parser.schema is not None and st.startswith('xs:'):
                    xsd_type = self.parser.schema.get_type(
                        st.replace('xs:', f'{{{XSD_NAMESPACE}}}', 1)
                    )
                    result = [x for x in get_atomic_sequence(xsd_type)]
                else:
                    result = [UntypedAtomic('1')]

                if occurs in ('', '?') and len(result) == 1:
                    return result[0]
                else:
                    return xlist(result)
            else:
                raise self.error('XPST0008', 'unknown variable %r' % str(varname))

        if isinstance(value, (tuple, list)):
            return xlist(value)
        elif value is None:
            return []
        else:
            return value


class AsteriskToken(XPathToken):

    symbol = lookup_name = '*'
    label = 'wildcard symbol'
    bp = lbp = rbp = 45

    def __str__(self) -> str:
        return f'{self.symbol!r} {self.label}'

    def nud(self) -> 'AsteriskToken':
        return self

    def led(self, left: XPathToken) -> 'AsteriskToken':
        self.label = 'operator'
        self[:] = left, self.parser.expression(rbp=45)
        return self

    def evaluate(self, context: ta.ContextType = None) -> ta.ValueType:
        op1: ta.ArithmeticType | None
        op2: ta.ArithmeticType
        if self:
            op1, op2 = self.get_operands(context, cls=ArithmeticProxy)
            if op1 is None:
                return []
            try:
                if isinstance(op2, (YearMonthDuration, DayTimeDuration)):
                    return op2 * op1
                return op1 * op2  # type:ignore[operator]
            except TypeError as err:
                if isinstance(context, XPathSchemaContext):
                    return []

                if isinstance(op1, (float, Decimal)):
                    if math.isnan(op1):
                        raise self.error('FOCA0005') from None
                    elif math.isinf(op1):
                        raise self.error('FODT0002') from None

                if isinstance(op2, (float, Decimal)):
                    if math.isnan(op2):
                        raise self.error('FOCA0005') from None
                    elif math.isinf(op2):
                        raise self.error('FODT0002') from None

                raise self.error('XPTY0004', err) from None
            except ValueError as err:
                if isinstance(context, XPathSchemaContext):
                    return []
                raise self.error('FOCA0005', err) from None
            except OverflowError as err:
                if isinstance(context, XPathSchemaContext):
                    return []
                elif isinstance(op1, AbstractDateTime):
                    raise self.error('FODT0001', err) from None
                elif isinstance(op1, Duration):
                    raise self.error('FODT0002', err) from None
                else:
                    raise self.error('FOAR0002', err) from None
        else:
            # This is not a multiplication operator but a wildcard select statement
            return xlist(self.select(context))

    def select(self, context: ta.ContextType = None) -> Iterator[ta.ItemType]:
        if self:
            # Product operator
            item = self.evaluate(context)
            if not isinstance(item, list):
                if context is not None:
                    context.item = item
                yield item
            elif context is not None:
                for context.item in item:
                    yield context.item
            else:
                yield from item
            return
        elif context is None:
            raise self.missing_context()

        # Wildcard literal
        if self.parser.schema is None:
            for item in context.iter_children_or_self():
                if item is None:
                    pass  # '*' wildcard doesn't match document nodes
                elif context.axis == 'attribute':
                    if isinstance(item, AttributeNode):
                        yield item
                elif isinstance(item, ElementNode):
                    yield item
        else:
            # XSD typed selection
            for item in context.iter_children_or_self():
                if context.is_principal_node_kind():
                    if isinstance(item, (ElementNode, AttributeNode)):
                        yield item


class ParentShortcutToken(XPathToken):
    symbol = lookup_name = '..'
    bp = lbp = rbp = 0
    label = 'shortcut expression'

    def nud(self) -> 'ParentShortcutToken':
        return self

    def evaluate(self, context: ta.ContextType = None) \
            -> ta.ParentNodeType | list[NoReturn]:
        if context is None:
            raise self.missing_context()

        for value in context.iter_parent():
            return value
        else:
            return []

    def select(self, context: ta.ContextType = None) -> Iterator[ta.ParentNodeType]:
        if context is None:
            raise self.missing_context()
        yield from context.iter_parent()


class ContextItemToken(XPathToken):
    """A token for parsing a context item reference."""

    symbol = lookup_name = '.'
    bp = lbp = rbp = 0
    label = 'context item expression'

    def __str__(self) -> str:
        return self.label

    def nud(self) -> 'ContextItemToken':
        return self

    def evaluate(self, context: ta.ContextType = None) -> ta.ItemType:
        if context is None:
            raise self.missing_context()
        return context.item

    def select(self, context: ta.ContextType = None) -> Iterator[ta.ItemType]:
        if context is None:
            raise self.missing_context()
        yield from context.iter_self()
