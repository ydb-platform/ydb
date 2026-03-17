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
XPath 1.0 implementation - part 3 (functions)
"""
import math
import decimal
from collections.abc import Iterator
from typing import Any

import elementpath.aliases as ta

from elementpath.namespaces import XML_ID, XML_LANG
from elementpath.datatypes import AnyURI, Float, DayTimeDuration, YearMonthDuration, \
    StringProxy, AnyAtomicType, Duration
from elementpath.helpers import get_double
from elementpath.xpath_nodes import XPathNode, ElementNode, TextNode, CommentNode, \
    ProcessingInstructionNode, DocumentNode, EtreeElementNode
from elementpath.xpath_context import XPathSchemaContext
from elementpath.xpath_tokens import XPathFunction

from ._xpath1_operators import XPath1Parser

__all__ = ['XPath1Parser']

method = XPath1Parser.method
function = XPath1Parser.function


###
# Kind tests (for matching of node types in XPath 1.0 or sequence types in XPath 2.0)
@method(function('node', nargs=0, label='kind test'))
def select__node_kind_test(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[XPathNode]:
    if context is None:
        raise self.missing_context()

    for item in context.iter_children_or_self():
        if isinstance(item, XPathNode):
            if not isinstance(item, DocumentNode) or item is context.root:
                yield item


@method('node')
def nud__item_sequence_type(self: XPathFunction) -> XPathFunction:
    XPathFunction.nud(self)
    if self.parser.next_token.symbol in ('*', '+', '?'):
        self.occurrence = self.parser.next_token.symbol
        self.parser.advance()
    return self


@method(function('processing-instruction', nargs=(0, 1), bp=79, label='kind test'))
def select__pi_kind_test(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ProcessingInstructionNode]:
    if context is None:
        raise self.missing_context()

    for item in context.iter_children_or_self():
        if isinstance(item, ProcessingInstructionNode):
            if not self:
                yield item
            elif isinstance(name := self[0].value, str):
                if item.name == ' '.join(name.strip().split()):
                    yield item


@method('processing-instruction')
def nud__pi_kind_test(self: XPathFunction) -> XPathFunction:
    self.parser.advance('(')
    if self.parser.next_token.symbol != ')':
        self.parser.next_token.expected('(name)', '(string)')
        self[0:] = self.parser.expression(5),
    self.parser.advance(')')
    return self


@method(function('comment', nargs=0, label='kind test'))
def select__comment_kind_test(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[CommentNode]:
    if context is None:
        raise self.missing_context()

    for item in context.iter_children_or_self():
        if isinstance(item, CommentNode):
            yield item


@method(function('text', nargs=0, label='kind test'))
def select__text_kind_test(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[TextNode]:
    if context is None:
        raise self.missing_context()

    for item in context.iter_children_or_self():
        if isinstance(item, TextNode):
            yield item


###
# Node set functions
@method(function('last', nargs=0, sequence_types=('xs:integer',)))
def evaluate__last_function(self: XPathFunction, context: ta.ContextType = None) -> int:
    if self.context is not None:
        context = self.context
    elif context is None:
        raise self.missing_context()
    elif context.size is None:
        raise self.missing_context("Context size is undefined")
    return context.size


@method(function('position', nargs=0,
                 sequence_types=('xs:integer',)))
def evaluate__position(self: XPathFunction, context: ta.ContextType = None) -> int:
    if self.context is not None:
        context = self.context
    elif context is None:
        raise self.missing_context()
    return context.position


@method(function('count', nargs=1, sequence_types=('item()*', 'xs:integer')))
def evaluate__count(self: XPathFunction, context: ta.ContextType = None) -> int:
    return len([x for x in self[0].select(self.context or context)])


@method(function('id', nargs=1, sequence_types=('xs:string*', 'element()*')))
def select__id(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ElementNode]:
    if self.context is not None:
        context = self.context
    elif context is None:
        raise self.missing_context()

    value = self[0].evaluate(context)
    item = context.item
    if item is None:
        item = context.root

    if isinstance(item, (ElementNode, DocumentNode)):
        for element in item.iter_descendants():
            if isinstance(element, EtreeElementNode) and element.value.get(XML_ID) == value:
                yield element


@method(function('name', nargs=(0, 1), sequence_types=('node()?', 'xs:string')))
@method(function('local-name', nargs=(0, 1), sequence_types=('node()?', 'xs:string')))
@method(function('namespace-uri', nargs=(0, 1), sequence_types=('node()?', 'xs:anyURI')))
def evaluate__name_related_functions(self: XPathFunction, context: ta.ContextType = None) \
        -> str | AnyURI:
    if self.context is not None:
        context = self.context
    elif context is None:
        raise self.missing_context()

    arg = self.get_argument(context, default_to_context=True)
    if arg is None:
        return ''
    elif not isinstance(arg, XPathNode):
        raise self.error('XPTY0004')

    name = arg.name
    if name is None:
        return ''

    symbol = self.symbol
    if symbol == 'name':
        node_name = arg.node_name
        if node_name is None:
            return ''
        return node_name.qname
    elif symbol == 'local-name':
        return name if not name or name[0] != '{' else name.split('}')[1]
    elif self.parser.version == '1.0':
        return '' if not name or name[0] != '{' else name.split('}')[0][1:]
    else:
        return AnyURI('') if not name or name[0] != '{' else AnyURI(name.split('}')[0][1:])


###
# String functions
@method(function('string', nargs=(0, 1), sequence_types=('item()?', 'xs:string')))
def evaluate__string(self: XPathFunction, context: ta.ContextType = None) -> str:
    if self.context is not None:
        context = self.context

    if not self:
        if context is None:
            raise self.missing_context()
        return self.string_value(context.item)
    return self.string_value(self.get_argument(context))


@method(function('contains', nargs=2,
                 sequence_types=('xs:string?', 'xs:string?', 'xs:boolean')))
def evaluate__contains(self: XPathFunction, context: ta.ContextType = None) -> bool:
    if self.context is not None:
        context = self.context

    arg1 = self.get_argument(context, default='', cls=str)
    arg2 = self.get_argument(context, index=1, default='', cls=str)
    return arg2 in arg1


@method(function('concat', nargs=(2, None),
                 sequence_types=('xs:anyAtomicType?', 'xs:anyAtomicType?', 'xs:string')))
def evaluate__concat(self: XPathFunction, context: ta.ContextType = None) -> str:
    if self.context is not None:
        context = self.context

    return ''.join(
        self.string_value(self.get_argument(context, index=k)) for k in range(len(self))
    )


@method(function('string-length', nargs=(0, 1),
                 sequence_types=('xs:string?', 'xs:integer')))
def evaluate__string_length(self: XPathFunction, context: ta.ContextType = None) -> int:
    if self.context is not None:
        context = self.context

    if self:
        return len(self.get_argument(context, default_to_context=True, default='', cls=str))
    elif context is None:
        raise self.missing_context()
    else:
        return len(self.string_value(context.item))


@method(function('normalize-space', nargs=(0, 1),
                 sequence_types=('xs:string?', 'xs:string')))
def evaluate__normalize_space(self: XPathFunction, context: ta.ContextType = None) -> str:
    if self.context is not None:
        context = self.context

    if self.parser.version == '1.0' or not self:
        arg = self.string_value(self.get_argument(context, default_to_context=True, default=''))
    else:
        arg = self.get_argument(context, default_to_context=True, default='', cls=str)
    return ' '.join(arg.strip().split())


@method(function('starts-with', nargs=2,
                 sequence_types=('xs:string?', 'xs:string?', 'xs:boolean')))
def evaluate__starts_with(self: XPathFunction, context: ta.ContextType = None) -> bool:
    if self.context is not None:
        context = self.context

    arg1: str = self.get_argument(context, default='', cls=str)
    arg2: str = self.get_argument(context, index=1, default='', cls=str)
    return arg1.startswith(arg2)


@method(function('translate', nargs=3,
                 sequence_types=('xs:string?', 'xs:string', 'xs:string', 'xs:string')))
def evaluate__translate(self: XPathFunction, context: ta.ContextType = None) -> str:
    if self.context is not None:
        context = self.context

    arg: str = self.get_argument(context, default='', cls=str)

    map_string: str = self.get_argument(context, index=1, cls=str)
    if map_string is None:
        message = "the 2nd argument of fn:translate() cannot be the empty sequence"
        raise self.error('XPTY0004', message)

    trans_string: str = self.get_argument(context, index=2, cls=str)
    if trans_string is None:
        message = "the 3rd argument of fn:translate() cannot be the empty sequence"
        raise self.error('XPTY0004', message)

    if len(map_string) == len(trans_string):
        return arg.translate(str.maketrans(map_string, trans_string))
    elif len(map_string) > len(trans_string):
        k = len(trans_string)
        return arg.translate(str.maketrans(map_string[:k], trans_string, map_string[k:]))
    else:
        return arg.translate(str.maketrans(map_string, trans_string[:len(map_string)]))


@method(function('substring', nargs=(2, 3),
                 sequence_types=('xs:string?', 'xs:double', 'xs:double', 'xs:string')))
def evaluate__substring(self: XPathFunction, context: ta.ContextType = None) -> str:
    if self.context is not None:
        context = self.context

    item: str = self.get_argument(context, default='', cls=str)
    try:
        start = self.get_argument(context, index=1, required=True)
        if math.isnan(start) or math.isinf(start):
            return ''
    except TypeError:
        if isinstance(context, XPathSchemaContext):
            start = 0
        else:
            raise self.error('FORG0006', "the second argument must be xs:numeric") from None
    else:
        start = int(round(start)) - 1

    if len(self) == 2:
        return item[max(start, 0):]
    else:
        try:
            length = self.get_argument(context, index=2, required=True)
            if math.isnan(length) or length <= 0:
                return ''
        except TypeError:
            if isinstance(context, XPathSchemaContext):
                length = len(item)
            else:
                raise self.error('FORG0006', "the third argument must be xs:numeric") from None

        if math.isinf(length):
            return item[max(start, 0):]
        else:
            stop = start + int(round(length))
            return item[slice(max(start, 0), max(stop, 0))]


@method(function('substring-before', nargs=2,
                 sequence_types=('xs:string?', 'xs:string?', 'xs:string')))
@method(function('substring-after', nargs=2,
                 sequence_types=('xs:string?', 'xs:string?', 'xs:string')))
def evaluate__substring_before_or_after_functions(
        self: XPathFunction, context: ta.ContextType = None) -> str:
    if self.context is not None:
        context = self.context

    arg1: str = self.get_argument(context, default='', cls=str)
    arg2: str = self.get_argument(context, index=1, default='', cls=str)

    index = arg1.find(arg2)
    if index < 0:
        return ''
    if self.symbol == 'substring-before':
        return arg1[:index]
    else:
        return arg1[index + len(arg2):]


###
# Boolean functions
@method(function('boolean', nargs=1,
                 sequence_types=('item()*', 'xs:boolean')))
def evaluate__boolean(self: XPathFunction, context: ta.ContextType = None) -> bool:
    return self.boolean_value(self[0].select(self.context or context))


@method(function('not', nargs=1, sequence_types=('item()*', 'xs:boolean')))
def evaluate__not(self: XPathFunction, context: ta.ContextType = None) -> bool:
    return not self.boolean_value(self[0].select(self.context or context))


@method(function('true', nargs=0, sequence_types=('xs:boolean',)))
def evaluate__true(self: XPathFunction, context: ta.ContextType = None) -> bool:
    return True


@method(function('false', nargs=0, sequence_types=('xs:boolean',)))
def evaluate__false(self: XPathFunction, context: ta.ContextType = None) -> bool:
    return False


@method(function('lang', nargs=1,
                 sequence_types=('xs:string?', 'xs:boolean')))
def evaluate__lang(self: XPathFunction, context: ta.ContextType = None) -> bool:
    if self.context is not None:
        context = self.context
    elif context is None:
        raise self.missing_context()

    if not isinstance(context.item, EtreeElementNode):
        return False
    else:
        try:
            attr = context.item.value.attrib[XML_LANG]
        except KeyError:
            for e in context.iter_ancestors():
                if isinstance(e, EtreeElementNode) and XML_LANG in e.value.attrib:
                    lang = e.value.attrib[XML_LANG]
                    if not isinstance(lang, str):
                        return False
                    break
            else:
                return False
        else:
            if not isinstance(attr, str):
                return False
            lang = attr.strip()

        if '-' in lang:
            lang, _ = lang.split('-')

        value = self[0].evaluate()
        if not isinstance(value, str):
            return False
        return lang.lower() == value.lower()


###
# Number functions
@method(function('number', nargs=(0, 1), sequence_types=('xs:anyAtomicType?', 'xs:double')))
def evaluate__number(self: XPathFunction, context: ta.ContextType = None) -> float:
    arg = self.get_argument(self.context or context, default_to_context=True)
    return self.number_value(arg)


@method(function('sum', nargs=(1, 2),
                 sequence_types=('xs:anyAtomicType*', 'xs:anyAtomicType?', 'xs:anyAtomicType?')))
def evaluate__sum(self: XPathFunction, context: ta.ContextType = None) -> ta.OneAtomicOrEmpty:
    if self.context is not None:
        context = self.context

    xsd_version = self.parser.xsd_version
    values: list[Any]
    try:
        values = [get_double(self.string_value(x), xsd_version)
                  if isinstance(x, XPathNode) else x
                  for x in self[0].select_flatten(context)]
    except (TypeError, ValueError):
        if self.parser.version == '1.0':
            return math.nan
        elif isinstance(context, XPathSchemaContext):
            return []
        raise self.error('FORG0006') from None

    if not values:
        zero = 0 if len(self) == 1 else self.get_argument(context, index=1)
        return [] if zero is None else zero

    if all(isinstance(x, (decimal.Decimal, int)) for x in values):
        result = sum(values) if len(values) > 1 else values[0]
    elif all(isinstance(x, DayTimeDuration) for x in values) or \
            all(isinstance(x, YearMonthDuration) for x in values):
        result = sum(values[1:], start=values[0])
    elif any(isinstance(x, Duration) for x in values):
        raise self.error('FORG0006', 'invalid sum of duration values')
    elif any(isinstance(x, (StringProxy, AnyURI)) for x in values):
        raise self.error('FORG0006', 'cannot apply fn:sum() to string-based types')
    elif any(isinstance(x, float) and math.isnan(x) for x in values):
        return math.nan
    elif all(isinstance(x, Float) for x in values):
        result = sum(values)
    else:
        try:
            result = sum(self.number_value(x) for x in values)
        except TypeError:
            if self.parser.version == '1.0':
                return math.nan
            elif isinstance(context, XPathSchemaContext):
                return []
            raise self.error('FORG0006') from None

    assert isinstance(result, AnyAtomicType)
    return result


@method(function('ceiling', nargs=1, sequence_types=('xs:numeric?', 'xs:numeric?')))
@method(function('floor', nargs=1, sequence_types=('xs:numeric?', 'xs:numeric?')))
def evaluate__ceiling_and_floor_functions(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneNumericOrEmpty:
    if self.context is not None:
        context = self.context

    arg = self.get_argument(context)
    if arg is None:
        return math.nan if self.parser.version == '1.0' else []
    elif isinstance(arg, XPathNode) or self.parser.compatibility_mode:
        arg = self.number_value(arg)

    try:
        if math.isnan(arg) or math.isinf(arg):
            assert isinstance(arg, (int, float, decimal.Decimal))
            return arg

        assert isinstance(arg, (int, float, decimal.Decimal))
        if self.symbol == 'floor':
            return type(arg)(math.floor(arg))
        else:
            return type(arg)(math.ceil(arg))
    except TypeError as err:
        if isinstance(context, XPathSchemaContext):
            return []
        elif isinstance(arg, str):
            raise self.error('XPTY0004', err) from None
        raise self.error('FORG0006', err) from None


@method(function('round', nargs=1, sequence_types=('xs:numeric?', 'xs:numeric?')))
def evaluate__round(self: XPathFunction, context: ta.ContextType = None) -> ta.OneNumericOrEmpty:
    if self.context is not None:
        context = self.context

    arg = self.get_argument(context)
    if arg is None:
        return math.nan if self.parser.version == '1.0' else []
    elif isinstance(arg, XPathNode) or self.parser.compatibility_mode:
        arg = self.number_value(arg)

    if isinstance(arg, float) and (math.isnan(arg) or math.isinf(arg)):
        return arg

    try:
        number = decimal.Decimal(arg)
        assert isinstance(arg, (int, float, decimal.Decimal))
        if number > 0:
            return type(arg)(number.quantize(decimal.Decimal('1'), rounding='ROUND_HALF_UP'))
        else:
            return type(arg)(number.quantize(decimal.Decimal('1'), rounding='ROUND_HALF_DOWN'))
    except TypeError as err:
        if isinstance(context, XPathSchemaContext):
            return []
        raise self.error('FORG0006', err) from None
    except decimal.InvalidOperation:
        if not isinstance(arg, str):
            assert isinstance(arg, (int, float, decimal.Decimal))
            return round(arg)
        elif isinstance(context, XPathSchemaContext):
            return []
        raise self.error('XPTY0004') from None
    except decimal.DecimalException as err:
        if isinstance(context, XPathSchemaContext):
            return []
        raise self.error('FOCA0002', err) from None

# XPath 1.0 definitions continue into module xpath1_axes
