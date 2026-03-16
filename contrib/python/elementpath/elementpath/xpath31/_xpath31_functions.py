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
XPath 3.1 implementation - part 3 (functions)
"""
import json
import locale
import math
import pathlib
import random
import re
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import product
from typing import Any, cast, Optional, NoReturn
from urllib.request import urlopen
from urllib.parse import urlsplit

import elementpath.aliases as ta

from elementpath.protocols import ElementProtocol, EtreeElementProtocol
from elementpath.exceptions import ElementPathTypeError
from elementpath.datatypes import AbstractBinary, AbstractDateTime, AnyAtomicType, \
    Base64Binary, BooleanProxy, DateTime, DoubleProxy, DoubleProxy10, Duration, \
    Language, NumericProxy, Timezone, UntypedAtomic
from elementpath.namespaces import XML_BASE, XPATH_FUNCTIONS_NAMESPACE
from elementpath.helpers import collapse_white_spaces, is_xml_codepoint, \
    escape_json_string, unescape_json_string, not_equal
from elementpath.sequences import xlist
from elementpath.etree import etree_iter_strings, is_etree_element
from elementpath.collations import CollationManager
from elementpath.compare import get_key_function, same_key
from elementpath.tree_builders import get_node_tree
from elementpath.xpath_nodes import XPathNode, DocumentNode, EtreeElementNode
from elementpath.xpath_tokens import XPathFunction, XPathConstructor, XPathMap, XPathArray
from elementpath.xpath_context import XPathSchemaContext
from elementpath.validators import validate_json_to_xml

from ._xpath31_operators import XPath31Parser

method = XPath31Parser.method
function = XPath31Parser.function

XPath31Parser.unregister('string-join')
XPath31Parser.unregister('trace')

SAFE_KEY_ATOMIC_TYPES = (
    int, Decimal, AbstractBinary, AbstractDateTime, Duration
)

TIMEZONE_MAP = {
    'UT': '00:00',
    'UTC': '00:00',
    'GMT': '00:00',
    'EST': '-05:00',
    'EDT': '-04:00',
    'CST': '-06:00',
    'CDT': '-05:00',
    'MST': '-07:00',
    'MDT': '-06:00',
    'PST': '-08:00',
    'PDT': '-07:00',
}


@XPath31Parser.constructor('numeric')
def cast_numeric_type(self: XPathConstructor, value: ta.AtomicType) -> ta.NumericType:
    if isinstance(value, NumericProxy):
        return cast(ta.NumericType, value)

    try:
        return cast(float, NumericProxy(value))  # type: ignore[arg-type]
    except ValueError as err:
        if isinstance(value, (str, UntypedAtomic)):
            raise self.error('FORG0001', err)
        raise self.error('FOCA0002', err)


@method(function('string-join', nargs=(1, 2),
                 sequence_types=('xs:anyAtomicType*', 'xs:string', 'xs:string')))
def evaluate__string_join(self: XPathFunction, context: ta.ContextType = None) -> str:
    if self.context is not None:
        context = self.context

    items = [self.string_value(s) for s in self[0].select(context)]

    if len(self) == 1:
        return ''.join(items)
    separator: str = self.get_argument(context, 1, required=True, cls=str)
    return separator.join(items)


@method(function('size', prefix='map', nargs=1,
                 sequence_types=('map(*)', 'xs:integer')))
def evaluate__map_size(self: XPathFunction, context: ta.ContextType = None) -> int:
    return len(self.get_argument(self.context or context, required=True, cls=XPathMap))


@method(function('keys', prefix='map', nargs=1,
                 sequence_types=('map(*)', 'xs:anyAtomicType*')))
def evaluate__map_keys(self: XPathFunction, context: ta.ContextType = None) \
        -> list[ta.AtomicType]:
    if self.context is not None:
        context = self.context

    map_: XPathMap = self.get_argument(context, required=True, cls=XPathMap)
    return xlist([x for x in map_.keys(context)])


@method(function('contains', prefix='map', nargs=2,
                 sequence_types=('map(*)', 'xs:anyAtomicType', 'xs:boolean')))
def evaluate__map_contains(self: XPathFunction, context: ta.ContextType = None) -> bool:
    if self.context is not None:
        context = self.context

    map_ = self.get_argument(context, required=True, cls=XPathMap)
    key = self.get_argument(context, index=1, required=True, cls=AnyAtomicType)
    if isinstance(key, float) and math.isnan(key):
        return any(isinstance(k, float) and math.isnan(k) for k in map_.keys(context))

    for k in map_.keys(context):
        try:
            if k == key:
                if isinstance(key, str) or isinstance(k, str):
                    return True
                elif isinstance(key, UntypedAtomic) ^ isinstance(k, UntypedAtomic):
                    return False
                else:
                    return True
        except TypeError:
            continue
    else:
        return False


@method(function('get', prefix='map', nargs=2,
                 sequence_types=('map(*)', 'xs:anyAtomicType', 'item()*')))
def evaluate__map_get(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.ValueType:
    if self.context is not None:
        context = self.context

    map_: XPathMap = self.get_argument(context, required=True, cls=XPathMap)
    key: AnyAtomicType = self.get_argument(
        context, index=1, required=True, cls=AnyAtomicType
    )
    return map_(key, context=context)


@method(function('put', prefix='map', nargs=3,
                 sequence_types=('map(*)', 'xs:anyAtomicType', 'item()*', 'map(*)')))
def evaluate__map_put(self: XPathFunction, context: ta.ContextType = None) -> XPathMap:
    if self.context is not None:
        context = self.context

    map_ = self.get_argument(context, required=True, cls=XPathMap)
    key = self.get_argument(context, index=1, required=True, cls=AnyAtomicType)
    value = self[2].evaluate(context)
    if value is None:
        value = []

    items = {k: v for k, v in map_.items(context) if not_equal(k, key)}
    items[key] = value
    return XPathMap(self.parser, items=items)


@method(function('remove', prefix='map', nargs=2,
                 sequence_types=('map(*)', 'xs:anyAtomicType*', 'map(*)')))
def evaluate__map_remove(self: XPathFunction, context: ta.ContextType = None) -> XPathMap:
    if self.context is not None:
        context = self.context

    map_ = self.get_argument(context, required=True, cls=XPathMap)
    keys = self[1].evaluate(context)
    if keys is None:
        return map_
    elif isinstance(keys, list):
        items = (
            (k, v) for k, v in map_.items(context) if all(not_equal(k, x) for x in keys)
        )
    else:
        items = ((k, v) for k, v in map_.items(context) if not_equal(k, keys))

    return XPathMap(self.parser, items=items)


@method(function('entry', prefix='map', nargs=2,
                 sequence_types=('xs:anyAtomicType', 'item()*', 'map(*)')))
def evaluate__map_entry(self: XPathFunction, context: ta.ContextType = None) -> XPathMap:
    if self.context is not None:
        context = self.context

    key = self.get_argument(context, required=True, cls=AnyAtomicType)
    value = self[1].evaluate(context)
    if value is None:
        value = []

    return XPathMap(self.parser, items=[(key, value)])


@method(function('merge', prefix='map', nargs=(1, 2),
                 sequence_types=('map(*)*', 'map(*)', 'map(*)')))
def evaluate__map_merge(self: XPathFunction, context: ta.ContextType = None) -> XPathMap:
    if self.context is not None:
        context = self.context

    duplicates = 'use-first'
    if len(self) > 1:
        options: XPathMap = self.get_argument(context, index=1, required=True, cls=XPathMap)
        for opt, value in options.items(context):
            if opt == 'duplicates':
                if value in ('reject', 'use-first', 'use-last', 'use-any', 'combine'):
                    duplicates = cast(str, value)
                else:
                    raise self.error('FOJS0005')

    items: dict[Any, Any] = {}
    for map_ in self[0].select(context):
        assert isinstance(map_, XPathMap)
        for k1, v in map_.items(context):
            # Speed up for certain key types or float values
            if isinstance(k1, SAFE_KEY_ATOMIC_TYPES) or \
                    isinstance(k1, float) and not math.isnan(k1):
                if k1 not in items:
                    items[k1] = v
                elif duplicates == 'reject':
                    raise self.error('FOJS0003')
                elif duplicates == 'use-last':
                    items.pop(k1)  # remove before to replace the key
                    items[k1] = v
                elif duplicates == 'combine':
                    try:
                        items[k1].append(v)
                    except AttributeError:
                        items[k1] = [items[k1], v]
                continue

            # TODO: too slow. An alternative idea is to couple with the type
            #   or an index for unsafe types, and then unpack after merge.
            for k2 in items:
                if same_key(k1, k2):
                    if duplicates == 'reject':
                        raise self.error('FOJS0003')
                    elif duplicates == 'use-last':
                        items.pop(k2)  # remove before to replace the key
                        items[k1] = v
                    elif duplicates == 'combine':
                        try:
                            items[k2].append(v)
                        except AttributeError:
                            items[k2] = [items[k2], v]
                    break
            else:
                items[k1] = v

    return XPathMap(self.parser, items)


@method(function('find', prefix='map', nargs=2,
                 sequence_types=('map(*)', 'xs:anyAtomicType', 'array(*)')))
def evaluate__map_find(self: XPathFunction, context: ta.ContextType = None) -> XPathArray:
    if self.context is not None:
        context = self.context

    key = self.get_argument(context, index=1, required=True, cls=AnyAtomicType)
    items = []

    def collect_matching_items(obj: ta.ValueType) -> None:
        if isinstance(obj, list):
            for x in obj:
                collect_matching_items(x)
        elif isinstance(obj, XPathArray):
            for y in obj.items(context):
                collect_matching_items(y)
        elif isinstance(obj, XPathMap):
            for k, v in obj.items(context):
                if k == key:
                    items.append(v)
                collect_matching_items(v)

    for item in self[0].select(context):
        collect_matching_items(item)

    return XPathArray(self.parser, items)


@method(function('for-each', prefix='map', nargs=2,
                 sequence_types=('map(*)', 'function(xs:anyAtomicType, item()*) as item()*',
                                 'item()*')))
def select__map_for_each(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    if self.context is not None:
        context = self.context

    map_: XPathMap = self.get_argument(context, required=True, cls=XPathMap)
    func: XPathFunction = self.get_argument(context, index=1, required=True, cls=XPathFunction)

    for k, v in map_.items(context):
        result = func(k, v, context=context)
        if isinstance(result, list):
            yield from result
        else:
            yield result


@method(function('size', prefix='array', nargs=1,
                 sequence_types=('array(*)', 'xs:integer')))
def evaluate__array_size(self: XPathFunction, context: ta.ContextType = None) -> int:
    return len(self.get_argument(self.context or context, required=True, cls=XPathArray))


@method(function('get', prefix='array', nargs=2,
                 sequence_types=('array(*)', 'xs:integer', 'item()*')))
def evaluate__array_get(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.ValueType:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    position: int = self.get_argument(context, index=1, required=True, cls=int)
    return array_(position, context=context)


@method(function('put', prefix='array', nargs=3,
                 sequence_types=('array(*)', 'xs:integer', 'item()*', 'array(*)')))
def evaluate__array_put(self: XPathFunction, context: ta.ContextType = None) -> XPathArray:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    position: int = self.get_argument(context, index=1, required=True, cls=int)
    member = self[2].evaluate(context)
    if member is None:
        member = []

    if position <= 0:
        raise self.error('FOAY0001')

    items = array_.items(context)
    try:
        items[position - 1] = member
    except IndexError:
        if isinstance(context, XPathSchemaContext):
            return array_
        raise self.error('FOAY0001')

    return XPathArray(self.parser, items=items)


@method(function('insert-before', prefix='array', nargs=3,
                 sequence_types=('array(*)', 'xs:integer', 'item()*', 'array(*)')))
def evaluate__array_insert_before(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    position: int = self.get_argument(context, index=1, required=True, cls=int)
    member = self[2].evaluate(context)
    if member is None:
        member = []

    items = array_.items(context)

    if position <= 0 or position > len(items) + 1:
        raise self.error('FOAY0001')

    try:
        items.insert(position - 1, member)
    except IndexError:
        if isinstance(context, XPathSchemaContext):
            return array_
        raise self.error('FOAY0001')

    return XPathArray(self.parser, items=items)


@method(function('append', prefix='array', nargs=2,
                 sequence_types=('array(*)', 'item()*', 'array(*)')))
def evaluate__array_append(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    appendage = self[1].evaluate(context)
    if appendage is None:
        appendage = []

    items = array_.items(context)
    items.append(appendage)
    return XPathArray(self.parser, items=items)


@method(function('remove', prefix='array', nargs=2,
                 sequence_types=('array(*)', 'xs:integer*', 'array(*)')))
def evaluate__array_remove(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    positions_ = self[1].evaluate(context)
    if positions_ is None:
        return array_

    positions: list[int] = []
    for p in positions_ if isinstance(positions_, list) else [positions_]:
        if isinstance(p, int) and 0 < p <= len(array_):
            positions.append(p)
        elif isinstance(context, XPathSchemaContext):
            return array_
        elif not isinstance(p, int):
            raise self.error('XPTY0004')
        else:
            raise self.error('FOAY0001')

    items = (v for k, v in enumerate(array_.items(context), 1) if k not in positions)
    return XPathArray(self.parser, items=items)


@method(function('subarray', prefix='array', nargs=(2, 3),
                 sequence_types=('array(*)', 'xs:integer', 'xs:integer', 'array(*)')))
def evaluate__array_subarray(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    start: int = self.get_argument(context, index=1, required=True, cls=int)
    if start < 1 or start > len(array_) + 1:
        if isinstance(context, XPathSchemaContext):
            return array_
        raise self.error('FOAY0001')

    if len(self) > 2:
        length = self.get_argument(context, index=2, required=True, cls=int)
        if length < 0:
            raise self.error('FOAY0002')
        if start + length > len(array_) + 1:
            raise self.error('FOAY0001')
        items = array_.items(context)[start - 1:start + length - 1]
    else:
        items = array_.items(context)[start - 1:]

    return XPathArray(self.parser, items=items)


@method(function('head', prefix='array', nargs=1,
                 sequence_types=('array(*)', 'item()*')))
def evaluate__array_head(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrMore[ta.ItemType]:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    items = array_.items(context)
    if not items:
        if isinstance(context, XPathSchemaContext):
            return array_
        raise self.error('FOAY0001')
    return cast(ta.ItemType, items[0])


@method(function('tail', prefix='array', nargs=1,
                 sequence_types=('array(*)', 'array(*)')))
def evaluate__array_tail(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    items = array_.items(context)
    if not items:
        if isinstance(context, XPathSchemaContext):
            return array_
        raise self.error('FOAY0001')
    return XPathArray(self.parser, items=items[1:])


@method(function('reverse', prefix='array', nargs=1,
                 sequence_types=('array(*)', 'array(*)')))
def evaluate__array_reverse(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    array_: XPathArray
    array_ = self.get_argument(context, required=True, cls=XPathArray)
    items = array_.items(context)
    return XPathArray(self.parser, items=reversed(items))


@method(function('join', prefix='array', nargs=1,
                 sequence_types=('array(*)', 'array(*)')))
def evaluate__array_join(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    items = []
    for array_ in self[0].select(context):
        if not isinstance(array_, XPathArray):
            raise self.error('XPTY0004')
        items.extend(array_.items(context))

    return XPathArray(self.parser, items=items)


@method(function('flatten', prefix='array', nargs=1,
                 sequence_types=('item()*', 'item()*')))
def evaluate__array_flatten(self: XPathFunction, context: ta.ContextType = None) \
        -> list[ta.ItemType]:
    if self.context is not None:
        context = self.context

    items: xlist[ta.ItemType] = xlist()
    for obj in self[0].select(context):
        if isinstance(obj, XPathArray):
            items.extend(obj.iter_flatten(context))
        else:
            items.append(obj)

    return items


@method(function('for-each', prefix='array', nargs=2,
                 sequence_types=('array(*)', 'function(item()*) as item()*', 'array(*)')))
def evaluate__array_for_each(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    func: XPathFunction = self.get_argument(context, index=1, required=True, cls=XPathFunction)
    items = array_.items(context)
    return XPathArray(self.parser, items=map(lambda x: func(x, context=context), items))


@method(function('for-each-pair', prefix='array', nargs=3,
                 sequence_types=('array(*)', 'array(*)',
                                 'function(item()*, item()*) as item()*', 'array(*)')))
def evaluate__array_for_each_pair(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    array1: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    array2: XPathArray = self.get_argument(context, index=1, required=True, cls=XPathArray)
    func: XPathFunction = self.get_argument(context, index=2, required=True, cls=XPathFunction)
    items = zip(array1.items(context), array2.items(context))
    return XPathArray(self.parser, items=map(lambda x: func(*x, context=context), items))


@method(function('filter', prefix='array', nargs=2,
                 sequence_types=('array(*)', 'function(item()*) as xs:boolean', 'array(*)')))
def evaluate__array_filter(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    func: XPathFunction = self.get_argument(context, index=1, required=True, cls=XPathFunction)
    items = array_.items(context)

    def filter_function(x: ta.FunctionArgType) -> bool:
        choice = func(x, context=context)
        if not isinstance(choice, bool):
            raise self.error('XPTY0004', f'{func} must return xs:boolean values')
        return choice

    return XPathArray(self.parser, items=filter(filter_function, items))


@method(function('fold-left', prefix='array', nargs=3,
                 sequence_types=('array(*)', 'item()*',
                                 'function(item()*, item()) as item()*', 'item()*')))
@method(function('fold-right', prefix='array', nargs=3,
                 sequence_types=('array(*)', 'item()*',
                                 'function(item()*, item()) as item()*', 'item()*')))
def select__array_fold_left_right_functions(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    if self.context is not None:
        context = self.context

    func = self[2][1] if self[2].symbol == ':' else self[2]
    if not isinstance(func, XPathFunction):
        func = self.get_argument(context, index=2, cls=XPathFunction, required=True)
    if func.arity != 2:
        raise self.error('XPTY0004', "function arity must be 2")

    assert isinstance(func, XPathFunction)
    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)
    zero = self.get_argument(context, index=1)

    result = zero

    if self.symbol == 'fold-left':
        for item in array_.items(context):
            result = func(result, item, context=context)
    else:
        for item in reversed(array_.items(context)):
            result = func(item, result, context=context)

    if isinstance(result, list):
        yield from result
    else:
        yield result


@method(function('sort', nargs=(1, 3),
                 sequence_types=('item()*', 'xs:string?',
                                 'function(item()) as xs:anyAtomicType*', 'item()*')))
def evaluate__sort(self: XPathFunction, context: ta.ContextType = None) -> ta.ValueType:
    if self.context is not None:
        context = self.context

    if len(self) < 2:
        collation = self.parser.default_collation
    else:
        collation = self.get_argument(context, 1, cls=str)
        if collation is None:
            collation = self.parser.default_collation

    if len(self) == 3:
        func = self.get_argument(context, index=2, required=True, cls=XPathFunction)
        key_function = get_key_function(
            collation, key_func=lambda x: func(x, context=context), token=self
        )
    else:
        key_function = get_key_function(collation, token=self)

    try:
        return xlist(sorted(self[0].select(context), key=key_function))
    except ElementPathTypeError:
        raise
    except TypeError:
        if isinstance(context, XPathSchemaContext):
            return []
        raise self.error('XPTY0004')


@method(function('sort', prefix='array', nargs=(1, 3),
                 sequence_types=('array(*)', 'xs:string?',
                                 'function(item()*) as xs:anyAtomicType*', 'array(*)')))
def evaluate__array_sort(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathArray:
    if self.context is not None:
        context = self.context

    array_: XPathArray = self.get_argument(context, required=True, cls=XPathArray)

    if len(self) < 2:
        collation = self.parser.default_collation
    else:
        collation = self.get_argument(context, 1, cls=str)
        if collation is None:
            collation = self.parser.default_collation

    if len(self) == 3:
        func: XPathFunction
        func = self.get_argument(context, index=2, required=True, cls=XPathFunction)
        key_function = get_key_function(
            collation, key_func=lambda x: func(x, context=context), token=self
        )
    else:
        key_function = get_key_function(collation, token=self)

    try:
        items = sorted(array_.items(context), key=key_function)
    except ElementPathTypeError:
        raise
    except TypeError:
        if isinstance(context, XPathSchemaContext):
            return array_
        raise self.error('XPTY0004')
    else:
        return XPathArray(self.parser, items)


@method(function('json-doc', nargs=(1, 2),
                 sequence_types=('xs:string?', 'map(*)', 'item()?')))
@method(function('parse-json', nargs=(1, 2),
                 sequence_types=('xs:string?', 'map(*)', 'item()?')))
def evaluate__parse_json_functions(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[ta.ItemType]:
    if self.symbol == 'json-doc':
        href = self.get_argument(context, cls=str)
        if href is None:
            return []

        try:
            if urlsplit(href).scheme:
                with urlopen(href) as fp:
                    json_text = fp.read().decode('utf-8')
            else:
                with pathlib.Path(href).open() as fp:
                    json_text = fp.read()
        except IOError:
            raise self.error('FOUT1170') from None

    else:
        href = None
        json_text = self.get_argument(context, cls=str)
        if json_text is None:
            return []

    def _fallback(*a: Any, **kw: Any) -> str:
        return '\uFFFD'

    liberal = False
    duplicates = 'use-first'
    escape = None
    fallback: Callable[..., str] = _fallback

    if len(self) > 1:
        map_ = self.get_argument(context, index=1, required=True, cls=XPathMap)
        for k, v in map_.items(context):
            if k == 'liberal':
                if not isinstance(v, bool):
                    raise self.error('XPTY0004')
                liberal = v
            elif k == 'duplicates':
                if not isinstance(v, str):
                    raise self.error('XPTY0004')
                elif v not in ('use-first', 'use-last', 'reject'):
                    raise self.error('FOJS0005')
                duplicates = v
            elif k == 'escape':
                if not isinstance(v, bool):
                    raise self.error('XPTY0004')
                escape = v
            elif k == 'fallback':
                if not isinstance(v, XPathFunction):
                    msg = 'fallback parameter is not a function'
                    raise self.error('XPTY0004', msg)
                elif v.arity != 1:
                    msg = f'fallback function has arity {v.arity} (must be 1)'
                    raise self.error('XPTY0004', msg)
                elif escape:
                    msg = "cannot provide both 'fallback' and 'escape' parameters"
                    raise self.error('FOJS0005', msg)

                fallback = cast(Callable[..., str], v)
                escape = False

    def decode_value(value: ta.OneOrMore[ta.ItemType]) -> ta.OneOrEmpty[ta.ItemType]:
        if value is None:
            return []
        elif isinstance(value, list):
            return XPathArray(self.parser, [decode_value(x) for x in value])
        elif not isinstance(value, str):
            return value
        elif escape:
            return json.dumps(value, ensure_ascii=True)[1:-1].replace('\\"', '"')

        return ''.join(
            x if is_xml_codepoint(ord(x)) else fallback(rf'\u{ord(x):04X}', context=context)
            for x in value
        )

    def json_object_pairs_to_map(obj: Iterable[tuple[str, ta.OneOrMore[ta.ItemType]]]) -> XPathMap:
        items: dict[ta.ItemType, ta.OneOrMore[ta.ItemType]] = {}
        key: Any
        value: Any

        for item in obj:
            key, value = decode_value(item[0]), decode_value(item[1])
            if key in items:
                if duplicates == 'use-first':
                    continue
                elif duplicates == 'reject':
                    raise self.error('FOJS0003')

            if isinstance(value, list):
                values: Any = [decode_value(x) for x in value]
                items[key] = XPathArray(self.parser, values) if values else values
            else:
                items[key] = value

        return XPathMap(self.parser, items)

    kwargs: dict[str, Any] = {'object_pairs_hook': json_object_pairs_to_map}
    if liberal or escape:
        kwargs['strict'] = False
    if liberal:
        def parse_constant(s: str) -> None:
            raise self.error('FOJS0001')

        kwargs['parse_constant'] = parse_constant

    try:
        result = json.JSONDecoder(**kwargs).decode(json_text)
    except json.JSONDecodeError:
        if href and urlsplit(href).fragment:
            raise self.error('FOUT1170') from None
        raise self.error('FOJS0001') from None
    else:
        return decode_value(result)


@method(function('load-xquery-module', nargs=(1, 2),
                 sequence_types=('xs:string', 'map(*)', 'map(*)')))
def evaluate__load_xquery_module(self: XPathFunction, context: ta.ContextType = None) \
        -> XPathMap:
    if self.context is not None:
        context = self.context

    try:
        module_uri = self.get_argument(context, required=True, cls=str)
    except TypeError:
        raise self.error('FOQM0006')

    if not module_uri:
        raise self.error('FOQM0001')

    if len(self) > 1:
        options = self.get_argument(context, index=1, required=True, cls=XPathMap)
        for k, v in options.items(context):
            if k == 'xquery-version':
                if not isinstance(v, (int, float, Decimal)):
                    raise self.error('FOQM0005')
            elif k == 'location-hints':
                if not isinstance(v, str) or \
                        not (isinstance(v, list) and all(isinstance(x, str) for x in v)):
                    raise self.error('FOQM0005')
            elif k == 'context-item':
                if isinstance(v, list) and len(v) > 1:
                    raise self.error('FOQM0005')
            elif k == 'variables' or k == 'vendor-options':
                if not isinstance(v, XPathMap) or \
                        any(not isinstance(x, str) for x in v.keys(context)):
                    raise self.error('FOQM0006')
            else:
                raise self.error('FOQM0005')

    raise self.error('FOQM0006')  # XQuery not available


@method(function('transform', nargs=1, sequence_types=('map(*)', 'map(*)')))
def evaluate__transform(self: XPathFunction, context: ta.ContextType = None) -> XPathMap:
    if self.context is not None:
        context = self.context

    options = self.get_argument(context, required=True, cls=XPathMap)
    for k, v in options.items(context):
        # Check only 'xslt-version' parameter until an effective
        # XSLT implementation will be loadable.
        if k == 'xslt-version':
            if not isinstance(v, (int, float, Decimal)):
                raise self.error('FOXT0002')

    raise self.error('FOXT0004')  # XSLT transformation has been disabled


@method(function('random-number-generator', nargs=(0, 1),
                 sequence_types=('xs:anyAtomicType?', 'map(xs:string, item())')))
def evaluate__random_number_generator(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.ItemType:
    if self.context is not None:
        context = self.context

    seed = self.get_argument(context, cls=AnyAtomicType)
    if not isinstance(seed, (int, str)):
        seed = str(seed)
    random.seed(seed)

    class Permute(XPathFunction):
        nargs = 1
        sequence_types = ('item()*', 'item()*')

        def __call__(self, *args: Any, **kwargs: Any) -> list[ta.ItemType] | list[NoReturn]:
            if not args:
                return []

            try:
                seq = [x for x in args[0]]
            except TypeError:
                return [args[0]]
            else:
                random.shuffle(seq)
                return seq

    class NextRandom(XPathFunction):
        nargs = 0
        sequence_types = ('map(xs:string, item())',)

        def __call__(self, *args: Any, **kwargs: Any) -> XPathMap:
            items = {
                'number': random.random(),
                'next': NextRandom(self.parser),
                'permute': Permute(self.parser),
            }
            return XPathMap(self.parser, items)

    return NextRandom(self.parser)()


@method(function('apply', nargs=2,
                 sequence_types=('function(*)', 'array(*)', 'item()*')))
def evaluate__apply(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.ValueType:
    if self.context is not None:
        context = self.context

    if isinstance(self[0], XPathFunction):
        func = self[0]
    else:
        func = self.get_argument(context, required=True, cls=XPathFunction)

    array_ = self.get_argument(context, index=1, required=True, cls=XPathArray)

    try:
        return func(*array_.items(context), context=context)
    except ElementPathTypeError as err:
        if err.code is None or not err.code.endswith(('XPST0017', 'XPTY0004')):
            raise
        raise self.error('FOAP0001') from None


@method(function('parse-ietf-date', nargs=1,
                 sequence_types=('xs:string?', 'xs:dateTime?')))
def evaluate__parse_ietf_date(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneItemOrEmpty:
    if self.context is not None:
        context = self.context

    value = self.get_argument(context, cls=str)
    if value is None:
        return []

    # Normalize the input
    value = collapse_white_spaces(value)
    value = value.replace(' -', '-').replace('- ', '-').replace(' +', '+')
    value = value.replace(' (', '(').replace('( ', '(').replace(' )', ')')

    # timezone +/-NN:N is invalid
    if re.search(r'(?<=[+\-])(\d{2}:\d)(?=\D)', value) is not None:
        raise self.error('FORG0010')

    # Minutes must be 2 digits
    if re.search(r' \d{1,2}:\d(?=\D)', value) is not None:
        raise self.error('FORG0010')

    # Adjust timezone part
    value = re.sub(r'(?<=\D)(\d)(?=\D)', '0\\g<1>', value)
    value = re.sub(r'(?<=\d[+\-])(\d{2}:)(?=($|[ (]))', '\\g<1>00', value)
    value = re.sub(r'(?<=\d[+\-])(\d{2})(?=($|[ (]))', '\\g<1>:00', value)
    value = re.sub(r'(?<=\d[+\-])(\d{3})(?=[ (])', '0\\g<1>', value)

    tzname_regex = r'(?<=[\d( ])(UT|UTC|GMT|EST|EDT|CST|CDT|MST|MDT|PST|PDT)\b'
    tzname_match = re.search(tzname_regex, value, re.IGNORECASE)
    if tzname_match is not None:
        # only to let be parsed by strptime()
        value = re.sub(tzname_regex, 'UTC', value, flags=re.IGNORECASE)

    illegal_tzname_regex = r'\b(CET)\b'
    if re.search(illegal_tzname_regex, value, re.IGNORECASE) is not None:
        raise self.error('FORG0010', 'illegal timezone name')

    if value and value[0].isalpha():
        # Parse dayname part (that is then ignored)
        try:
            dayname, _value = value.split(' ', maxsplit=1)
        except ValueError:
            raise self.error('FORG0010') from None
        else:
            if dayname.endswith(','):
                dayname = dayname[:-1]

            for fmt in ['%A', '%a']:
                try:
                    datetime.strptime(dayname, fmt)
                except ValueError:
                    pass
                else:
                    value = _value
                    break

    # Parse 24:00 cases
    if ' 24:00 ' in value:
        value = value.replace(' 24:00 ', ' 00:00 ')
        day_offset = True
    elif ' 24:00:00' in value and ' 24:00:00.' not in value:
        value = value.replace(' 24:00:00', ' 00:00:00')
        day_offset = True
    else:
        day_offset = False

    # Parsing generating every combination
    if value and value[0].isalpha():
        # Parse asctime rule
        fmt_alternatives = (
            ['%b %d %H:%M', '%b-%d %H:%M'],
            ['', ':%S', ':%S.%f'],
            ['', '%Z', ' %Z', '%z', '%z(%Z)'],
            [' %Y', ' %y'],
        )
        # Adjust 2-digits year
        value = re.sub(r'(?<= )(\d{2})$', '19\\g<1>', value)
    else:
        # Parse datespec rule
        fmt_alternatives = (
            ['%d %b ', '%d-%b-', '%d %b-', '%d-%b '],
            ['%Y %H:%M', '%y %H:%M'],
            ['', ':%S', ':%S.%f'],
            ['', '%Z', ' %Z', '%z', '%z(%Z)'],
        )
        # Adjust 2-digits year
        value = re.sub(r'(?<=[ \-])(\d{2})(?= \d{2}:\d{2})', '19\\g<1>', value)

    for fmt_chunks in product(*fmt_alternatives):
        fmt = ''.join(fmt_chunks)
        if '%f%Z' in fmt:
            continue

        try:
            dt = datetime.strptime(value, fmt)
        except ValueError:
            continue
        else:
            if tzname_match is not None and dt.tzinfo is None:
                tzname = tzname_match.group(0).upper()
                dt = dt.replace(tzinfo=Timezone.fromstring(TIMEZONE_MAP[tzname]))

            if dt.tzinfo is not None:
                offset = dt.tzinfo.utcoffset(None)
                seconds = offset.days * 86400 + offset.seconds if offset else 0
                if abs(seconds) > 14 * 3600:
                    raise self.error('FORG0010')
            if day_offset:
                dt = dt + timedelta(seconds=86400)

            return DateTime.fromdatetime(dt)
    else:
        raise self.error('FORG0010')


@method(function('contains-token', nargs=(2, 3),
                 sequence_types=('xs:string*', 'xs:string', 'xs:string', 'xs:boolean')))
def evaluate__contains_token(self: XPathFunction, context: ta.ContextType = None) -> bool:
    if self.context is not None:
        context = self.context

    token_string = self.get_argument(context, index=1, required=True, cls=str)
    token_string = token_string.strip()

    if len(self) < 3:
        collation = self.parser.default_collation
    else:
        collation = self.get_argument(context, 2, required=True, cls=str)

    with CollationManager(collation, self) as manager:
        for input_string in self[0].select(context):
            if not isinstance(input_string, str):
                raise self.error('XPTY0004')
            if any(x and manager.eq(token_string, x)
                   for x in re.split('[ \t\n\r\f\v]+', input_string)):
                return True
        else:
            return False


@method(function('collation-key', nargs=(1, 2),
                 sequence_types=('xs:string', 'xs:string', 'xs:base64Binary')))
def evaluate__collation_key(self: XPathFunction, context: ta.ContextType = None) \
        -> Base64Binary:
    if self.context is not None:
        context = self.context

    key = self.get_argument(context, required=True, cls=str)
    if len(self) > 1:
        collation = self.get_argument(context, index=1, required=True, cls=str)
    else:
        collation = self.parser.default_collation

    try:
        with CollationManager(collation, self) as manager:
            base64_key = Base64Binary.encoder(manager.strxfrm(key).encode())
            return Base64Binary(base64_key, ordered=True)
    except locale.Error:
        raise self.error('FOCH0004')


@method(function('default-language', nargs=0, sequence_types=('xs:language',)))
def evaluate__default_language(self: XPathFunction, context: ta.ContextType = None) \
        -> Language:
    if self.context is not None:
        context = self.context
    elif context is None:
        raise self.missing_context()

    if context.default_language is not None:
        return context.default_language
    lang = locale.getlocale()[0]
    return Language(lang.replace('_', '-') if lang else lang)


NULL_TAG = f'{{{XPATH_FUNCTIONS_NAMESPACE}}}null'
BOOLEAN_TAG = f'{{{XPATH_FUNCTIONS_NAMESPACE}}}boolean'
NUMBER_TAG = f'{{{XPATH_FUNCTIONS_NAMESPACE}}}number'
STRING_TAG = f'{{{XPATH_FUNCTIONS_NAMESPACE}}}string'
ARRAY_TAG = f'{{{XPATH_FUNCTIONS_NAMESPACE}}}array'
MAP_TAG = f'{{{XPATH_FUNCTIONS_NAMESPACE}}}map'
BOOLEAN_VALUES = {'true', 'false', '1', '0'}


@method(function('xml-to-json', nargs=(1, 2),
                 sequence_types=('node()?', 'map(*)', 'xs:string?')))
def evaluate__xml_to_json(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[str]:
    if self.context is not None:
        context = self.context

    input_node = self.get_argument(context, cls=XPathNode)
    if input_node is None:
        return []

    if len(self) > 1:
        options = self.get_argument(context, index=1, required=True, cls=XPathMap)
        indent = options(context, 'indent')
        if indent is not None and isinstance(indent, bool):
            raise self.error('FOJS0005')

    def elem_to_json(elements: Iterable[ElementProtocol]) -> str:
        chunks = []

        def check_attributes(*exclude: str) -> None:
            for name in child.attrib:
                if name is None or name in exclude:
                    continue
                elif name.startswith('{') and \
                        not name.startswith(f'{{{XPATH_FUNCTIONS_NAMESPACE}}}'):
                    continue
                raise self.error('FOJS0006', f"{child} has an invalid attribute {name!r}")

        def check_escapes(s: str) -> None:
            if re.search(r'(?<!\\)\\(?![urtnfb/"\\])', s):
                raise self.error('FOJS0007', f"invalid escape sequence in {s!r}")

            hex_digits = '0123456789abcdefABCDEF'
            for chunk in s.split('\\u')[1:]:
                if len(chunk) < 4 or any(x not in hex_digits for x in chunk[:4]):
                    raise self.error('FOJS0007', f"invalid unicode escape in {s!r}")

        for child in elements:
            if callable(child.tag):
                continue

            if child.tag == NULL_TAG:
                check_attributes()
                if child.text is not None:
                    msg = 'a null element cannot have a text value'
                    raise self.error('FOJS0006', msg)
                chunks.append('null')

            elif child.tag == BOOLEAN_TAG:
                check_attributes('key')
                if BooleanProxy(''.join(etree_iter_strings(child))):
                    chunks.append('true')
                else:
                    chunks.append('false')

            elif child.tag == NUMBER_TAG:
                check_attributes('key')
                value = ''.join(etree_iter_strings(child))
                try:
                    if self.parser.xsd_version == '1.0':
                        number = cast(float, DoubleProxy10(value))
                    else:
                        number = cast(float, DoubleProxy(value))
                except ValueError:
                    chunks.append('nan')
                else:
                    if math.isnan(number) or math.isinf(number):
                        msg = f'invalid number value {value!r}'
                        raise self.error('FOJS0006', msg)
                    chunks.append(str(number).rstrip('0').rstrip('.'))

            elif child.tag == STRING_TAG:
                check_attributes('key', 'escaped-key', 'escaped')
                if len(child):
                    msg = f"{child} cannot have element children"
                    raise self.error('FOJS0006', msg)

                value = ''.join(etree_iter_strings(child))
                check_escapes(value)

                escaped = child.get('escaped', '0').strip()
                if escaped not in BOOLEAN_VALUES:
                    msg = f"{child} has an invalid value for 'escaped' attribute"
                    raise self.error('FOJS0006', msg)

                value = escape_json_string(value, escaped in ('true', '1'))
                chunks.append(f'"{value}"')

            elif child.tag == ARRAY_TAG:
                check_attributes('key')
                if len(child):
                    if child.text is not None and child.text.strip() or \
                            any(e.tail and e.tail.strip() for e in child):
                        msg = f"{child} has an invalid mixed content"
                        raise self.error('FOJS0006', msg)

                chunks.append(f'[{elem_to_json(child)}]')

            elif child.tag == MAP_TAG:
                map_chunks = []
                map_keys = set()
                for e in child:
                    key = e.get('key')
                    if not isinstance(key, str):
                        msg = f'object invalid key type {type(key)}'
                        raise self.error('FOJS0006', msg)

                    check_escapes(key)

                    escaped_key = e.get('escaped-key', '0').strip()
                    if escaped_key not in BOOLEAN_VALUES:
                        msg = f"{e} has an invalid value for 'escaped-key' attribute"
                        raise self.error('FOJS0006', msg)

                    key = escape_json_string(key, escaped=escaped_key in ('true', '1'))
                    map_chunks.append(f'"{key}":{elem_to_json((e,))}')

                    unescaped_key = unescape_json_string(key)
                    if unescaped_key in map_keys:
                        msg = f"key {key!r} duplication in map after escaping"
                        raise self.error('FOJS0006', msg)
                    map_keys.add(unescaped_key)

                chunks.append('{%s}' % ','.join(map_chunks))
            else:
                msg = f'invalid element tag {child.tag!r}'
                raise self.error('FOJS0006', msg)

        return ','.join(chunks)

    if isinstance(input_node, DocumentNode):
        return elem_to_json(child.value for child in input_node
                            if isinstance(child, EtreeElementNode))
    elif isinstance(input_node, EtreeElementNode):
        return elem_to_json((input_node.value,))
    else:
        raise self.error('FOJS0006')


@method(function('json-to-xml', nargs=(1, 2),
                 sequence_types=('xs:string?', 'map(*)', 'document-node()?')))
def evaluate__json_to_xml(self: XPathFunction, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[DocumentNode]:
    if self.context is not None:
        context = self.context

    json_text = self.get_argument(context, cls=str)
    if json_text is None or isinstance(context, XPathSchemaContext):
        return []
    elif context is not None:
        etree = context.etree
    else:
        raise self.missing_context()

    def _fallback(*a: Any, **kw: Any) -> str:
        return '&#xFFFD;'

    liberal = False
    validate = False
    duplicates = None
    escape = False
    fallback: Callable[..., str] = _fallback

    if len(self) > 1:
        options = self.get_argument(context, index=1, required=True, cls=XPathMap)

        for key, value in options.items(context):
            if key == 'liberal':
                if not isinstance(value, bool):
                    raise self.error('XPTY0004')
                liberal = value

            elif key == 'duplicates':
                if not isinstance(value, str):
                    raise self.error('XPTY0004')
                elif value not in ('reject', 'retain', 'use-first'):
                    raise self.error('FOJS0005')
                duplicates = value

            elif key == 'validate':
                if not isinstance(value, bool):
                    raise self.error('XPTY0004')
                validate = value

            elif key == 'escape':
                if not isinstance(value, bool):
                    raise self.error('XPTY0004')
                escape = value

            elif key == 'fallback':
                if escape:
                    msg = "'fallback' function provided with escape=True"
                    raise self.error('FOJS0005', msg)
                if not isinstance(value, XPathFunction):
                    raise self.error('XPTY0004')
                fallback = cast(Callable[..., str], value)

            else:
                raise self.error('FOJS0005')

        if duplicates is None:
            duplicates = 'reject' if validate else 'retain'
        elif validate and duplicates == 'retain':
            raise self.error('FOJS0005')

    def escape_string(s: str) -> str:
        s = re.sub(r'\\(?!/)', r'\\\\', s)
        s = s.replace('\b', r'\b'). \
            replace('\r', r'\r'). \
            replace('\n', r'\n'). \
            replace('\t', r'\t'). \
            replace('\f', r'\f'). \
            replace('/', r'\/')
        return ''.join(
            x if is_xml_codepoint(ord(x)) else rf'\u{ord(x):04X}' for x in s
        )

    def value_to_etree(v: Optional[ta.ItemType], **attrib: str) -> ElementProtocol:
        if v is None:
            elem = etree.Element(NULL_TAG, **attrib)
        elif isinstance(v, list):
            elem = etree.Element(ARRAY_TAG, **attrib)
            for item in v:
                elem.append(value_to_etree(item))
        elif isinstance(v, bool):
            elem = etree.Element(BOOLEAN_TAG, **attrib)
            elem.text = 'true' if v else 'false'
        elif isinstance(v, (int, float)):
            elem = etree.Element(NUMBER_TAG, **attrib)
            elem.text = str(v)
        elif isinstance(v, str):
            if not escape:
                v = ''.join(x if is_xml_codepoint(ord(x)) else
                            fallback(rf'\u{ord(x):04X}', context=context) for x in v)
                elem = etree.Element(STRING_TAG, **attrib)
            else:
                v = escape_string(v)
                if '\\' in v:
                    elem = etree.Element(STRING_TAG, escaped='true', **attrib)
                else:
                    elem = etree.Element(STRING_TAG, **attrib)

            elem.text = v

        elif is_etree_element(v):
            e = cast(EtreeElementProtocol, v)
            e.attrib.update(attrib)
            return e
        else:
            raise ElementPathTypeError(f'unexpected type {type(v)}')

        return cast(ElementProtocol, elem)

    def json_object_to_etree(obj: Iterable[tuple[str, Optional[ta.ItemType]]]) -> ElementProtocol:
        keys = set()
        items = []
        for k, v in obj:
            if k not in keys:
                keys.add(k)
            elif duplicates == 'use-first':
                continue
            elif duplicates == 'reject':
                raise self.error('FOJS0003')

            if not escape:
                k = ''.join(x if is_xml_codepoint(ord(x))
                            else fallback(rf'\u{ord(x):04X}', context=context) for x in k)
                k = k.replace('"', '&#34;')
                attrib = {'key': k}
            else:
                k = escape_string(k)
                if '\\' in k:
                    attrib = {'escaped-key': 'true', 'key': k}
                else:
                    attrib = {'key': k}

            items.append(value_to_etree(v, **attrib))

        elem = etree.Element(MAP_TAG)
        for item in items:
            elem.append(item)
        return cast(ElementProtocol, elem)

    kwargs: dict[str, Any] = {'object_pairs_hook': json_object_to_etree}
    if liberal or escape:
        kwargs['strict'] = False
    if liberal:
        def parse_constant(s: Any) -> None:
            raise self.error('FOJS0001')

        kwargs['parse_constant'] = parse_constant

    etree.register_namespace('fn', XPATH_FUNCTIONS_NAMESPACE)
    try:
        if json_text.startswith('\uFEFF'):
            # Exclude BOM character
            result = json.JSONDecoder(**kwargs).decode(json_text[1:])
        else:
            result = json.JSONDecoder(**kwargs).decode(json_text)
    except json.JSONDecodeError as err:
        raise self.error('FOJS0001', str(err)) from None

    if is_etree_element(result):
        document = etree.ElementTree(result)
    else:
        document = etree.ElementTree(value_to_etree(result))

    root = document.getroot()
    if XML_BASE not in root.attrib and self.parser.base_uri:
        root.set(XML_BASE, self.parser.base_uri)

    if validate:
        validate_json_to_xml(document.getroot())

    namespaces = {'j': XPATH_FUNCTIONS_NAMESPACE}
    return cast(DocumentNode, get_node_tree(document, namespaces))


@method(function('trace', nargs=(1, 2), sequence_types=('item()*', 'xs:string', 'item()*')))
def select__trace(self: XPathFunction, context: ta.ContextType = None) \
        -> Iterator[ta.ItemType]:
    if self.context is not None:
        context = self.context

    if len(self) == 1:
        for value in self[0].select(context):
            self.parser.tracer(str(value).strip())
            yield value
    else:
        label = self.get_argument(context, index=1, cls=str)
        for value in self[0].select(context):
            self.parser.tracer('{} {}'.format(label, str(value).strip()))
            yield value
