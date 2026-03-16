#
# Copyright (c), 2023-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections.abc import Iterator
from copy import copy
from decimal import Decimal

from elementpath import XPath2Parser, XPathToken, XPathFunction, XPathSchemaContext, XPathNode
from elementpath.datatypes import AnyAtomicType, NumericProxy

ItemType = AnyAtomicType | str | bool | int | float | Decimal | XPathNode | XPathFunction


class SchemaFindParser(XPath2Parser):
    """
    Parser for XSD schema nodes with find/findall/iterfind API. Redefines predicate
    expression in order to select nodes also when a numeric predicate is given and
    size is equal to 1.
    """


SchemaFindParser.unregister('[')


# noinspection PyUnusedLocal
@SchemaFindParser.method('[', bp=80)
def led__predicate(self: XPathToken, left: XPathToken) -> XPathToken:
    self[:] = left, self.parser.expression()
    self.parser.advance(']')
    return self


@SchemaFindParser.method('[')
def select__predicate(self: XPathToken, context: XPathSchemaContext | None = None) \
        -> Iterator[ItemType]:
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
            elif context.size == 1 and isinstance(predicate[0], int) and predicate[0] > 1:
                yield context.item
        elif self.boolean_value(predicate):
            yield context.item
