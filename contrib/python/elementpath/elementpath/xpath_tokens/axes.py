#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections.abc import Iterator

import elementpath.aliases as ta
from elementpath.xpath_context import XPathContext
from .base import XPathToken


class XPathAxis(XPathToken):
    pattern = r'\b[^\d\W][\w.\-\xb7\u0300-\u036F\u203F\u2040]*(?=\s*\:\:|\s*\(\:.*\:\)\s*\:\:)'
    label = 'axis'
    reverse_axis: bool = False

    def __str__(self) -> str:
        return f'{self.symbol!r} axis'

    def nud(self) -> 'XPathAxis':
        self.parser.advance('::')
        self.parser.expected_next(
            '(name)', '*', '{', 'Q{', 'text', 'node', 'document-node',
            'comment', 'processing-instruction', 'element', 'attribute',
            'schema-attribute', 'schema-element', 'namespace-node',
        )
        self._items[:] = self.parser.expression(rbp=self.rbp),
        return self

    @property
    def source(self) -> str:
        return '%s::%s' % (self.symbol, self[0].source)

    def select_with_focus(self, context: XPathContext) -> Iterator[ta.ItemType]:
        """Select item with an inner focus on dynamic context."""
        status = context.item, context.size, context.position, context.axis
        results = [x for x in self.select(context)]
        context.item, context.size, context.position, context.axis = status
        context.axis = None

        if self.reverse_axis:
            context.size = context.position = len(results)
            for context.item in results:
                yield context.item
                context.position -= 1
        else:
            context.size = len(results)
            for context.position, context.item in enumerate(results, start=1):
                yield context.item

        context.item, context.size, context.position, context.axis = status
