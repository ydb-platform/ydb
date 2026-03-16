#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections.abc import Iterable, Iterator
from typing import Any, Optional

import elementpath.aliases as ta

from elementpath.exceptions import ElementPathValueError
from elementpath.sequences import xlist, XSequence
from elementpath.helpers import split_function_test

from elementpath.sequence_types import match_sequence_type
from .functions import XPathFunction


class XPathArray(XPathFunction):
    """
    A token for processing XPath 3.1+ arrays.
    """
    symbol = 'array'
    label = 'array'
    pattern = r'(?<!\$)\barray(?=\s*(?:\(\:.*\:\))?\s*\{(?!\:))'
    _array: Optional[list[ta.ValueType]] = None

    def __init__(self, parser: ta.XPathParserType,
                 items: Optional[Iterable[Any]] = None) -> None:
        if items is not None:
            self._array = [
                x if not isinstance(x, (tuple, list, XSequence)) else xlist(x) for x in items
            ]
        super().__init__(parser)

    def __repr__(self) -> str:
        if self._array is not None:
            return f'<{self.__class__.__name__} object at {hex(id(self))}>'
        return "<{} object (not evaluated constructor) at {}>".format(
            self.__class__.__name__, hex(id(self))
        )

    def __str__(self) -> str:
        if self._array is not None:
            return f'[{", ".join(map(repr, self._array))}]'

        items_desc = f'{len(self)} items' if len(self) != 1 else '1 item'
        if self.symbol == 'array':
            return f'not evaluated curly array constructor with {items_desc}'
        return f'not evaluated square array constructor with {items_desc}'

    def __len__(self) -> int:
        if self._array is None:
            return len(self._items)
        return len(self._array)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, XPathArray):
            if self._array is None or other._array is None:
                raise ElementPathValueError("cannot compare not evaluated arrays")
            return self._array == other._array
        return NotImplemented

    @property
    def source(self) -> str:
        if self._array is None:
            items = ', '.join(f'{tk.source}' for tk in self)
        else:
            items = ', '.join(f'{v!r}' for v in self._array)
        return f'array{{{items}}}' if self.symbol == 'array' else f'[{items}]'

    def nud(self) -> 'XPathArray':
        self.parser.advance('{')
        del self._items[:]
        if self.parser.next_token.symbol not in ('}', '(end)'):
            while True:
                self._items.append(self.parser.expression(5))
                if self.parser.next_token.symbol != ',':
                    break
                self.parser.advance()

        self.parser.advance('}')
        return self

    def evaluate(self, context: ta.ContextType = None) -> 'XPathArray':
        if self._array is not None:
            return self
        return XPathArray(self.parser, items=self._evaluate(context))

    def _evaluate(self, context: ta.ContextType = None) -> list[ta.ValueType]:
        if self.symbol == 'array':
            # A comma in a curly array constructor is the comma operator, not a delimiter.
            items: list[ta.ValueType] = []
            for tk in self._items:
                items.extend(tk.select(context))
            return items
        else:
            return [tk.evaluate(context) for tk in self._items]

    def __call__(self, *args: ta.FunctionArgType, context: ta.ContextType = None) -> ta.ValueType:
        if len(args) != 1 or not isinstance(args[0], int):
            raise self.error('XPTY0004', 'exactly one xs:integer argument is expected')

        position = args[0]
        if position <= 0:
            raise self.error('FOAY0001')

        if self._array is not None:
            items = self._array
        else:
            items = self._evaluate(context)

        try:
            return items[position - 1]
        except IndexError:
            raise self.error('FOAY0001')

    def items(self, context: ta.ContextType = None) -> list[ta.ValueType]:
        if self._array is not None:
            return self._array
        return self._evaluate(context)

    def iter_flatten(self, context: ta.ContextType = None) -> Iterator[ta.ItemType]:
        if self._array is not None:
            items = self._array
        else:
            items = self._evaluate(context)

        for item in items:
            if isinstance(item, XPathArray):
                yield from item.iter_flatten(context)
            elif isinstance(item, list):
                yield from item
            else:
                yield item

    def match_function_test(self, function_test: ta.SequenceTypesType,
                            as_argument: bool = False) -> bool:
        if isinstance(function_test, (list, tuple)):
            sequence_types = function_test
        else:
            sequence_types = split_function_test(function_test)

        if not sequence_types or not sequence_types[-1]:
            return False
        elif sequence_types[0] == '*':
            return True
        elif len(sequence_types) != 2:
            return False

        index_type, value_type = sequence_types
        if index_type.endswith(('+', '*')):
            return False

        return match_sequence_type(1, index_type) and \
            all(match_sequence_type(v, value_type, self.parser) for v in self.items())
