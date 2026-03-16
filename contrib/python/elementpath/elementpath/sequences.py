#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import operator
from collections.abc import Callable, Iterable, Sequence
from itertools import chain
from typing import Any, cast, NoReturn, overload, TypeVar, SupportsIndex, Union

from elementpath.aliases import ItemType

__all__ = ['xlist', 'XSequence', 'empty_sequence', 'sequence_concat',
           'sequence_count', 'iterate_sequence']


T = TypeVar('T', bound=ItemType)
_S = TypeVar('_S')


class xlist(list[T]):
    """An extended list type for processing XQuery/XPath sequences."""
    __slots__ = ()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, list):
            return super().__eq__(other)
        return False if len(self) != 1 else not self[0] != other

    def __ne__(self, other: Any) -> bool:
        if isinstance(other, list):
            return super().__ne__(other)
        return True if len(self) != 1 else not self[0] == other

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, list):
            return super().__lt__(other)
        elif len(self) == 1:
            return bool(self[0] < other)
        return NotImplemented

    def __le__(self, other: Any) -> bool:
        if isinstance(other, list):
            return super().__le__(other)
        elif len(self) == 1:
            return bool(self[0] <= other)
        return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, list):
            return super().__gt__(other)
        elif len(self) == 1:
            return bool(self[0] > other)
        return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, list):
            return super().__ge__(other)
        elif len(self) == 1:
            return bool(self[0] >= other)
        return NotImplemented

    @overload
    def __add__(self, other: list[T]) -> list[T]: ...

    @overload
    def __add__(self, other: list[_S]) -> list[_S | T]: ...

    def __add__(self, other: list[T] | list[_S]) -> list[T] | list[_S | T]:
        if isinstance(other, list):
            return xlist(chain(self, cast(list[T], other)))
        elif len(self) == 1:
            return xlist([self[0] + other])
        return NotImplemented

    def __radd__(self, other: list[T]) -> list[T]:
        if isinstance(other, list):
            return xlist(chain(other, self))
        return NotImplemented

    def __mul__(self, value: SupportsIndex) -> list[T]:
        return NotImplemented

    __rmul__ = __mul__

    @property
    def sequence_type(self) -> str:
        if not self:
            return 'empty-sequence()'
        elif len(self) == 1:
            return 'item()'
        else:
            return 'item()*'


###
# XDM 4.0 constructors and accessors as internal API.
def empty_sequence() -> list[NoReturn]:
    return []


def sequence_concat(s1: list[T], s2: list[T]) -> list[T]:
    match len(s1) + len(s2):
        case 0:
            return []
        case 1:
            return xlist(s1 or s2)
        case _:
            return list(chain(s1, s2))


def sequence_count(seq: list[Any]) -> int:
    return len(seq)


def iterate_sequence(seq: list[T], action: Callable[[T, int], list[T]]) -> list[T]:
    items: list[T] = []
    for position, item in enumerate(seq, start=1):
        items.extend(action(item, position))
    return items if len(items) != 1 else xlist(items)


###
# Sequence class for external usage.
#
SequenceArgType = Union[list[ItemType], tuple[ItemType], 'XSequence']
ActionArgType = Callable[[ItemType, int], list[ItemType]]


class XSequence(Sequence[ItemType]):
    """
    An immutable XPath sequence for external processing of XQuery/XPath sequences.

    Ref: https://qt4cg.org/specifications/xpath-datamodel-40/Overview.html
    """
    __slots__ = '_items',

    @classmethod
    def empty_sequence(cls) -> 'XSequence':
        return cls()

    @classmethod
    def sequence_concat(cls, *seq: SequenceArgType) -> 'XSequence':
        if any(not isinstance(x, (list, tuple, XSequence)) for x in seq):
            raise TypeError("arguments must be sequences")
        return XSequence(chain(*seq))

    @classmethod
    def sequence_count(cls, seq: SequenceArgType) -> int:
        return len(seq)

    @classmethod
    def iterate_sequence(cls, seq: SequenceArgType, action: ActionArgType) -> 'XSequence':
        items: list[ItemType] = []
        for position, item in enumerate(seq, start=1):
            items.extend(action(item, position))
        return XSequence(items)

    def __init__(self, items: Iterable[ItemType] = ()) -> None:
        self._items = tuple(items)
        if any(isinstance(x, (list, XSequence)) for x in self._items):
            raise TypeError(f"{self!r} initialized with a nested sequence")

    def __str__(self) -> str:
        return f'({", ".join(map(repr, self))})'

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}([{", ".join(map(repr, self))}])'

    def _operator(self, op: Callable[[Any, Any], bool], other: object) -> bool:
        if isinstance(other, XSequence):
            return op(self._items, other._items)
        elif isinstance(other, tuple):
            return op(self._items, other)
        elif isinstance(other, list):
            return op(self._items, tuple(other))
        elif len(self._items) == 1:
            return op(self._items[0], other)
        else:
            return cast(bool, NotImplemented)

    @overload
    def __getitem__(self, item: int) -> ItemType: ...

    @overload
    def __getitem__(self, item: slice) -> tuple[ItemType, ...]: ...

    def __getitem__(self, item: int | slice) -> ItemType | tuple[ItemType, ...]:
        return self._items[item]

    def __len__(self) -> int:
        return len(self._items)

    def __hash__(self) -> int:
        return hash(self._items)

    def __eq__(self, other: Any) -> bool:
        return self._operator(operator.eq, other)

    def __ne__(self, other: Any) -> bool:
        return not self._operator(operator.eq, other)

    def __lt__(self, other: Any) -> bool:
        return self._operator(operator.lt, other)

    def __le__(self, other: Any) -> bool:
        return self._operator(operator.le, other)

    def __gt__(self, other: Any) -> bool:
        return self._operator(operator.gt, other)

    def __ge__(self, other: Any) -> bool:
        return self._operator(operator.ge, other)

    def __add__(self, other: Any) -> 'XSequence':
        if isinstance(other, (list, tuple, XSequence)):
            return XSequence(chain(self, other))
        elif len(self) == 1:
            return XSequence([self[0] + other])
        return NotImplemented

    def __radd__(self, other: Any) -> 'XSequence':
        if isinstance(other, (list, tuple)):
            return XSequence(chain(other, self))
        return NotImplemented

    @property
    def sequence_type(self) -> str:
        if not self:
            return 'empty-sequence()'
        elif len(self) == 1:
            return 'item()'
        else:
            return 'item()*'
