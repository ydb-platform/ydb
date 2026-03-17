from __future__ import annotations

import weakref
from weakref import ReferenceType

from typing import (
    Iterable, Generic, Iterator, TypeVar, TYPE_CHECKING, Any, Callable,
)

# Used a generic type for any case where we need a generic type without any bounds
# (e.g. for the LinkedList interface and some super-classes/mixins).
T = TypeVar('T')


def resolve_ref(ref: ReferenceType[T] | None) -> T | None:
    return ref() if ref is not None else None


class _CaseInsensitiveString(str):
    """Case insensitive string.
    """
    __slots__ = ['str_lower']

    if TYPE_CHECKING:  # pragma: no cover
        # neither pylint nor mypy cope with str_lower being defined in __new__
        def __init__(self, s: str) -> None:
            super().__init__(s)   # type: ignore
            self.str_lower = ''

    def __new__(cls, str_):  # type: ignore
        s = str.__new__(cls, str_)
        # We cache the lower case version of the string to speed up some operations
        s.str_lower = str_.lower()
        return s

    def __hash__(self) -> int:
        return hash(self.str_lower)

    def __eq__(self, other: Any) -> Any:
        try:
            return self.str_lower == other.lower()
        except AttributeError:
            return False

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def lower(self) -> str:
        return self.str_lower


_strI = _CaseInsensitiveString


def default_field_sort_key(x: str) -> str:
    return x.lower()


class LinkedListNode(Generic[T]):

    __slots__ = ('_previous_node', 'value', 'next_node', '__weakref__')

    def __init__(self, value: T) -> None:
        self._previous_node: ReferenceType[LinkedListNode[T]] | None = None
        self.next_node: LinkedListNode[T] | None = None
        self.value = value

    @property
    def previous_node(self) -> LinkedListNode[T] | None:
        return resolve_ref(self._previous_node)

    @previous_node.setter
    def previous_node(self, node: LinkedListNode[T] | None) -> None:
        self._previous_node = weakref.ref(node) if node is not None else None

    def remove(self) -> T:
        LinkedListNode.link_nodes(self.previous_node, self.next_node)
        self.previous_node = None
        self.next_node = None
        return self.value

    def iter_next(self, *,
                  skip_current: bool | None = False
                  ) -> Iterator[LinkedListNode[T]]:
        node = self.next_node if skip_current else self
        while node:
            yield node
            node = node.next_node

    def iter_previous(self, *,
                      skip_current: bool | None = False
                      ) -> Iterator[LinkedListNode[T]]:
        node = self.previous_node if skip_current else self
        while node:
            yield node
            node = node.previous_node

    @staticmethod
    def link_nodes(previous_node: LinkedListNode[T] | None,
                   next_node: LinkedListNode[T] | None) -> None:
        if next_node:
            next_node.previous_node = previous_node
        if previous_node:
            previous_node.next_node = next_node

    @staticmethod
    def _insert_link(first_node: LinkedListNode[T] | None,
                     new_node: LinkedListNode[T],
                     last_node: LinkedListNode[T] | None,
                     ) -> None:
        LinkedListNode.link_nodes(first_node, new_node)
        LinkedListNode.link_nodes(new_node, last_node)

    def insert_before(self, new_node: LinkedListNode[T]) -> None:
        assert self is not new_node and new_node is not self.previous_node
        LinkedListNode._insert_link(self.previous_node, new_node, self)

    def insert_after(self, new_node: LinkedListNode[T]) -> None:
        assert self is not new_node and new_node is not self.next_node
        LinkedListNode._insert_link(self, new_node, self.next_node)


class LinkedList(Generic[T]):
    """Specialized linked list implementation to support the deb822 parser needs

    We deliberately trade "encapsulation" for features needed by this library
    to facilitate their implementation.  Notably, we allow nodes to leak and assume
    well-behaved calls to remove_node - because that makes it easier to implement
    components like Deb822InvalidParagraphElement.
    """

    __slots__ = ('head_node', 'tail_node', '_size')

    def __init__(self, values: Iterable[T] | None = None) -> None:
        self.head_node: LinkedListNode[T] | None = None
        self.tail_node: LinkedListNode[T] | None = None
        self._size = 0
        if values is not None:
            self.extend(values)

    def __bool__(self) -> bool:
        return self.head_node is not None

    def __len__(self) -> int:
        return self._size

    @property
    def tail(self) -> T | None:
        return self.tail_node.value if self.tail_node is not None else None

    def pop(self) -> None:
        if self.tail_node is None:
            raise IndexError('pop from empty list')
        self.remove_node(self.tail_node)

    def iter_nodes(self) -> Iterator[LinkedListNode[T]]:
        head_node = self.head_node
        if head_node is None:
            return
        yield from head_node.iter_next()

    def __iter__(self) -> Iterator[T]:
        yield from (node.value for node in self.iter_nodes())

    def __reversed__(self) -> Iterator[T]:
        tail_node = self.tail_node
        if tail_node is None:
            return
        yield from (n.value for n in tail_node.iter_previous())

    def remove_node(self, node: LinkedListNode[T]) -> None:
        if node is self.head_node:
            self.head_node = node.next_node
            if self.head_node is None:
                self.tail_node = None
        elif node is self.tail_node:
            self.tail_node = node.previous_node
            # That case should have happened in the "if node is self._head"
            # part
            assert self.tail_node is not None
        assert self._size > 0
        self._size -= 1
        node.remove()

    def insert_at_head(self, value: T) -> LinkedListNode[T]:
        if self.head_node is None:
            return self.append(value)
        return self.insert_before(value, self.head_node)

    def append(self, value: T) -> LinkedListNode[T]:
        node = LinkedListNode(value)
        if self.head_node is None:
            self.head_node = node
            self.tail_node = node
        else:
            # Primarily as a hint to mypy
            assert self.tail_node is not None
            # Optimize for lots of appends (will happen if you are reading a Packages file) by
            # inlining relevant bits of tail_node.insert_after (removing unnecessary checks and
            # linking).
            assert self.tail_node is not node
            node.previous_node = self.tail_node
            self.tail_node.next_node = node
            self.tail_node = node
        self._size += 1
        return node

    def insert_before(self,
                      value: T,
                      existing_node: LinkedListNode[T]) -> LinkedListNode[T]:
        return self.insert_node_before(LinkedListNode(value), existing_node)

    def insert_after(self,
                     value: T,
                     existing_node: LinkedListNode[T]) -> LinkedListNode[T]:
        return self.insert_node_after(LinkedListNode(value), existing_node)

    def insert_node_before(self,
                           new_node: LinkedListNode[T],
                           existing_node: LinkedListNode[T]) -> LinkedListNode[T]:
        if self.head_node is None:
            raise ValueError("List is empty; node argument cannot be valid")
        if new_node.next_node is not None or new_node.previous_node is not None:
            raise ValueError("New node must not already be inserted!")
        existing_node.insert_before(new_node)
        if existing_node is self.head_node:
            self.head_node = new_node
        self._size += 1
        return new_node

    def insert_node_after(self,
                          new_node: LinkedListNode[T],
                          existing_node: LinkedListNode[T]) -> LinkedListNode[T]:
        if self.tail_node is None:
            raise ValueError("List is empty; node argument cannot be valid")
        if new_node.next_node is not None or new_node.previous_node is not None:
            raise ValueError("New node must not already be inserted!")
        existing_node.insert_after(new_node)
        if existing_node is self.tail_node:
            self.tail_node = new_node
        self._size += 1
        return new_node

    def extend(self, values: Iterable[T]) -> None:
        for v in values:
            self.append(v)

    def clear(self) -> None:
        self.head_node = None
        self.tail_node = None
        self._size = 0


class OrderedSet:
    """A set-like object that preserves order when iterating over it

    We use this to keep track of keys in Deb822Dict, because it's much faster
    to look up if a key is in a set than in a list.
    """

    def __init__(self, iterable: Iterable[str] | None = None) -> None:

        # We implement the OrderedSet as a "Home-built" LinkedHashSet because
        # python does not provide better facilities for it.  On the flip side,
        # we can add specialized functionality on top of it like "insert after"
        # or "move to the end".
        self.__table: dict[str, LinkedListNode[str]] = {}
        self.__order: LinkedList[str] = LinkedList()
        if iterable is None:
            iterable = []
        for item in iterable:
            self.add(item)

    def add(self, item: str) -> None:
        if item not in self:
            # We rely on the dict to raise an exception if the item is unhashable
            # Unfortunately, we need to add it to the linked list first (to obtain
            # the node) which makes this a bit more cumbersome than one might have
            # hoped.
            node = self.__order.append(item)
            try:
                self.__table[item] = node
            except Exception:
                self.__order.remove_node(node)
                raise

    def remove(self, item: str) -> None:
        # The dict will raise KeyError, so we don't need to handle that
        # ourselves
        node = self.__table[item]
        del self.__table[item]
        self.__order.remove_node(node)

    def __iter__(self) -> Iterator[str]:
        # Return an iterator of items in the order they were added
        return iter(self.__order)

    def __reversed__(self) -> Iterator[str]:
        # Return an iterator of items in the opposite order they were added
        return iter(reversed(self.__order))

    def __len__(self) -> int:
        return len(self.__order)

    def __contains__(self, item: str) -> bool:
        # This is what makes OrderedSet faster than using a list to keep track
        # of keys.  Lookup in a dict is O(1) instead of O(n) for a list.
        return item in self.__table

    # ### list-like methods
    append = add

    def extend(self, iterable: Iterable[str]) -> None:
        for item in iterable:
            self.add(item)

    # ### methods specialized for Deb822 usage
    def order_last(self, item: str) -> None:
        """Re-order the given item so it is "last" in the set"""
        self._reorder(item, self.__order.append)

    def order_first(self, item: str) -> None:
        """Re-order the given item so it is "first" in the set"""
        self._reorder(item, self.__order.insert_at_head)

    def order_before(self, item: str, reference_item: str) -> None:
        """Re-order the given item so appears directly after the reference item in the sequence"""
        if item == reference_item:
            raise ValueError("Cannot re-order an item relative to itself")
        reference_node = self.__table[reference_item]
        self._reorder(item, lambda x: self.__order.insert_before(x, reference_node))

    def order_after(self, item: str, reference_item: str) -> None:
        """Re-order the given item so appears directly before the reference item in the sequence"""
        if item == reference_item:
            raise ValueError("Cannot re-order an item relative to itself")
        reference_node = self.__table[reference_item]
        self._reorder(item, lambda x: self.__order.insert_after(x, reference_node))

    def _reorder(self,
                 item: str,
                 reinserter: Callable[[str], LinkedListNode[str]],
                 ) -> None:
        node = self.__table[item]
        self.__order.remove_node(node)
        new_node = reinserter(node.value)
        self.__table[item] = new_node
