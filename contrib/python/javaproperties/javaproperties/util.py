from __future__ import annotations
from collections.abc import Iterable, Iterator, Mapping
import re
from typing import Generic, Optional, TypeVar

CONTINUED_RGX = re.compile(r"(?<!\\)((?:\\\\)*)\\\r?\n?\Z")

EOL_RGX = re.compile(r"\r\n?|\n")

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class LinkedList(Generic[T]):
    def __init__(self) -> None:
        self.start: Optional["LinkedListNode[T]"] = None
        self.end: Optional["LinkedListNode[T]"] = None

    def __iter__(self) -> Iterator[T]:
        return (n.value for n in self.iternodes())

    def iternodes(self) -> Iterator["LinkedListNode[T]"]:
        n = self.start
        while n is not None:
            yield n
            n = n.next

    def append(self, value: T) -> LinkedListNode[T]:
        n = LinkedListNode(value, self)
        if self.start is None:
            self.start = n
        else:
            assert self.end is not None
            self.end.next = n
            n.prev = self.end
        self.end = n
        return n

    def find_node(self, node: "LinkedListNode[T]") -> Optional[int]:
        for i, n in enumerate(self.iternodes()):
            if n is node:
                return i
        return None


class LinkedListNode(Generic[T]):
    def __init__(self, value: T, lst: LinkedList[T]) -> None:
        self.value: T = value
        self.lst: LinkedList[T] = lst
        self.prev: Optional["LinkedListNode[T]"] = None
        self.next: Optional["LinkedListNode[T]"] = None

    def unlink(self) -> None:
        if self.prev is not None:
            self.prev.next = self.next
        if self.next is not None:
            self.next.prev = self.prev
        if self is self.lst.start:
            self.lst.start = self.next
        if self is self.lst.end:
            self.lst.end = self.prev

    def insert_after(self, value: T) -> LinkedListNode[T]:
        """Inserts a new node with value ``value`` after the node ``self``"""
        n = LinkedListNode(value, self.lst)
        n.prev = self
        n.next = self.next
        self.next = n
        if n.next is not None:
            n.next.prev = n
        else:
            assert self is self.lst.end
            self.lst.end = n
        return n

    def insert_before(self, value: T) -> LinkedListNode[T]:
        """
        Inserts a new node with value ``value`` before the node ``self``
        """
        n = LinkedListNode(value, self.lst)
        n.next = self
        n.prev = self.prev
        self.prev = n
        if n.prev is not None:
            n.prev.next = n
        else:
            assert self is self.lst.start
            self.lst.start = n
        return n


def itemize(
    kvs: Mapping[K, V] | Iterable[tuple[K, V]],
    sort_keys: bool = False,
) -> Iterable[tuple[K, V]]:
    items: Iterable[tuple[K, V]]
    if isinstance(kvs, Mapping):
        items = ((k, kvs[k]) for k in kvs)
    else:
        items = kvs
    if sort_keys:
        items = sorted(items)
    return items


def ascii_splitlines(s: str) -> list[str]:
    """
    Like `str.splitlines(True)`, except it only treats LF, CR LF, and CR as
    line endings
    """
    lines = []
    lastend = 0
    for m in EOL_RGX.finditer(s):
        lines.append(s[lastend : m.end()])
        lastend = m.end()
    if lastend < len(s):
        lines.append(s[lastend:])
    return lines
