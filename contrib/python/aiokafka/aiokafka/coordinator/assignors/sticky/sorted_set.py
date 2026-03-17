from collections.abc import Callable, Collection, Iterable, Iterator
from typing import (
    Any,
    Generic,
    TypeVar,
    final,
)

T = TypeVar("T")


@final
class SortedSet(Generic[T], Collection[T]):
    def __init__(
        self,
        iterable: Iterable[T] | None = None,
        key: Callable[[T], Any] | None = None,
    ) -> None:
        self._key: Callable[[T], Any] = key if key is not None else lambda x: x
        self._set: set[T] = set(iterable) if iterable is not None else set()

        self._cached_last: T | None = None
        self._cached_first: T | None = None

    def first(self) -> T | None:
        if self._cached_first is not None:
            return self._cached_first

        first = None
        for element in self._set:
            if first is None or self._key(first) > self._key(element):
                first = element
        self._cached_first = first
        return first

    def last(self) -> T | None:
        if self._cached_last is not None:
            return self._cached_last

        last = None
        for element in self._set:
            if last is None or self._key(last) < self._key(element):
                last = element
        self._cached_last = last
        return last

    def pop_last(self) -> T:
        value = self.last()

        if value is None:
            raise KeyError

        self._set.remove(value)
        self._cached_last = None
        return value

    def add(self, value: T) -> None:
        if self._cached_last is not None and self._key(value) > self._key(
            self._cached_last
        ):
            self._cached_last = value
        if self._cached_first is not None and self._key(value) < self._key(
            self._cached_first
        ):
            self._cached_first = value

        return self._set.add(value)

    def remove(self, value: T) -> None:
        if self._cached_last is not None and self._cached_last == value:
            self._cached_last = None
        if self._cached_first is not None and self._cached_first == value:
            self._cached_first = None

        return self._set.remove(value)

    def __contains__(self, value: Any) -> bool:
        return value in self._set

    def __iter__(self) -> Iterator[T]:
        return iter(sorted(self._set, key=self._key))

    def __len__(self) -> int:
        return len(self._set)
