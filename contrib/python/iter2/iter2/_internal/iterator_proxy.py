from __future__ import annotations

import typing as tp

from dataclasses import dataclass

from .interfaces.iterator2 import Iterator2

from .lib.functions import Fn1, Predicate
from .lib.boxed_values import (
    Box2,
    Option2, Some2, None2,
)
from .lib.operators import unpack_and_call

from .algorithms.plain.folding import consume_iterator
from .algorithms.plain.mapping import (
    map_items,
    map_packed_items,
)
from .algorithms.plain.tapping import (
    map_tap_items,
    map_tap_packed_items,
)
from .algorithms.plain.filtering import (
    filter_items,
    filter_none_items,
    filter_items_by_type_predicate,
    filter_items_by_type,
)
from .algorithms.plain.merging import flatten_iterable


# ---

@dataclass(frozen=True, slots=True)
class Iterator2Proxy[Item](
    Iterator2[Item],
):
    iterator: tp.Iterator[Item]

    # --- (de)lifting ---

    @classmethod
    def from_iterable[NewItem](cls, iterable: tp.Iterable[NewItem], /) -> Iterator2Proxy[NewItem]:
        return cls(iter(iterable))  # type: ignore - FIXME

    def to_iterator(self) -> tp.Iterator[Item]:
        return self.iterator

    def to_boxed_iterator(self) -> Box2[tp.Iterator[Item]]:
        return Box2(self.iterator)

    # --- is tp.Iterator[Item] ---

    def __next__(self) -> Item:
        return next(self.iterator)

    # ---

    def next(self) -> Option2[Item]:
        try:
            return Some2(next(self.iterator))
        except StopIteration:
            return None2

    # --- consuming ---

    def consume(self) -> None:
        return consume_iterator(self.iterator)

    def foreach(self, fn: Fn1[Item, tp.Any], /) -> None:
        return consume_iterator(
            map_items(self.iterator, fn)
        )

    def foreach_unpacked[*Values](
        self: Iterator2Proxy[tp.Tuple[*Values]],
        fn: tp.Callable[[*Values], tp.Any],
        /,
    ) -> None:
        return consume_iterator(
            map_packed_items(self.iterator, fn)
        )

    # --- applying ---

    def apply_raw[NewValue](self, fn: Fn1[tp.Iterable[Item], NewValue], /) -> NewValue:
        return fn(self.iterator)

    # ---

    def apply_raw_and_box[NewValue](self, fn: Fn1[tp.Iterable[Item], NewValue], /) -> Box2[NewValue]:
        return Box2(fn(self.iterator))

    # ---

    def apply_and_iter[NewItem](
        self,
        fn: Fn1[tp.Self, tp.Iterable[NewItem]],
        /,
    ) -> Iterator2Proxy[NewItem]:
        return Iterator2Proxy(iter(fn(self)))

    def apply_raw_and_iter[NewItem](
        self,
        fn: Fn1[tp.Iterable[Item], tp.Iterable[NewItem]],
        /,
    ) -> Iterator2Proxy[NewItem]:
        return self.from_iterable(fn(self))

    # --- mapping ---

    def map[NewItem](self, fn: Fn1[Item, NewItem], /) -> Iterator2Proxy[NewItem]:
        return Iterator2Proxy(
            map_items(self.iterator, fn)
        )

    def map_unpacked[*Values, NewItem](
        self: Iterator2Proxy[tp.Tuple[*Values]],
        fn: tp.Callable[[*Values], NewItem],
        /,
    ) -> Iterator2Proxy[NewItem]:
        return Iterator2Proxy(
            map_packed_items(self.iterator, fn)
        )

    def map_tap(self, fn: Fn1[Item, tp.Any], /) -> Iterator2Proxy[Item]:
        return Iterator2Proxy(
            map_tap_items(self.iterator, fn)
        )

    def map_tap_unpacked[*Values](
        self: Iterator2Proxy[tp.Tuple[*Values]],
        fn: tp.Callable[[*Values], tp.Any],
        /,
    ) -> Iterator2Proxy[tp.Tuple[*Values]]:
        return Iterator2Proxy(
            map_tap_packed_items(self.iterator, fn)
        )

    # --- filtering ---

    def filter(self, predicate: Predicate[Item], /) -> Iterator2Proxy[Item]:
        return Iterator2Proxy(
            filter_items(self.iterator, predicate)
        )

    def filter_unpacked[*Values](
        self: Iterator2Proxy[tp.Tuple[*Values]],
        predicate: tp.Callable[[*Values], bool],
        /,
    ) -> Iterator2Proxy[tp.Tuple[*Values]]:
        return Iterator2Proxy(
            filter_items(
                self.iterator,
                unpack_and_call(predicate),
            )
        )

    def filter_by_type_predicate[DesiredType](
        self,
        type_predicate: tp.Callable[[Item], tp.TypeGuard[DesiredType]],
        /,
    ) -> Iterator2Proxy[DesiredType]:
        return Iterator2Proxy(
            filter_items_by_type_predicate(self.iterator, type_predicate)
        )

    def filter_by_type[DesiredType](self, type: tp.Type[DesiredType], /) -> Iterator2Proxy[DesiredType]:
        return Iterator2Proxy(
            filter_items_by_type(self.iterator, type)
        )

    def filter_none[Value](self: Iterator2Proxy[tp.Optional[Value]]) -> Iterator2Proxy[Value]:
        return Iterator2Proxy(
            filter_none_items(self.iterator)
        )

    # --- merging ---

    def flatten[Value](self: Iterator2Proxy[tp.Iterable[Value]]) -> Iterator2Proxy[Value]:
        return Iterator2Proxy(
            flatten_iterable(self.iterator)
        )
