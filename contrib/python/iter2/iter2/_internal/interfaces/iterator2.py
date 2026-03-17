from __future__ import annotations

import typing as tp

from ..lib.functions import Fn1, Predicate
from ..lib.operators import unpack_and_call

from ..lib.boxed_values import Box2, Option2

from ..algorithms.plain.folding import consume_iterator
from ..algorithms.plain.mapping import (
    map_items,
    map_packed_items,
)
from ..algorithms.plain.tapping import (
    map_tap_items,
    map_tap_packed_items,
)
from ..algorithms.plain.filtering import (
    filter_items,
    filter_none_items,
    filter_items_by_type_predicate,
    filter_items_by_type,
)
from ..algorithms.plain.merging import (
    flatten_iterable,
)


# ---

class Iterator2[Item](tp.Protocol):

    # --- (de)lifting ---

    @classmethod
    def from_iterable[NewItem](cls, iterable: tp.Iterable[NewItem], /) -> Iterator2[NewItem]: ...

    def to_iterator(self) -> tp.Iterator[Item]: ...

    def to_iterator_in_box(self) -> Box2[tp.Iterator[Item]]:
        return Box2(self.to_iterator())

    def wrap_in_box(self) -> Box2[tp.Self]:
        return Box2(self)

    # --- is tp.Iterator[Item] ---

    def __iter__(self) -> Iterator2[Item]:
        return self

    def __next__(self) -> Item: ...

    # ---

    def next(self) -> Option2[Item]: ...

    # --- consuming ---

    def consume(self) -> None:
        return consume_iterator(self)

    def foreach(self, fn: Fn1[Item, tp.Any], /) -> None:
        return self.map(fn).consume()

    def foreach_unpacked[*Values](
        self: Iterator2[tp.Tuple[*Values]],
        fn: tp.Callable[[*Values], tp.Any],
        /,
    ) -> None:
        return self.map_unpacked(fn).consume()

    # --- applying ---

    def apply[NewValue](self, fn: Fn1[tp.Self, NewValue], /) -> NewValue:
        return fn(self)

    def apply_raw[NewValue](self, fn: Fn1[tp.Iterable[Item], NewValue], /) -> NewValue:
        return fn(self.to_iterator())

    # ---

    def apply_and_box[NewValue](self, fn: Fn1[tp.Self, NewValue], /) -> Box2[NewValue]:
        return Box2(fn(self))

    def apply_raw_and_box[NewValue](self, fn: Fn1[tp.Iterable[Item], NewValue], /) -> Box2[NewValue]:
        return Box2(fn(self.to_iterator()))

    # ---

    def apply_and_iter[NewItem](
        self,
        fn: Fn1[tp.Self, tp.Iterable[NewItem]],
        /,
    ) -> Iterator2[NewItem]:
        return self.from_iterable(fn(self))

    def apply_raw_and_iter[NewItem](
        self,
        fn: Fn1[tp.Iterable[Item], tp.Iterable[NewItem]],
        /,
    ) -> Iterator2[NewItem]:
        return self.from_iterable(fn(self))

    # --- mapping ---

    def map[NewItem](self, fn: Fn1[Item, NewItem], /) -> Iterator2[NewItem]:
        return self.from_iterable(
            map_items(self.to_iterator(), fn)
        )

    def map_unpacked[*Values, NewItem](
        self: Iterator2[tp.Tuple[*Values]],
        fn: tp.Callable[[*Values], NewItem],
        /,
    ) -> Iterator2[NewItem]:
        return self.from_iterable(
            map_packed_items(self.to_iterator(), fn)
        )

    def map_tap(self, fn: Fn1[Item, tp.Any], /) -> Iterator2[Item]:
        return self.from_iterable(
            map_tap_items(self.to_iterator(), fn)
        )

    def map_tap_unpacked[*Values](
        self: Iterator2[tp.Tuple[*Values]],
        fn: tp.Callable[[*Values], tp.Any],
        /,
    ) -> Iterator2[tp.Tuple[*Values]]:
        return self.from_iterable(
            map_tap_packed_items(self.to_iterator(), fn)
        )

    # --- filtering ---

    def filter(self, predicate: Predicate[Item], /) -> Iterator2[Item]:
        return self.from_iterable(
            filter_items(self.to_iterator(), predicate)
        )

    def filter_unpacked[*Values](
        self: Iterator2[tp.Tuple[*Values]],
        predicate: tp.Callable[[*Values], bool],
        /,
    ) -> Iterator2[tp.Tuple[*Values]]:
        return self.filter(unpack_and_call(predicate))

    def filter_by_type_predicate[DesiredType](
        self,
        type_predicate: tp.Callable[[Item], tp.TypeGuard[DesiredType]],
        /,
    ) -> Iterator2[DesiredType]:
        return self.from_iterable(
            filter_items_by_type_predicate(self.to_iterator(), type_predicate)
        )

    def filter_by_type[DesiredType](self, type: tp.Type[DesiredType], /) -> Iterator2[DesiredType]:
        return self.from_iterable(
            filter_items_by_type(self.to_iterator(), type)
        )

    def filter_none[Value](self: Iterator2[tp.Optional[Value]]) -> Iterator2[Value]:
        return self.from_iterable(
            filter_none_items(self.to_iterator())
        )

    # --- merging ---

    def flatten[Value](self: Iterator2[tp.Iterable[Value]]) -> Iterator2[Value]:
        return self.from_iterable(
            flatten_iterable(self.to_iterator())
        )
