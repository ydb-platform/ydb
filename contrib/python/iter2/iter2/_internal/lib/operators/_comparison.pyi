from __future__ import annotations

import typing as tp
import _typeshed as tp_shed

from ..functions import Predicate


# ---

@tp.overload
def equals[T: _SupportsEqualityCheck](
    obj1: T, obj2: T,
) -> bool: ...


@tp.overload
def equals[T: _SupportsEqualityCheck](
    obj: T,
) -> Predicate[T]: ...


class _SupportsEqualityCheck(tp.Protocol):
    def __eq__(self, other: tp.Self, /) -> bool: ...


# ---

@tp.overload
def greater[T: tp_shed.SupportsRichComparison](
    obj1: T, obj2: T,
) -> bool: ...


@tp.overload
def greater[T: tp_shed.SupportsRichComparison](
    obj: T,
) -> Predicate[T]: ...


@tp.overload
def greater_or_equals[T: tp_shed.SupportsRichComparison](
    obj1: T, obj2: T,
) -> bool: ...


@tp.overload
def greater_or_equals[T: tp_shed.SupportsRichComparison](
    obj: T,
) -> Predicate[T]: ...


@tp.overload
def lesser[T: tp_shed.SupportsRichComparison](
    obj1: T, obj2: T,
) -> bool: ...


@tp.overload
def lesser[T: tp_shed.SupportsRichComparison](
    obj: T,
) -> Predicate[T]: ...


@tp.overload
def lesser_or_equals[T: tp_shed.SupportsRichComparison](
    obj1: T, obj2: T,
) -> bool: ...


@tp.overload
def lesser_or_equals[T: tp_shed.SupportsRichComparison](
    obj: T,
) -> Predicate[T]: ...
