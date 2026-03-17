from __future__ import annotations

import typing as tp

from dataclasses import dataclass

from .boxed_values import (
    Option2,
    Some2,
    None2,
)


# ---

@dataclass(frozen=True, slots=True)
class _Undefined2:
    def is_not[T](self, value: T | _Undefined2) -> tp.TypeGuard[T]:
        return value is not self


UNDEFINED: tp.Final = _Undefined2()


type MaybeUndefined2[T] = T | _Undefined2


# ---

def undefined_to_option[T](maybe_undefined_value: MaybeUndefined2[T]) -> Option2[T]:
    if maybe_undefined_value is UNDEFINED:
        return None2
    else:
        return Some2(maybe_undefined_value)  # type: ignore - `is UNDEFINED` is faster then `isinstance(obj, _Undefined2)`
