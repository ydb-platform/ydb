"""
Common types.

.. versionadded:: 7.0.0
"""

from __future__ import annotations

from decimal import Decimal
import typing as t

from typing_extensions import Protocol


IterateeObjT = t.Union[int, str, t.List[t.Any], t.Tuple[t.Any, ...], t.Dict[t.Any, t.Any]]
NumberT = t.Union[float, int, Decimal]
NumberNoDecimalT = t.Union[float, int]
PathT = t.Union[t.Hashable, t.List[t.Hashable]]


_T_co = t.TypeVar("_T_co", covariant=True)
_T_contra = t.TypeVar("_T_contra", contravariant=True)


class SupportsMul(Protocol[_T_contra, _T_co]):
    def __mul__(self, x: _T_contra) -> _T_co: ...


class SupportsRound(Protocol[_T_co]):
    def __round__(self) -> _T_co: ...
