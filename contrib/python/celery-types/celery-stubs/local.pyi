from collections.abc import Callable
from typing import Generic, TypeAlias, TypeVar

from typing_extensions import Self

_T = TypeVar("_T")
_R = TypeVar("_R")

_Getter: TypeAlias = Callable[[_T], _R]
_Setter: TypeAlias = Callable[[_T, _R], None]

class class_property(Generic[_T, _R]):
    __get: _Getter[_T, _R]
    __set: _Setter[_T, _R] | None
    def __init__(
        self,
        getter: _Getter[_T, _R],
        setter: _Setter[_T, _R] | None = None,
    ) -> None: ...
    def __get__(self, obj: _T | None, type: type[_T] | None = None) -> _R: ...
    def __set__(self, obj: _T | None, value: _R) -> Self: ...
    def setter(self, setter: _Setter[_T, _R]) -> Self: ...
