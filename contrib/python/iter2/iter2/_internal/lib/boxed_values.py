from __future__ import annotations

import typing as tp

from dataclasses import dataclass

from .functions import (
    Thunk,
    Fn1,
    Predicate,
)


# ---

_T_co = tp.TypeVar('_T_co', covariant=True)


# ---

@dataclass(frozen=True, slots=True)
class Box2[Value]:
    value: Value

    # --- Mapping ---

    def map[NewValue](self, fn: Fn1[Value, NewValue]) -> Box2[NewValue]:
        return Box2(fn(self.value))

    def map_unpacked[*Values, NewValue](
        self: Box2[tp.Tuple[*Values]],
        fn: tp.Callable[[*Values], NewValue],
    ) -> Box2[NewValue]:
        return Box2(fn(*self.value))

    # --- Filtering ---

    def filter(self, fn: Predicate[Value]) -> Option2[Value]:
        return Some2(self.value).filter(fn)

    def filter_unpacked[*Values](
        self: Box2[tp.Tuple[*Values]],
        fn: tp.Callable[[*Values], bool],
    ) -> Option2[tp.Tuple[*Values]]:
        return Some2(self.value).filter_unpacked(fn)

    def filter_by_type[DesiredType](self, type: tp.Type[DesiredType]) -> Option2[DesiredType]:
        if isinstance(self.value, type):
            return Some2(self.value)
        else:
            return None2

    def filter_by_type_predicate[DesiredType](self, fn: tp.Callable[[Value], tp.TypeGuard[DesiredType]]) -> Option2[DesiredType]:
        if fn(self.value):
            return Some2(self.value)
        else:
            return None2


# ---

class Option2(tp.Generic[_T_co]):
    @classmethod
    def from_optional[T](cls, opt_value: tp.Optional[T]) -> Option2[T]:
        return Some2(opt_value) if opt_value is not None else None2

    # ---

    def apply[NewValue](self, fn: Fn1[tp.Self, NewValue]) -> NewValue:
        return fn(self)

    def apply_and_box[NewValue](self, fn: Fn1[tp.Self, NewValue]) -> Box2[NewValue]:
        return Box2(fn(self))

    # ---

    def is_some(self) -> bool: ...
    def is_none(self) -> bool: ...
    def is_empty(self) -> bool:
        return self.is_none()

    # ---

    def value_or_raise_exception(self) -> _T_co: ...
    def value_or[NewValue](self, default: NewValue) -> _T_co | NewValue: ...
    def value_or_else[NewValue](self, default_func: Thunk[NewValue]) -> NewValue: ...

    def boxed_value_or_raise_exception(self) -> Box2[_T_co]:
        return Box2(self.value_or_raise_exception())

    def boxed_value_or[NewValue](self, default: NewValue) -> Box2[_T_co | NewValue]:
        return Box2(self.value_or(default))

    def boxed_value_or_else[NewValue](self, default_func: Thunk[NewValue]) -> Box2[NewValue]:
        return Box2(self.value_or_else(default_func))

    # ---

    def map[NewValue](self, func: Fn1[_T_co, NewValue]) -> Option2[NewValue]: ...
    def map_unpacked[*Values, NewValue](
        self: Option2[tp.Tuple[*Values]],
        func: tp.Callable[[*Values], NewValue],
    ) -> Option2[NewValue]: ...

    # ---

    def filter(self, predicate: Predicate[_T_co]) -> Option2[_T_co]: ...
    def filter_unpacked[*Values](
        self: Option2[tp.Tuple[*Values]],
        predicate: tp.Callable[[*Values], bool],
    ) -> Option2[tp.Tuple[*Values]]: ...

    def filter_by_type[DesiredType](self, type: tp.Type[DesiredType]) -> Option2[DesiredType]: ...
    def filter_by_type_predicate[DesiredType](self, fn: tp.Callable[[_T_co], tp.TypeGuard[DesiredType]]) -> Option2[DesiredType]: ...

    # ---

    def resolve[NewSuccessValue, NewFailureValue](
        self,
        some_cb: Fn1[_T_co, NewSuccessValue],
        none_cb: Thunk[NewFailureValue],
    ) -> NewSuccessValue | NewFailureValue: ...


# ---

class None2UnwrapError(Exception): ...


@dataclass(frozen=True, slots=True)
class None2Type(Option2[tp.Never]):
    def __repr__(self):
        return 'None2'

    # ---

    def is_some(self) -> bool:
        return False

    def is_none(self) -> bool:
        return True

    # ---

    def value_or_raise_exception(self) -> tp.Never:
        raise None2UnwrapError()

    def value_or[NewValue](self, default: NewValue) -> NewValue:
        return default

    def value_or_else[NewValue](self, default_func: Thunk[NewValue]) -> NewValue:
        return default_func()

    # ---

    # NOTE: tp.Never is used to prevent typechecking of meaningless operations on value proved to be None2

    def map(self, func: tp.Never) -> tp.Never:  # type: ignore
        return self  # type: ignore

    def map_unpacked(self, func: tp.Never) -> tp.Never:  # type: ignore
        return self  # type: ignore

    def filter(self, predicate: tp.Never) -> tp.Never:  # type: ignore
        return self  # type: ignore

    def filter_unpacked(self, predicate: tp.Never) -> tp.Never:  # type: ignore
        return self  # type: ignore

    def filter_by_type(self, type: tp.Never) -> tp.Never:  # type: ignore
        return self  # type: ignore

    def filter_by_type_predicate(self, type_predicate: tp.Never) -> tp.Never:  # type: ignore
        return self  # type: ignore

    # ---

    def resolve[NewValue](
        self,
        some_cb: tp.Any,
        none_cb: Thunk[NewValue],
    ) -> NewValue:
        return none_cb()


None2 = None2Type()  # Only one instance


# ---

@dataclass(frozen=True, slots=True)
class Some2[Value](Option2[Value]):
    value: Value

    # ---

    def __repr__(self):
        return f'{self.__class__.__name__}({self.value!r})'

    # ---

    def is_some(self) -> bool:
        return True

    def is_none(self) -> bool:
        return False

    # ---

    def value_or_raise_exception(self) -> Value:
        return self.value

    def value_or(self, default) -> Value:
        return self.value

    def value_or_else(self, default_func) -> Value:
        return self.value

    # ---

    def map[NewValue](self, func: Fn1[Value, NewValue]) -> Some2[NewValue]:
        return Some2(func(self.value))

    def map_unpacked[*Values, NewValue](
        self: Some2[tp.Tuple[*Values]],
        func: tp.Callable[[*Values], NewValue],
    ) -> Some2[NewValue]:
        return Some2(func(*self.value))

    # ---

    def filter(self, predicate: Predicate[Value]) -> Option2[Value]:
        if predicate(self.value):
            return self
        else:
            return None2

    def filter_unpacked[*Values](
        self: Some2[tp.Tuple[*Values]],
        predicate: tp.Callable[[*Values], bool],
    ) -> Option2[tp.Tuple[*Values]]:
        if predicate(*self.value):
            return self
        else:
            return None2

    def filter_by_type[DesiredType](self, type: tp.Type[DesiredType]) -> Option2[DesiredType]:
        if isinstance(self.value, type):
            return self  # type: ignore - is equivalent to Some2(self.value) that type checks
        else:
            return None2

    def filter_by_type_predicate[DesiredType](self, fn: tp.Callable[[Value], tp.TypeGuard[DesiredType]]) -> Option2[DesiredType]:
        if fn(self.value):
            return self  # type: ignore - is equivalent to Some2(self.value) that type checks
        else:
            return None2

    # ---

    def resolve[NewSuccessValue](
        self,
        some_cb: Fn1[Value, NewSuccessValue],
        none_cb: tp.Any,
    ) -> NewSuccessValue:
        return some_cb(self.value)
