from collections.abc import Container, Mapping
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar, Union

from ..common import TypeHint
from ..model_tools.definitions import Accessor, Default

T = TypeVar("T")


class _BaseLoc:
    def cast_or_raise(
        self,
        tp: type[T],
        exception_factory: Callable[[], Union[BaseException, type[BaseException]]],
    ) -> T:
        if type(self) in _CAST_SOURCES[tp]:
            return self  # type: ignore[return-value]
        raise exception_factory()

    def cast(self, tp: type[T]) -> T:
        return self.cast_or_raise(tp, lambda: TypeError(f"Cannot cast {self} to {tp}"))

    def is_castable(self, tp: type[T]) -> bool:
        return type(self) in _CAST_SOURCES[tp]


@dataclass(frozen=True)
class _TypeHintLoc(_BaseLoc):
    type: TypeHint


@dataclass(frozen=True)
class _FieldLoc(_TypeHintLoc):
    field_id: str
    default: Default
    metadata: Mapping[Any, Any] = field(hash=False)


@dataclass(frozen=True)
class _InputFieldLoc(_FieldLoc):
    is_required: bool


@dataclass(frozen=True)
class _InputFuncFieldLoc(_FieldLoc):
    func: Callable


@dataclass(frozen=True)
class _OutputFieldLoc(_FieldLoc):
    accessor: Accessor


@dataclass(frozen=True)
class _GenericParamLoc(_TypeHintLoc):
    generic_pos: int


class TypeHintLoc(_TypeHintLoc):
    pass


class FieldLoc(_FieldLoc):
    pass


class InputFieldLoc(_InputFieldLoc):
    def complement_with_func(self, func: Callable) -> "InputFuncFieldLoc":
        return InputFuncFieldLoc(
            type=self.type,
            field_id=self.field_id,
            default=self.default,
            metadata=self.metadata,
            func=func,
        )


class InputFuncFieldLoc(_InputFuncFieldLoc):
    pass


class OutputFieldLoc(_OutputFieldLoc):
    pass


class GenericParamLoc(_GenericParamLoc):
    pass


_CAST_SOURCES: dict[Any, Container[Any]] = {
    TypeHintLoc: {TypeHintLoc, FieldLoc, InputFieldLoc, OutputFieldLoc, GenericParamLoc, InputFuncFieldLoc},
    FieldLoc: {FieldLoc, InputFieldLoc, OutputFieldLoc, InputFuncFieldLoc},
    InputFieldLoc: {InputFieldLoc, InputFuncFieldLoc},
    InputFuncFieldLoc: {InputFuncFieldLoc},
    OutputFieldLoc: {OutputFieldLoc},
    GenericParamLoc: {GenericParamLoc},
}

AnyLoc = Union[TypeHintLoc, FieldLoc, InputFieldLoc, InputFuncFieldLoc, OutputFieldLoc, GenericParamLoc]
