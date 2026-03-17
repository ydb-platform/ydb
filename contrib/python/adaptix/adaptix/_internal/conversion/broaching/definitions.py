from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar, Union

from adaptix._internal.common import VarTuple
from adaptix._internal.model_tools.definitions import Accessor


class BasePlanElement:
    pass


PlanT = TypeVar("PlanT", bound=BasePlanElement)


@dataclass(frozen=True)
class ParameterElement(BasePlanElement):
    name: str


@dataclass(frozen=True)
class ConstantElement(BasePlanElement):
    value: Any


@dataclass(frozen=True)
class PositionalArg(Generic[PlanT]):
    element: PlanT


@dataclass(frozen=True)
class KeywordArg(Generic[PlanT]):
    key: str
    element: PlanT


@dataclass(frozen=True)
class UnpackIterable(Generic[PlanT]):
    element: PlanT


@dataclass(frozen=True)
class UnpackMapping(Generic[PlanT]):
    element: PlanT


FuncCallArg = Union[
    PositionalArg[PlanT],
    KeywordArg[PlanT],
    UnpackIterable[PlanT],
    UnpackMapping[PlanT],
]


@dataclass(frozen=True)
class FunctionElement(BasePlanElement, Generic[PlanT]):
    func: Callable[..., Any]
    args: VarTuple[FuncCallArg[PlanT]]


@dataclass(frozen=True)
class AccessorElement(BasePlanElement, Generic[PlanT]):
    target: PlanT
    accessor: Accessor
