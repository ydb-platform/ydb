import inspect
import operator
import re
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from copy import copy
from dataclasses import dataclass, replace
from functools import reduce
from inspect import isabstract, isgenerator
from re import Pattern
from typing import Any, ClassVar, Optional, TypeVar, Union, final

from ..common import TypeHint, VarTuple
from ..datastructures import ImmutableStack
from ..type_tools import (
    BaseNormType,
    NormTV,
    is_bare_generic,
    is_generic,
    is_parametrized,
    is_protocol,
    is_subclass_soft,
    normalize_type,
)
from ..type_tools.normalize_type import NotSubscribedError
from .essential import DirectMediator
from .location import AnyLoc, FieldLoc, GenericParamLoc, TypeHintLoc

LocStackT = TypeVar("LocStackT", bound="LocStack")
AnyLocT_co = TypeVar("AnyLocT_co", bound=AnyLoc, covariant=True)


class LocStack(ImmutableStack[AnyLocT_co]):
    def replace_last_type(self: LocStackT, tp: TypeHint, /) -> LocStackT:
        return self.replace_last(replace(self.last, type=tp))


class LocStackChecker(ABC):
    @abstractmethod
    def check_loc_stack(self, mediator: DirectMediator, loc_stack: LocStack) -> bool:
        ...

    @final
    def __or__(self, other: Any) -> "LocStackChecker":
        if isinstance(other, LocStackChecker):
            return OrLocStackChecker([self, other])
        return NotImplemented

    @final
    def __and__(self, other: Any) -> "LocStackChecker":
        if isinstance(other, LocStackChecker):
            return AndLocStackChecker([self, other])
        return NotImplemented

    @final
    def __xor__(self, other: Any) -> "LocStackChecker":
        if isinstance(other, LocStackChecker):
            return XorLocStackChecker([self, other])
        return NotImplemented

    @final
    def __invert__(self) -> "LocStackChecker":
        return InvertLSC(self)


class InvertLSC(LocStackChecker):
    def __init__(self, lsc: LocStackChecker):
        self._lsc = lsc

    def check_loc_stack(self, mediator: DirectMediator, loc_stack: LocStack) -> bool:
        return not self._lsc.check_loc_stack(mediator, loc_stack)


class BinOperatorLSC(LocStackChecker, ABC):
    def __init__(self, loc_stack_checkers: Iterable[LocStackChecker]):
        self._loc_stack_checkers = loc_stack_checkers

    @abstractmethod
    def _reduce(self, elements: Iterable[bool], /) -> bool:
        ...

    def check_loc_stack(self, mediator: DirectMediator, loc_stack: LocStack) -> bool:
        return self._reduce(
            loc_stack_checker.check_loc_stack(mediator, loc_stack)
            for loc_stack_checker in self._loc_stack_checkers
        )


class OrLocStackChecker(BinOperatorLSC):
    _reduce = any  # type: ignore[assignment]


class AndLocStackChecker(BinOperatorLSC):
    _reduce = all  # type: ignore[assignment]


class XorLocStackChecker(BinOperatorLSC):
    def _reduce(self, elements: Iterable[bool], /) -> bool:
        return reduce(operator.xor, elements)


class LastLocChecker(LocStackChecker, ABC):
    _expected_location: ClassVar[type]

    def __init_subclass__(cls, **kwargs):
        param_list = list(inspect.signature(cls._check_location).parameters.values())
        cls._expected_location = param_list[2].annotation

    @final
    def check_loc_stack(self, mediator: DirectMediator, loc_stack: LocStack) -> bool:
        last_loc = loc_stack.last
        if last_loc.is_castable(self._expected_location):
            return self._check_location(mediator, last_loc)
        return False

    @abstractmethod
    def _check_location(self, mediator: DirectMediator, loc: Any) -> bool:
        pass


@dataclass(frozen=True)
class ExactFieldNameLSC(LastLocChecker):
    field_id: str

    def _check_location(self, mediator: DirectMediator, loc: FieldLoc) -> bool:
        return self.field_id == loc.field_id


@dataclass(frozen=True)
class ReFieldNameLSC(LastLocChecker):
    pattern: Pattern[str]

    def _check_location(self, mediator: DirectMediator, loc: FieldLoc) -> bool:
        return self.pattern.fullmatch(loc.field_id) is not None


@dataclass(frozen=True)
class ExactTypeLSC(LastLocChecker):
    norm: BaseNormType

    def _check_location(self, mediator: DirectMediator, loc: TypeHintLoc) -> bool:
        try:
            norm = normalize_type(loc.type)
        except ValueError:
            return False
        return norm == self.norm


@dataclass(frozen=True)
class OriginSubclassLSC(LastLocChecker):
    type_: type

    def _check_location(self, mediator: DirectMediator, loc: TypeHintLoc) -> bool:
        try:
            norm = normalize_type(loc.type)
        except ValueError:
            return False
        return is_subclass_soft(norm.origin, self.type_)


@dataclass(frozen=True)
class ExactOriginLSC(LastLocChecker):
    origin: Any

    def _check_location(self, mediator: DirectMediator, loc: TypeHintLoc) -> bool:
        try:
            norm = normalize_type(loc.type)
        except ValueError:
            return False
        return norm.origin == self.origin


class VarTupleLSC(LastLocChecker):
    def _check_location(self, mediator: DirectMediator, loc: TypeHintLoc) -> bool:
        try:
            norm = normalize_type(loc.type)
        except ValueError:
            return False
        return norm.origin is tuple and len(norm.args) == 2 and norm.args[-1] == Ellipsis  # noqa: PLR2004


@dataclass(frozen=True)
class GenericParamLSC(LastLocChecker):
    pos: int

    def _check_location(self, mediator: DirectMediator, loc: GenericParamLoc) -> bool:
        return loc.generic_pos == self.pos


@dataclass(frozen=True)
class LocStackEndChecker(LocStackChecker):
    loc_stack_checkers: Sequence[LocStackChecker]

    def check_loc_stack(self, mediator: DirectMediator, loc_stack: LocStack) -> bool:
        if len(loc_stack) < len(self.loc_stack_checkers):
            return False

        for i, checker in enumerate(reversed(self.loc_stack_checkers), start=0):
            if not checker.check_loc_stack(mediator, loc_stack.reversed_slice(i)):
                return False
        return True


class AnyLocStackChecker(LocStackChecker):
    def check_loc_stack(self, mediator: DirectMediator, loc_stack: LocStack) -> bool:
        return True


Pred = Union[str, re.Pattern, type, TypeHint, LocStackChecker, "LocStackPattern"]


def _create_non_type_hint_loc_stack_checker(pred: Pred) -> Optional[LocStackChecker]:
    if isinstance(pred, str):
        if pred.isidentifier():
            return ExactFieldNameLSC(pred)  # this is only an optimization
        return ReFieldNameLSC(re.compile(pred))

    if isinstance(pred, re.Pattern):
        return ReFieldNameLSC(pred)

    if isinstance(pred, LocStackChecker):
        return pred

    if isinstance(pred, LocStackPattern):
        return pred.build_loc_stack_checker()

    return None


def _create_loc_stack_checker_by_origin(origin):
    if is_protocol(origin) or isabstract(origin):
        return OriginSubclassLSC(origin)
    return ExactOriginLSC(origin)


def create_loc_stack_checker(pred: Pred) -> LocStackChecker:
    result = _create_non_type_hint_loc_stack_checker(pred)
    if result is not None:
        return result

    try:
        norm = normalize_type(pred)
    except NotSubscribedError:
        return ExactOriginLSC(pred)
    except ValueError:
        raise ValueError(f"Cannot create LocStackChecker from {pred}")

    if isinstance(norm, NormTV):
        raise ValueError(f"Cannot create LocStackChecker from {pred} type var")  # noqa: TRY004

    if is_bare_generic(pred):
        return _create_loc_stack_checker_by_origin(norm.origin)

    if is_generic(pred):
        raise ValueError(
            f"Cannot create LocStackChecker from {pred} generic alias (parametrized generic)",
        )

    if not is_generic(norm.origin) and not is_parametrized(pred):
        return _create_loc_stack_checker_by_origin(norm.origin)   # this is only an optimization
    return ExactTypeLSC(norm)


Pat = TypeVar("Pat", bound="LocStackPattern")


_ANY = AnyLocStackChecker()


class LocStackPattern:
    def __init__(self, stack: VarTuple[LocStackChecker]):
        self._stack = stack

    @property
    def ANY(self) -> AnyLocStackChecker:  # noqa: N802
        if self._stack:
            raise AttributeError("You must access to ANY only via `P.ANY`, other usage is misleading")
        return _ANY

    @classmethod
    def _from_lsc(cls: type[Pat], lsc: LocStackChecker) -> Pat:
        return cls((lsc, ))

    def _extend_stack(self: Pat, elements: Iterable[LocStackChecker]) -> Pat:
        self_copy = copy(self)
        self_copy._stack = self._stack + tuple(elements)
        return self_copy

    def __getattr__(self: Pat, item: str) -> Pat:
        if item.startswith("__") and item.endswith("__"):
            raise AttributeError
        return self[item]

    def __getitem__(self: Pat, item: Union[Pred, VarTuple[Pred]]) -> Pat:
        if isinstance(item, tuple) or isgenerator(item):
            return self._extend_stack(
                [OrLocStackChecker([self._ensure_loc_stack_checker_from_pred(el) for el in item])],
            )
        return self._extend_stack([self._ensure_loc_stack_checker_from_pred(item)])

    def _ensure_loc_stack_checker(self: Pat, other: Union[Pat, LocStackChecker]) -> LocStackChecker:
        if isinstance(other, LocStackChecker):
            return other
        return other.build_loc_stack_checker()

    def __or__(self: Pat, other: Union[Pat, LocStackChecker]) -> Pat:
        return self._from_lsc(
            self.build_loc_stack_checker() | self._ensure_loc_stack_checker(other),
        )

    def __ror__(self: Pat, other: Union[Pat, LocStackChecker]) -> Pat:
        return self._from_lsc(
            self._ensure_loc_stack_checker(other) | self.build_loc_stack_checker(),
        )

    def __and__(self: Pat, other: Union[Pat, LocStackChecker]) -> Pat:
        return self._from_lsc(
            self.build_loc_stack_checker() & self._ensure_loc_stack_checker(other),
        )

    def __rand__(self: Pat, other: Union[Pat, LocStackChecker]) -> Pat:
        return self._from_lsc(
            self._ensure_loc_stack_checker(other) & self.build_loc_stack_checker(),
        )

    def __xor__(self: Pat, other: Union[Pat, LocStackChecker]) -> Pat:
        return self._from_lsc(
            self.build_loc_stack_checker() ^ self._ensure_loc_stack_checker(other),
        )

    def __rxor__(self: Pat, other: Union[Pat, LocStackChecker]) -> Pat:
        return self._from_lsc(
            self._ensure_loc_stack_checker(other) ^ self.build_loc_stack_checker(),
        )

    def __invert__(self: Pat) -> Pat:
        return self._from_lsc(
            ~self.build_loc_stack_checker(),
        )

    def __add__(self: Pat, other: Pat) -> Pat:
        return self._extend_stack(other._stack)

    def _ensure_loc_stack_checker_from_pred(self, pred: Any) -> LocStackChecker:
        if isinstance(pred, LocStackPattern):
            raise TypeError(
                "Cannot use LocStackPattern as predicate inside LocStackPattern."
                " If you want to combine several LocStackPattern, you can use `+` operator",
            )

        return create_loc_stack_checker(pred)

    def generic_arg(self: Pat, pos: int, pred: Pred) -> Pat:
        return self._extend_stack(
            [GenericParamLSC(pos) & self._ensure_loc_stack_checker_from_pred(pred)],
        )

    def build_loc_stack_checker(self) -> LocStackChecker:
        if len(self._stack) == 0:
            raise ValueError(
                "Cannot produce LocStackChecker from LocStackPattern without stack."
                " You need to parametrize P object with predicates.",
            )
        if len(self._stack) == 1:
            return self._stack[0]
        return LocStackEndChecker(self._stack)


P = LocStackPattern(stack=())
