from __future__ import annotations

import sys
from collections.abc import Iterable, Sequence
from typing import (
    Any,
    ForwardRef,
    Literal,
    TypeVar,
    get_args,
    get_origin,
)

from dishka._adaptix.type_tools.basic_utils import eval_forward_ref, is_generic
from dishka.exception_base import DishkaError


def eval_maybe_forward(t: Any, wrapper: Any) -> Any:
    if isinstance(t, str):
        t = ForwardRef(t)
    elif not isinstance(t, ForwardRef):
        return t
    return eval_forward_ref(
        vars(sys.modules[wrapper.__module__]),
        t,
    )


def eval_maybe_forward_many(args: Sequence[Any], wrapper: Any) -> list[type]:
    return [
        eval_maybe_forward(t, wrapper) for t in args
    ]


class _TypeMatcher:
    def __init__(self) -> None:
        self.type_var_subst: dict[TypeVar, Any] = {}

    def _is_broader_or_same_generic(self, t1: Any, t2: Any) -> bool:
        origin1 = eval_maybe_forward(get_origin(t1) or t1, t1)
        origin2 = eval_maybe_forward(get_origin(t2) or t2, t2)
        if origin1 is not origin2:
            return False
        args1 = eval_maybe_forward_many(get_args(t1), t1)
        params1 = t1.__parameters__
        if len(args1) != len(params1):
            args1 += params1[len(args1):]
        args2 = eval_maybe_forward_many(get_args(t2), t2)
        params2 = t1.__parameters__
        if len(args2) != len(params2):
            args2 += params1[len(args2):]

        if len(args1) != len(args2):
            return False
        for a1, a2 in zip(args1, args2, strict=False):
            if not self.is_broader_or_same_type(a1, a2):
                return False
        return True

    def _is_bound_broader(self, t1: TypeVar, t2: Any) -> bool:
        bound1 = eval_maybe_forward(t1.__bound__, t1)
        if get_origin(bound1):
            raise UnsupportedGenericBoundsError(t1)
        if not isinstance(t2, TypeVar):
            origin2 = get_origin(t2)
            if origin2 == Literal:
                if self._is_literal_same_types(t2, [bound1]):
                    return True
                return False
            if origin2:
                t2 = origin2
            return issubclass(t2, bound1)
        if t2.__bound__:
            bound2 = eval_maybe_forward(t2.__bound__, t2)
            return issubclass(bound2, bound1)
        return False

    def _is_constraint_broader(self, t1: TypeVar, t2: Any) -> bool:
        constraints1 = eval_maybe_forward_many(t1.__constraints__, t1)
        if not isinstance(t2, TypeVar):
            origin2 = get_origin(t2)
            if origin2 == Literal:
                if self._is_literal_same_types(t2, constraints1):
                    return True
                return False
            return t2 in constraints1
        if t2.__constraints__:
            constraints2 = eval_maybe_forward_many(t2.__constraints__, t2)
            return set(constraints1) >= set(constraints2)
        return False

    def _is_broader_or_same_type_var(self, t1: TypeVar, t2: Any) -> bool:
        if t1 in self.type_var_subst:
            return self.type_var_subst[t1] == t2  # type: ignore[no-any-return]
        self.type_var_subst[t1] = t2

        if not t1.__bound__ and not t1.__constraints__:
            return True
        elif t1.__bound__:
            return self._is_bound_broader(t1, t2)
        else:
            return self._is_constraint_broader(t1, t2)

    def _is_literal_same_types(
        self, t_literal: Any, types_to_check: Iterable[Any],
    ) -> bool:
        literal_args = get_args(t_literal)
        if len(literal_args) > 1 or len(literal_args) == 0:
            return False
        return any(
            isinstance(literal_args[0], type_to_check)
            for type_to_check in types_to_check
        )

    def is_broader_or_same_type(self, t1: Any, t2: Any) -> bool:
        if t1 == t2:
            return True
        if get_origin(t1) is not None or is_generic(t1):
            return self._is_broader_or_same_generic(t1, t2)
        elif isinstance(t1, TypeVar):
            return self._is_broader_or_same_type_var(t1, t2)
        return False


def is_broader_or_same_type(t1: Any, t2: Any) -> bool:
    return _TypeMatcher().is_broader_or_same_type(t1, t2)


def get_typevar_replacement(t1: Any, t2: Any) -> dict[TypeVar, Any]:
    matcher = _TypeMatcher()
    matcher.is_broader_or_same_type(t1, t2)
    return matcher.type_var_subst


class UnsupportedGenericBoundsError(TypeError, DishkaError):
    def __init__(self, bounds: TypeVar) -> None:
        self.bounds = bounds

    def __str__(self) -> str:
        return f"Generic bounds are not supported: {self.bounds}"
