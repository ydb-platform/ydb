import sys
import typing
from itertools import chain
from typing import Any, ForwardRef, TypeVar

from ..common import TypeHint, VarTuple
from ..feature_requirement import HAS_PARAM_SPEC, HAS_TV_DEFAULT, HAS_TV_TUPLE
from .basic_utils import create_union, eval_forward_ref, is_user_defined_generic, strip_alias
from .constants import BUILTIN_ORIGIN_TO_TYPEVARS


class ImplicitParamsGetter:
    def _process_limit_element(self, type_var: TypeVar, tp: TypeHint) -> TypeHint:
        if isinstance(tp, ForwardRef):
            return eval_forward_ref(vars(sys.modules[type_var.__module__]), tp)
        return tp

    def _derive_default(self, type_var) -> TypeHint:
        if HAS_PARAM_SPEC and isinstance(type_var, typing.ParamSpec):
            return ...
        if HAS_TV_TUPLE and isinstance(type_var, typing.TypeVarTuple):
            return typing.Unpack[tuple[Any, ...]]
        if type_var.__constraints__:
            return create_union(
                tuple(
                    self._process_limit_element(type_var, constraint)
                    for constraint in type_var.__constraints__
                ),
            )
        if type_var.__bound__ is None:
            return Any
        return self._process_limit_element(type_var, type_var.__bound__)

    def _get_default_tuple(self, type_var) -> VarTuple[TypeHint]:
        if HAS_TV_DEFAULT and type_var.has_default():
            if isinstance(type_var, TypeVar):
                return (type_var.__default__, )  # type: ignore[attr-defined]
            return type_var.__default__
        return (self._derive_default(type_var), )

    def get_implicit_params(self, origin) -> VarTuple[TypeHint]:
        if is_user_defined_generic(origin):
            type_vars = origin.__parameters__
        else:
            type_vars = BUILTIN_ORIGIN_TO_TYPEVARS.get(origin, ())

        return tuple(
            chain.from_iterable(
                self._get_default_tuple(type_var)
                for type_var in type_vars
            ),
        )


_getter = ImplicitParamsGetter()


def fill_implicit_params(tp: TypeHint) -> TypeHint:
    params = _getter.get_implicit_params(strip_alias(tp))
    if params:
        return tp[params]
    raise ValueError(f"Cannot derive implicit parameters for {tp}")
