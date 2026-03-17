import sys
import typing
from typing import Any, ForwardRef, TypeVar

from ..common import TypeHint, VarTuple
from ..feature_requirement import HAS_PARAM_SPEC, HAS_TV_TUPLE
from .basic_utils import create_union, eval_forward_ref, is_user_defined_generic, strip_alias
from .constants import BUILTIN_ORIGIN_TO_TYPEVARS


class ImplicitParamsGetter:
    def _process_limit_element(self, type_var: TypeVar, tp: TypeHint) -> TypeHint:
        if isinstance(tp, ForwardRef):
            return eval_forward_ref(vars(sys.modules[type_var.__module__]), tp)
        return tp

    def _process_type_var(self, type_var) -> TypeHint:
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

    def get_implicit_params(self, origin) -> VarTuple[TypeHint]:
        if is_user_defined_generic(origin):
            type_vars = origin.__parameters__
        else:
            type_vars = BUILTIN_ORIGIN_TO_TYPEVARS.get(origin, ())

        return tuple(
            self._process_type_var(type_var)
            for type_var in type_vars
        )


def fill_implicit_params(tp: TypeHint) -> TypeHint:
    params = ImplicitParamsGetter().get_implicit_params(strip_alias(tp))
    if params:
        return tp[params]
    raise ValueError(f"Can not derive implicit parameters for {tp}")
