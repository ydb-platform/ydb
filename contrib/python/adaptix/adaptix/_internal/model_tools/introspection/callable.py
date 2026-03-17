import inspect
import typing
from inspect import Parameter, Signature
from types import MappingProxyType
from typing import Any, Optional

from ...common import VarTuple
from ...feature_requirement import HAS_PY_312
from ...type_tools import get_all_type_hints, normalize_type
from ..definitions import (
    DefaultValue,
    InputField,
    InputShape,
    IntrospectionError,
    NoDefault,
    Param,
    ParamKind,
    ParamKwargs,
    Shape,
)
from .typed_dict import get_typed_dict_shape

_PARAM_KIND_CONV: dict[Any, ParamKind] = {
    Parameter.POSITIONAL_ONLY: ParamKind.POS_ONLY,
    Parameter.POSITIONAL_OR_KEYWORD: ParamKind.POS_OR_KW,
    Parameter.KEYWORD_ONLY: ParamKind.KW_ONLY,
}


def _is_empty(value):
    return value is Signature.empty


def _upack_typed_dict_kwargs(
    param_kwargs: Optional[ParamKwargs],
) -> tuple[VarTuple[InputField], VarTuple[Param], Optional[ParamKwargs]]:
    if not HAS_PY_312 or param_kwargs is None:
        return (), (), param_kwargs

    try:
        norm = normalize_type(param_kwargs.type)
    except ValueError:
        return (), (), param_kwargs

    if norm.origin != typing.Unpack:
        return (), (), param_kwargs

    try:
        shape = get_typed_dict_shape(norm.args[0].source)
    except IntrospectionError:
        return (), (), param_kwargs

    return shape.input.fields, shape.input.params, shape.input.kwargs


def get_callable_shape(func, params_slice=slice(0, None)) -> Shape[InputShape, None]:
    try:
        signature = inspect.signature(func)
    except TypeError:
        raise IntrospectionError

    params = list(signature.parameters.values())[params_slice]
    kinds = [p.kind for p in params]

    if Parameter.VAR_POSITIONAL in kinds:
        raise IntrospectionError(
            f"Cannot create InputShape"
            f" from the function that has {Parameter.VAR_POSITIONAL}"
            f" parameter",
        )

    param_kwargs = next(
        (
            ParamKwargs(Any if _is_empty(param.annotation) else param.annotation)
            for param in params if param.kind == Parameter.VAR_KEYWORD
        ),
        None,
    )
    extra_fields, extra_params, param_kwargs = _upack_typed_dict_kwargs(param_kwargs)

    type_hints = get_all_type_hints(func)

    return Shape(
        input=InputShape(
            constructor=func,
            fields=tuple(
                InputField(
                    type=type_hints.get(param.name, Any),
                    id=param.name,
                    is_required=_is_empty(param.default) or param.kind == Parameter.POSITIONAL_ONLY,
                    default=NoDefault() if _is_empty(param.default) else DefaultValue(param.default),
                    metadata=MappingProxyType({}),
                    original=param,
                )
                for param in params
                if param.kind != Parameter.VAR_KEYWORD
            ) + extra_fields,
            params=tuple(
                Param(
                    field_id=param.name,
                    kind=_PARAM_KIND_CONV[param.kind],
                    name=param.name,
                )
                for param in params
                if param.kind != Parameter.VAR_KEYWORD
            ) + extra_params,
            kwargs=param_kwargs,
            overriden_types=frozenset(
                param.name
                for param in params
                if param.kind != Parameter.VAR_KEYWORD
            ),
        ),
        output=None,
    )
