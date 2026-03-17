import types
from typing import TypeVar, get_args, get_origin, get_type_hints

from ..common import TypeHint, VarTuple
from ..feature_requirement import HAS_SUPPORTED_PYDANTIC_PKG

__all__ = ("get_all_type_hints", "get_generic_args", "get_type_vars", "is_pydantic_class", "strip_alias")


if HAS_SUPPORTED_PYDANTIC_PKG:
    from pydantic import BaseModel

    _PYDANTIC_MCS = type(types.new_class("_PydanticSample", (BaseModel,), {}))

    def is_pydantic_class(tp) -> bool:
        return isinstance(tp, _PYDANTIC_MCS) and tp != BaseModel
else:
    def is_pydantic_class(tp) -> bool:
        return False


def strip_alias(tp: TypeHint) -> TypeHint:
    origin = tp.__pydantic_generic_metadata__["origin"] if is_pydantic_class(tp) else get_origin(tp)
    return tp if origin is None else origin


def get_type_vars(tp: TypeHint) -> VarTuple[TypeVar]:
    if is_pydantic_class(tp):
        return tp.__pydantic_generic_metadata__["parameters"]

    type_vars = getattr(tp, "__parameters__", ())
    # UnionType object contains descriptor inside `__parameters__`
    if not isinstance(type_vars, tuple):
        return ()
    return type_vars


def get_generic_args(tp: TypeHint) -> VarTuple[TypeHint]:
    if is_pydantic_class(tp):
        return tp.__pydantic_generic_metadata__["args"]
    return get_args(tp)


def get_all_type_hints(obj, globalns=None, localns=None):
    return get_type_hints(obj, globalns, localns, include_extras=True)
