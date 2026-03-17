import types
import typing
from typing import Annotated, Any, ForwardRef, Generic, NewType, Protocol, TypedDict, TypeVar, Union

from ..common import TypeHint, VarTuple
from ..feature_requirement import HAS_PY_312, HAS_PY_313
from .constants import BUILTIN_ORIGIN_TO_TYPEVARS
from .fundamentals import get_generic_args, get_type_vars, strip_alias


def is_subclass_soft(cls, classinfo) -> bool:
    """Acts like builtin issubclass, but returns False instead of rising TypeError"""
    try:
        return issubclass(cls, classinfo)
    except TypeError:
        return False


_NEW_TYPE_CLS = type(NewType("", None))


def is_new_type(tp) -> bool:
    return isinstance(tp, _NEW_TYPE_CLS)


_TYPED_DICT_MCS = type(types.new_class("_TypedDictSample", (TypedDict,), {}))


def is_typed_dict_class(tp) -> bool:
    return isinstance(tp, _TYPED_DICT_MCS)


_NAMED_TUPLE_METHODS = ("_fields", "_field_defaults", "_make", "_replace", "_asdict")


def is_named_tuple_class(tp) -> bool:
    return (
        is_subclass_soft(tp, tuple)
        and all(
            hasattr(tp, attr_name)
            for attr_name in _NAMED_TUPLE_METHODS
        )
    )


def is_protocol(tp):
    if not isinstance(tp, type):
        return False

    return Protocol in tp.__bases__


def create_union(args: tuple):
    return Union[args]


def is_parametrized(tp: TypeHint) -> bool:
    return bool(get_generic_args(tp))


if HAS_PY_312:
    def is_user_defined_generic(tp: TypeHint) -> bool:
        return (
            bool(get_type_vars(tp))
            and (
                is_subclass_soft(strip_alias(tp), Generic)
                or isinstance(tp, typing.TypeAliasType)  # type: ignore[attr-defined]
            )
        )
else:
    def is_user_defined_generic(tp: TypeHint) -> bool:
        return (
            bool(get_type_vars(tp))
            and is_subclass_soft(strip_alias(tp), Generic)
        )


def is_generic(tp: TypeHint) -> bool:
    """Check if the type could be parameterized"""
    return (
        bool(get_type_vars(tp))
        or (
            strip_alias(tp) in BUILTIN_ORIGIN_TO_TYPEVARS
            and tp is not type
            and not is_parametrized(tp)
        )
        or (
            strip_alias(tp) == Annotated
            and tp != Annotated
            and is_generic(tp.__origin__)
        )
    )


def is_bare_generic(tp: TypeHint) -> bool:
    """Check if the type could be parameterized, excluding type aliases (list[T] etc.)"""
    return is_generic(tp) and not is_parametrized(tp)


def is_generic_class(cls: type) -> bool:
    """Check if the class represents a generic type.
    This function is faster than ``.is_generic()``, but it is limited to testing only classes
    """
    return (
        cls in BUILTIN_ORIGIN_TO_TYPEVARS
        or (
            issubclass(cls, Generic)  # type: ignore[arg-type]
            and bool(cls.__parameters__)  # type: ignore[attr-defined]
        )
    )


def get_type_vars_of_parametrized(tp: TypeHint) -> VarTuple[TypeVar]:
    params = get_type_vars(tp)
    if not params:
        return ()
    if isinstance(tp, type):
        if isinstance(tp, types.GenericAlias):
            return params
        return ()
    if strip_alias(tp) != tp and get_generic_args(tp) == ():
        return ()
    return params


if HAS_PY_313:
    def eval_forward_ref(namespace: dict[str, Any], forward_ref: ForwardRef):
        return forward_ref._evaluate(namespace, None, (), recursive_guard=frozenset())  # type: ignore[misc, arg-type]
else:
    def eval_forward_ref(namespace: dict[str, Any], forward_ref: ForwardRef):
        return forward_ref._evaluate(namespace, None, recursive_guard=frozenset())
