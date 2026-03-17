__all__ = [
    "create_annotated_tp",
    "remove_optional_from_tp",
]

import operator
from functools import reduce
from types import NoneType, UnionType
from typing import Annotated, Any, Union, get_args, get_origin


def remove_optional_from_tp(tp: Any, /) -> Any:
    if get_origin(tp) in (Union, UnionType):
        args = tuple(arg for arg in get_args(tp) if arg is not NoneType)

        if len(args) == 1:
            return remove_optional_from_tp(args[0])

        return reduce(operator.or_, args)
    if get_origin(tp) is Annotated:
        return create_annotated_tp(
            remove_optional_from_tp(get_args(tp)[0]),
            *tp.__metadata__,
        )

    return tp


def create_annotated_tp(tp: Any, /, *annotations: Any) -> Any:
    if annotations:
        return Annotated[(tp, *annotations)]

    return tp
