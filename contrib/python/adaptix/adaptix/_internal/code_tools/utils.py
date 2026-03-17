# ruff: noqa: E721
import builtins
import math
from enum import Enum
from typing import Any, Optional

BUILTIN_TO_NAME = {
    (getattr(builtins, name), type(getattr(builtins, name))): name
    for name in sorted(dir(builtins))
    if not name.startswith("__") and name != "_"
}
NAME_TO_BUILTIN = {name: obj for (obj, _), name in BUILTIN_TO_NAME.items()}


class _CannotBeRenderedError(Exception):
    pass


def get_literal_expr(obj: object) -> Optional[str]:
    if type(obj) in (int, str, bytes, bytearray):
        return repr(obj)
    if type(obj) is float:
        if math.isinf(obj) or math.isnan(obj):
            return None
        return repr(obj)

    try:
        name = BUILTIN_TO_NAME[obj, type(obj)]
    except (KeyError, TypeError):
        try:
            return _get_complex_literal_expr(obj)
        except _CannotBeRenderedError:
            return None

    return name


def _provide_lit_expr(obj: object) -> str:
    literal_repr = get_literal_expr(obj)
    if literal_repr is None:
        raise _CannotBeRenderedError
    return literal_repr


def _parenthesize(parentheses: str, elements) -> str:
    return parentheses[0] + ", ".join(map(_provide_lit_expr, elements)) + parentheses[1]


def _try_sort(iterable):
    try:
        return sorted(iterable)
    except TypeError:
        return iterable


def _get_complex_literal_expr(obj: object) -> Optional[str]:  # noqa: PLR0911, C901
    if type(obj) is list:
        return _parenthesize("[]", obj)

    if type(obj) is tuple:
        if len(obj) == 1:
            return f"({_provide_lit_expr(obj[0])}, )"
        return _parenthesize("()", obj)

    if type(obj) is set:
        if obj:
            return _parenthesize("{}", _try_sort(obj))
        return "set()"

    if type(obj) is frozenset:
        if obj:
            return "frozenset(" + _parenthesize("{}", _try_sort(obj)) + ")"
        return "frozenset()"

    if type(obj) is slice:
        parts = (obj.start, obj.step, obj.stop)
        return "slice" + _parenthesize("()", parts)

    if type(obj) is range:
        parts = (obj.start, obj.step, obj.stop)
        return "range" + _parenthesize("()", parts)

    if type(obj) is dict:
        body = ", ".join(
            f"{_provide_lit_expr(key)}: {_provide_lit_expr(value)}"
            for key, value in obj.items()
        )
        return "{" + body + "}"

    return None


_CLS_TO_FACTORY_LITERAL: dict[Any, str] = {
    list: "[]",
    dict: "{}",
    tuple: "()",
    str: '""',
    bytes: 'b""',
    type(None): "None",
}


def get_literal_from_factory(obj: object) -> Optional[str]:
    try:
        return _CLS_TO_FACTORY_LITERAL.get(obj)
    except TypeError:
        return None


_SINGLETONS = {None, Ellipsis, NotImplemented}


def is_singleton(obj: object) -> bool:
    return obj in _SINGLETONS or isinstance(obj, (bool, Enum))
