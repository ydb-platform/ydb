from typing import Callable

from ..common import TypeHint
from ..type_tools import is_parametrized
from ..utils import pairs
from .loc_stack_filtering import LocStack
from .location import AnyLoc, FieldLoc, InputFuncFieldLoc, TypeHintLoc


def _format_type(tp: TypeHint) -> str:
    if isinstance(tp, type) and not is_parametrized(tp):
        return tp.__qualname__
    str_tp = str(tp)
    if str_tp.startswith("typing."):
        return str_tp[7:]
    return str_tp


def format_type(tp: TypeHint, *, brackets: bool = True) -> str:
    if brackets:
        return f"‹{_format_type(tp)}›"
    return _format_type(tp)


def get_callable_name(func: Callable) -> str:
    return getattr(func, "__qualname__", None) or repr(func)


def format_loc_stack(loc_stack: LocStack[AnyLoc], *, always_wrap_with_brackets: bool = False) -> str:
    fmt_tp = _format_type(loc_stack.last.type)

    try:
        field_loc = loc_stack.last.cast(FieldLoc)
    except TypeError:
        return f"‹{fmt_tp}›" if always_wrap_with_brackets else fmt_tp
    else:
        fmt_field = f"{field_loc.field_id}: {fmt_tp}"

    if loc_stack.last.is_castable(InputFuncFieldLoc):
        return f"parameter ‹{fmt_field}›"

    if len(loc_stack) >= 2:  # noqa: PLR2004
        src_owner = _format_type(loc_stack[-2].type)
        return f"‹{src_owner}.{fmt_field}›"

    return f"‹{fmt_field}›"


def find_owner_with_field(stack: LocStack) -> tuple[TypeHintLoc, FieldLoc]:
    for next_loc, prev_loc in pairs(reversed(stack)):
        if next_loc.is_castable(FieldLoc):
            return prev_loc, next_loc
    raise ValueError
