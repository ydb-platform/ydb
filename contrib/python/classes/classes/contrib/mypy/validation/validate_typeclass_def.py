from typing import Union

from mypy.nodes import (
    ARG_POS,
    EllipsisExpr,
    ExpressionStmt,
    FuncDef,
    PassStmt,
    StrExpr,
)
from mypy.plugin import FunctionContext, MethodContext
from mypy.types import CallableType, Instance
from typing_extensions import Final

_Contexts = Union[MethodContext, FunctionContext]

# Messages:

_AT_LEAST_ONE_ARG_MSG: Final = (
    'Typeclass definition must have at least one positional argument'
)
_FIRST_ARG_KIND_MSG: Final = (
    'First argument in typeclass definition must be positional'
)
_REDUNDANT_BODY_MSG: Final = 'Typeclass definitions must not have bodies'


def check_type(
    typeclass: Instance,
    ctx: _Contexts,
) -> bool:
    """Checks typeclass definition."""
    return all([
        _check_first_arg(typeclass, ctx),
        _check_body(typeclass, ctx),
    ])


def _check_first_arg(
    typeclass: Instance,
    ctx: _Contexts,
) -> bool:
    sig = typeclass.args[1]
    assert isinstance(sig, CallableType)

    if not len(sig.arg_kinds):
        ctx.api.fail(_AT_LEAST_ONE_ARG_MSG, ctx.context)
        return False

    if sig.arg_kinds[0] != ARG_POS:
        ctx.api.fail(_FIRST_ARG_KIND_MSG, ctx.context)
        return False
    return True


def _check_body(
    typeclass: Instance,
    ctx: _Contexts,
) -> bool:
    sig = typeclass.args[1]
    assert isinstance(sig, CallableType)
    assert isinstance(sig.definition, FuncDef)

    body = sig.definition.body.body
    if body:
        is_useless_body = (
            len(body) == 1 and
            (isinstance(body[0], PassStmt) or (
                isinstance(body[0], ExpressionStmt) and
                isinstance(body[0].expr, (EllipsisExpr, StrExpr))
            ))
        )
        if is_useless_body:
            # We allow a single ellipsis in function a body.
            # We also allow just a docstring.
            return True

        ctx.api.fail(_REDUNDANT_BODY_MSG, ctx.context)
        return False
    return True
