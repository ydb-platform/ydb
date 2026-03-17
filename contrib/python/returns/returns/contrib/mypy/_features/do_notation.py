from typing import Final

from mypy.maptype import map_instance_to_supertype
from mypy.nodes import Expression, GeneratorExpr, TypeInfo
from mypy.plugin import MethodContext
from mypy.subtypes import is_subtype
from mypy.typeops import make_simplified_union
from mypy.types import (
    AnyType,
    CallableType,
    Instance,
    TypeOfAny,
    UnionType,
    get_proper_type,
)
from mypy.types import Type as MypyType

_INVALID_DO_NOTATION_SOURCE: Final = (
    'Invalid type supplied in do-notation: expected "{0}", got "{1}"'
)
_LITERAL_GENERATOR_EXPR_REQUIRED: Final = (
    'Literal generator expression is required, not a variable or function call'
)
_IF_CONDITIONS_ARE_NOT_ALLOWED: Final = (
    'Using "if" conditions inside a generator is not allowed'
)


def analyze(ctx: MethodContext) -> MypyType:
    """
    Used to handle validation and error types in :ref:`do-notation`.

    What it does?

    1. For all types we ensure that only a single container type
       is used in a single do-notation. We don't allow mixing them.
    2. For types with error types (like ``Result``),
       it inferes what possible errors types can we have.
       The result is a ``Union`` of all possible errors.
    3. Ensures that expression passed into ``.do`` method is literal.
    4. Checks that default value is provided
       if generator expression has ``if`` conditions inside.

    """
    default_return = get_proper_type(ctx.default_return_type)
    if not ctx.args or not ctx.args[0]:
        return default_return

    expr = ctx.args[0][0]
    if not isinstance(expr, GeneratorExpr):
        ctx.api.fail(_LITERAL_GENERATOR_EXPR_REQUIRED, expr)
        return default_return
    if not isinstance(ctx.type, CallableType):
        return default_return
    if not isinstance(default_return, Instance):
        return default_return

    return _do_notation(
        expr=expr,
        type_info=ctx.type.type_object(),
        default_return_type=default_return,
        ctx=ctx,
    )


def _do_notation(
    expr: GeneratorExpr,
    type_info: TypeInfo,
    default_return_type: Instance,
    ctx: MethodContext,
) -> MypyType:
    types = []
    for seq in expr.sequences:
        error_type = _try_fetch_error_type(type_info, seq, ctx)
        if error_type is not None:
            types.append(error_type)

    _check_if_conditions(expr, ctx)

    if types:
        return default_return_type.copy_modified(
            args=[
                default_return_type.args[0],
                make_simplified_union(types),
                *default_return_type.args[2:],
            ],
        )
    return default_return_type


def _try_fetch_error_type(
    type_info: TypeInfo,
    seq: Expression,
    ctx: MethodContext,
) -> MypyType | None:
    inst = Instance(
        type_info,
        [
            AnyType(TypeOfAny.implementation_artifact)
            for _ in type_info.type_vars
        ],
    )
    typ = ctx.api.expr_checker.accept(seq)  # type: ignore
    if is_subtype(typ, inst, ignore_type_params=True):
        is_success, error_type = _extract_error_type(typ, type_info)
        if is_success:
            return error_type

    ctx.api.fail(
        _INVALID_DO_NOTATION_SOURCE.format(inst, typ),
        seq,
    )
    return None


def _extract_error_type(
    typ: MypyType,
    type_info: TypeInfo,
) -> tuple[bool, MypyType | None]:
    typ = get_proper_type(typ)
    if isinstance(typ, Instance):
        return True, _decide_error_type(
            map_instance_to_supertype(typ, type_info),
        )

    if isinstance(typ, UnionType):
        types = []
        is_success = True
        for type_item in typ.items:
            is_success, error_type = _extract_error_type(type_item, type_info)
            if error_type is not None:
                types.append(error_type)
        if is_success:
            return True, make_simplified_union(types)
    return False, None


def _decide_error_type(typ: Instance) -> MypyType | None:
    if len(typ.args) < 2:
        return None
    if isinstance(get_proper_type(typ.args[1]), AnyType):
        return None
    return typ.args[1]


def _check_if_conditions(
    expr: GeneratorExpr,
    ctx: MethodContext,
) -> None:
    if any(cond for cond in expr.condlists):
        ctx.api.fail(_IF_CONDITIONS_ARE_NOT_ALLOWED, expr)
