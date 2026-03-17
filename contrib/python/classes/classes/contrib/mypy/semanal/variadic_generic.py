from typing import Callable, Optional

from mypy.plugin import AnalyzeTypeContext
from mypy.types import Instance
from mypy.types import Type as MypyType

_ValidateCallback = Callable[[Instance, AnalyzeTypeContext], bool]


def analize_variadic_generic(
    ctx: AnalyzeTypeContext,
    validate_callback: Optional[_ValidateCallback] = None,
) -> MypyType:
    """
    Variadic generic support.

    What is "variadic generic"?
    It is a generic type with any amount of type variables.
    Starting with 0 up to infinity.

    We also conditionally validate types of passed arguments.
    """
    sym = ctx.api.lookup_qualified(ctx.type.name, ctx.context)  # type: ignore
    if not sym or not sym.node:
        # This will happen if `Supports[IsNotDefined]` will be called.
        return ctx.type

    instance = Instance(
        sym.node,
        ctx.api.anal_array(ctx.type.args),  # type: ignore
    )

    if validate_callback is not None:
        validate_callback(instance, ctx)
    return instance
