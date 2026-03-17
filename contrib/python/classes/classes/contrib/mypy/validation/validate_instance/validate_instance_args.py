from mypy.plugin import MethodContext
from mypy.types import TupleType, UninhabitedType

from classes._registry import INVALID_ARGUMENTS_MSG  # noqa: WPS436
from classes.contrib.mypy.typeops.instance_context import InstanceContext


def check_type(
    instance_context: InstanceContext,
) -> bool:
    """
    Checks that args to ``.instance`` method are correct.

    We cannot use ``@overload`` on ``.instance`` because ``mypy``
    does not correctly handle ``ctx.api.fail`` on ``@overload`` items:
    it then tries new ones, which produce incorrect results.
    So, that's why we need this custom checker.
    """
    return all([
        _check_all_args(instance_context.passed_args, instance_context.ctx),
    ])


def _check_all_args(
    passed_args: TupleType,
    ctx: MethodContext,
) -> bool:
    fake_args = [
        passed_arg
        for passed_arg in passed_args.items[1:]
        if isinstance(passed_arg, UninhabitedType)
    ]
    if not fake_args:
        ctx.api.fail(INVALID_ARGUMENTS_MSG, ctx.context)
        return False
    return True
