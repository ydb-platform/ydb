"""
Typing ``pipe`` functions requires several phases.

It is pretty obvious from its usage:

1. When we pass a sequence of functions we have to reduce
   the final callable type, it is require to match the ``callable`` protocol.
   And at this point we also kinda try
   to check that all pipeline functions do match,
   but this is impossible to do 100% correctly at this point,
   because generic functions don't have a type argument
   to infer the final result

2. When we call the function, we need to check for two things.
   First, we check that passed argument fits our instance requirement.
   Second, we check that pipeline functions match.
   Now we have all arguments to do the real inference.

3. We also need to fix generic in method signature.
   It might be broken, because we add new generic arguments and return type.
   So, it is safe to reattach generic back to the function.

Here's when it works:

.. code:: python

  >>> from returns.pipeline import pipe

  >>> def first(arg: int) -> bool:
  ...     return arg > 0
  >>> def second(arg: bool) -> str:
  ...     return 'bigger' if arg else 'not bigger'

  >>> pipeline = pipe(first, second)  # `analyzed` is called
  >>> assert pipeline(1) == 'bigger'  # `signature and `infer` are called
  >>> assert pipeline(0) == 'not bigger'  # `signature and `infer` again

"""

from collections.abc import Callable

from mypy.nodes import ARG_POS
from mypy.plugin import FunctionContext, MethodContext, MethodSigContext
from mypy.types import (
    AnyType,
    CallableType,
    FunctionLike,
    Instance,
    ProperType,
    TypeOfAny,
    UnionType,
    get_proper_type,
    get_proper_types,
)
from mypy.types import Type as MypyType

from returns.contrib.mypy._typeops.analtype import translate_to_function
from returns.contrib.mypy._typeops.inference import PipelineInference
from returns.contrib.mypy._typeops.transform_callable import detach_callable


def analyze(ctx: FunctionContext) -> MypyType:
    """This hook helps when we create the pipeline from sequence of funcs."""
    default_return = get_proper_type(ctx.default_return_type)
    if not isinstance(default_return, Instance):
        return default_return

    if not ctx.arg_types[0]:  # We do require to pass `*functions` arg.
        ctx.api.fail('Too few arguments for "pipe"', ctx.context)
        return default_return

    arg_types = [arg_type[0] for arg_type in ctx.arg_types if arg_type]
    first_step, last_step = _get_pipeline_def(arg_types, ctx)
    if not isinstance(first_step, FunctionLike):
        return default_return
    if not isinstance(last_step, FunctionLike):
        return default_return

    return default_return.copy_modified(
        args=[
            # First type argument represents first function arguments type:
            _unify_type(first_step, _get_first_arg_type),
            # Second argument represents pipeline final return type:
            _unify_type(last_step, lambda case: case.ret_type),
            # Other types are just functions inside the pipeline:
            *arg_types,
        ],
    )


def infer(ctx: MethodContext) -> MypyType:
    """This hook helps when we finally call the created pipeline."""
    if not isinstance(ctx.type, Instance):
        return ctx.default_return_type

    pipeline_functions = get_proper_types(ctx.type.args[2:])
    return PipelineInference(
        get_proper_type(ctx.arg_types[0][0]),
    ).from_callable_sequence(
        pipeline_functions,
        list((ARG_POS,) * len(pipeline_functions)),
        ctx,
    )


def signature(ctx: MethodSigContext) -> CallableType:
    """Helps to fix generics in method signature."""
    return detach_callable(ctx.default_signature)


def _get_first_arg_type(case: CallableType) -> MypyType:
    """Function might not have args at all."""
    if case.arg_types:
        return case.arg_types[0]
    return AnyType(TypeOfAny.implementation_artifact)


def _unify_type(
    function: FunctionLike,
    fetch_type: Callable[[CallableType], MypyType],
) -> MypyType:
    return UnionType.make_union([fetch_type(case) for case in function.items])


def _get_pipeline_def(
    arg_types: list[MypyType],
    ctx: FunctionContext,
) -> tuple[ProperType, ProperType]:
    first_step = get_proper_type(arg_types[0])
    last_step = get_proper_type(arg_types[-1])

    if not isinstance(first_step, FunctionLike):
        first_step = translate_to_function(first_step, ctx)
    if not isinstance(last_step, FunctionLike):
        last_step = translate_to_function(last_step, ctx)
    return first_step, last_step
