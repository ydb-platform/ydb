from collections.abc import Iterable, Mapping
from typing import TypeAlias, cast, final

from mypy.argmap import map_actuals_to_formals
from mypy.constraints import infer_constraints_for_callable
from mypy.expandtype import expand_type
from mypy.nodes import ARG_POS, ArgKind
from mypy.plugin import FunctionContext
from mypy.types import (
    CallableType,
    FunctionLike,
    ProperType,
    TypeVarId,
    get_proper_type,
)
from mypy.types import Type as MypyType

from returns.contrib.mypy._structures.args import FuncArg
from returns.contrib.mypy._structures.types import CallableContext
from returns.contrib.mypy._typeops.analtype import analyze_call

#: Mapping of `typevar` to real type.
_Constraints: TypeAlias = Mapping[TypeVarId, MypyType]


@final
class CallableInference:
    """
    Used to infer function arguments and return type.

    There are multiple ways to do it.
    For example, one can infer argument types from its usage.
    """

    def __init__(
        self,
        case_function: CallableType,
        ctx: FunctionContext,
        *,
        fallback: CallableType | None = None,
    ) -> None:
        """
        Create the callable inference.

        Sometimes we need two functions.
        When construction one function from another
        there might be some lost information during the process.
        That's why we optionally need ``fallback``.
        If it is not provided, we treat ``case_function`` as a full one.

        Args:
            case_function: function with solved constraints.
            fallback: Function with unsolved constraints.
            ctx: Function context with checker and expr_checker objects.

        """
        self._case_function = case_function
        self._fallback = fallback or self._case_function
        self._ctx = ctx

    def from_usage(
        self,
        applied_args: list[FuncArg],
    ) -> CallableType:
        """Infers function constrains from its usage: passed arguments."""
        constraints = self._infer_constraints(applied_args)
        return expand_type(self._case_function, constraints)

    def _infer_constraints(
        self,
        applied_args: list[FuncArg],
    ) -> _Constraints:
        """Creates mapping of ``typevar`` to real type that we already know."""
        checker = self._ctx.api.expr_checker  # type: ignore
        kinds = [arg.kind for arg in applied_args]
        exprs = [arg.expression(self._ctx.context) for arg in applied_args]

        formal_to_actual = map_actuals_to_formals(
            kinds,
            [arg.name for arg in applied_args],
            self._fallback.arg_kinds,
            self._fallback.arg_names,
            lambda index: checker.accept(exprs[index]),
        )
        constraints = infer_constraints_for_callable(
            self._fallback,
            arg_types=[arg.type for arg in applied_args],
            arg_kinds=kinds,
            arg_names=[arg.name for arg in applied_args],
            formal_to_actual=formal_to_actual,
            context=checker.argument_infer_context(),
        )
        return {
            constraint.type_var: constraint.target for constraint in constraints
        }


@final
class PipelineInference:
    """
    Very helpful tool to work with functions like ``flow`` and ``pipe``.

    It iterates all over the given list of pipeline steps,
    passes the first argument, and then infers types step by step.
    """

    def __init__(self, instance: ProperType) -> None:
        """We do need the first argument to start the inference."""
        self._instance = instance

    def from_callable_sequence(
        self,
        pipeline_types: Iterable[ProperType],
        pipeline_kinds: Iterable[ArgKind],
        ctx: CallableContext,
    ) -> ProperType:
        """Pass pipeline functions to infer them one by one."""
        parameter = FuncArg(None, self._instance, ARG_POS)
        ret_type = get_proper_type(ctx.default_return_type)

        for pipeline, kind in zip(pipeline_types, pipeline_kinds, strict=False):
            ret_type = self._proper_type(
                analyze_call(
                    cast(FunctionLike, pipeline),
                    [parameter],
                    ctx,
                    show_errors=True,
                ),
            )
            parameter = FuncArg(None, ret_type, kind)
        return ret_type

    def _proper_type(self, typ: MypyType) -> ProperType:
        res_typ = get_proper_type(typ)
        if isinstance(res_typ, CallableType):
            return get_proper_type(res_typ.ret_type)
        return res_typ  # It might be `Instance` or `AnyType` or `Nothing`
