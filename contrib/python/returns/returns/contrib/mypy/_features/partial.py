from collections.abc import Iterator
from itertools import starmap
from typing import Final, final

from mypy.nodes import ARG_STAR, ARG_STAR2
from mypy.plugin import FunctionContext
from mypy.types import (
    CallableType,
    FunctionLike,
    Instance,
    Overloaded,
    ProperType,
    TypeType,
    get_proper_type,
)

from returns.contrib.mypy._structures.args import FuncArg
from returns.contrib.mypy._typeops.analtype import (
    analyze_call,
    safe_translate_to_function,
)
from returns.contrib.mypy._typeops.inference import CallableInference
from returns.contrib.mypy._typeops.transform_callable import (
    Functions,
    Intermediate,
    detach_callable,
    proper_type,
)

_SUPPORTED_TYPES: Final = (
    CallableType,
    Instance,
    TypeType,
    Overloaded,
)


def analyze(ctx: FunctionContext) -> ProperType:
    """
    This hook is used to make typed curring a thing in `returns` project.

    This plugin is a temporary solution to the problem.
    It should be later replaced with the official way of doing things.
    One day functions will have better API and we plan
    to submit this plugin into ``mypy`` core plugins,
    so it would not be required.

    Internally we just reduce the original function's argument count.
    And drop some of them from function's signature.
    """
    default_return = get_proper_type(ctx.default_return_type)
    if not isinstance(default_return, CallableType):
        return default_return

    function_def = get_proper_type(ctx.arg_types[0][0])
    func_args = _AppliedArgs(ctx)

    if len(list(filter(len, ctx.arg_types))) == 1:
        return function_def  # this means, that `partial(func)` is called
    if not isinstance(function_def, _SUPPORTED_TYPES):
        return default_return
    if isinstance(function_def, Instance | TypeType):
        # We force `Instance` and similar types to coercse to callable:
        function_def = func_args.get_callable_from_context()

    is_valid, applied_args = func_args.build_from_context()
    if not isinstance(function_def, CallableType | Overloaded) or not is_valid:
        return default_return

    return _PartialFunctionReducer(
        default_return,
        function_def,
        applied_args,
        ctx,
    ).new_partial()


@final
class _PartialFunctionReducer:
    """
    Helper object to work with curring.

    Here's a quick overview of things that is going on inside:

    1. Firstly we create intermediate callable that represents a subset
       of argument that are passed with the ``curry`` call
    2. Then, we run typechecking on this intermediate function
       and passed arguments to make sure that everything is correct
    3. Then, we subtract intermediate arguments from the passed function
    4. Finally we run type substitution on newly created final function
       to replace generic vars we already know to make sure
       that everything still works and the number of type vars is reduced

    This plugin requires several things:

    - One should now how ``ExpressionChecker`` from ``mypy`` works
    - What ``FunctionLike`` is
    - How kinds work in type checking
    - What ``map_actuals_to_formals`` is
    - How constraints work

    That's not an easy plugin to work with.
    """

    def __init__(
        self,
        default_return_type: FunctionLike,
        original: FunctionLike,
        applied_args: list[FuncArg],
        ctx: FunctionContext,
    ) -> None:
        """
        Saving the things we need.

        Args:
            default_return_type: default callable type got by ``mypy``.
            original: passed function to be curried.
            applied_args: arguments that are already provided in the definition.
            ctx: plugin hook context provided by ``mypy``.

        """
        self._default_return_type = default_return_type
        self._original = original
        self._applied_args = applied_args
        self._ctx = ctx

        self._case_functions: list[CallableType] = []
        self._fallbacks: list[CallableType] = []

    def new_partial(self) -> ProperType:
        """
        Creates new partial functions.

        Splits passed functions into ``case_function``
        where each overloaded spec is processed inducidually.
        Then we combine everything back together removing unfit parts.
        """
        for case_function in self._original.items:
            fallback, intermediate = self._create_intermediate(case_function)
            self._fallbacks.append(fallback)

            if intermediate:
                partial = self._create_partial_case(
                    case_function,
                    intermediate,
                    fallback,
                )
                self._case_functions.append(partial)
        return self._create_new_partial()

    def _create_intermediate(
        self,
        case_function: CallableType,
    ) -> tuple[CallableType, CallableType | None]:
        intermediate = Intermediate(case_function).with_applied_args(
            self._applied_args,
        )
        return intermediate, analyze_call(
            intermediate,
            self._applied_args,
            self._ctx,
            show_errors=False,
        )

    def _create_partial_case(
        self,
        case_function: CallableType,
        intermediate: CallableType,
        fallback: CallableType,
    ) -> CallableType:
        partial = CallableInference(
            Functions(case_function, intermediate).diff(),
            self._ctx,
            fallback=fallback,
        ).from_usage(self._applied_args)

        if case_function.is_generic():
            # We can deal with really different `case_function` over here.
            # The first one is regular `generic` function
            # that has variables and typevars in its spec.
            # In this case, we process `partial` the same way.
            # It should be generic also.
            #
            # The second possible type of `case_function` is pseudo-generic.
            # These are functions that contain typevars in its spec,
            # but variables are empty.
            # Probably these functions are already used in a generic context.
            # So, we ignore them and do not add variables back.
            #
            # Regular functions are also untouched by this.
            return detach_callable(partial)
        return partial.copy_modified(variables=[])

    def _create_new_partial(self) -> ProperType:
        """
        Creates a new partial function-like from set of callables.

        We also need fallbacks here, because sometimes
        there are no possible ways to create at least a single partial case.
        In this scenario we analyze the set of fallbacks
        and tell user what went wrong.
        """
        if not self._case_functions:
            analyze_call(
                proper_type(self._fallbacks),
                self._applied_args,
                self._ctx,
                show_errors=True,
            )
            return self._default_return_type
        return proper_type(self._case_functions)


@final
class _AppliedArgs:
    """Builds applied args that were partially applied."""

    def __init__(self, function_ctx: FunctionContext) -> None:
        """
        We need the function default context.

        The first arguments of ``partial`` is skipped:
        it is the applied function itself.
        """
        self._function_ctx = function_ctx
        self._parts = zip(
            self._function_ctx.arg_names[1:],
            self._function_ctx.arg_types[1:],
            self._function_ctx.arg_kinds[1:],
            strict=False,
        )

    def get_callable_from_context(self) -> ProperType:
        """Returns callable type from the context."""
        return get_proper_type(
            safe_translate_to_function(
                self._function_ctx.arg_types[0][0],
                self._function_ctx,
            )
        )

    def build_from_context(self) -> tuple[bool, list[FuncArg]]:
        """
        Builds handy arguments structures from the context.

        Some usages might be invalid,
        because we cannot really infer some arguments.

        .. code:: python

            partial(some, *args)
            partial(other, **kwargs)

        Here ``*args`` and ``**kwargs`` can be literally anything!
        In these cases we fallback to the default return type.
        """
        applied_args = []
        for names, types, kinds in self._parts:
            for arg in self._generate_applied_args(
                zip(names, types, kinds, strict=False)
            ):
                if arg.kind in {ARG_STAR, ARG_STAR2}:
                    # We cannot really work with `*args`, `**kwargs`.
                    return False, []

                applied_args.append(arg)
        return True, applied_args

    def _generate_applied_args(self, arg_parts) -> Iterator[FuncArg]:
        yield from starmap(FuncArg, arg_parts)
