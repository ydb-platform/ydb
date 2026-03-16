import itertools
from typing import Dict, List

from mypy.checker import TypeChecker
from mypy.errorcodes import ErrorCode
from mypy.errors import Errors
from mypy.nodes import CallExpr, Decorator, Expression, FuncDef, MypyFile, NameExpr
from mypy.options import Options
from mypy.plugin import CheckerPluginInterface, FunctionContext, Plugin
from mypy.subtypes import is_subtype
from mypy.types import CallableType, Instance, Type, TypeVarType

thinc_model_fullname = "thinc.model.Model"
chained_out_fullname = "thinc.types.XY_YZ_OutT"
intoin_outtoout_out_fullname = "thinc.types.XY_XY_OutT"


def plugin(version: str):
    return ThincPlugin


class ThincPlugin(Plugin):
    def __init__(self, options: Options) -> None:
        super().__init__(options)

    def get_function_hook(self, fullname: str):
        return function_hook


def function_hook(ctx: FunctionContext) -> Type:
    try:
        return get_reducers_type(ctx)
    except AssertionError:
        # Add more function callbacks here
        return ctx.default_return_type


def get_reducers_type(ctx: FunctionContext) -> Type:
    """
    Determine a more specific model type for functions that combine models.

    This function operates on function *calls*. It analyzes each function call
    by looking at the function definition and the arguments passed as part of
    the function call, then determines a more specific return type for the
    function call.

    This method accepts a `FunctionContext` as part of the Mypy plugin
    interface. This function context provides easy access to:
    * `args`: List of "actual arguments" filling each "formal argument" of the
      called function. "Actual arguments" are those passed to the function
      as part of the function call. "Formal arguments" are the parameters
      defined by the function definition. The same actual argument may serve
      to fill multiple formal arguments. In some cases the relationship may
      even be ambiguous. For example, calling `range(*args)`, the actual
      argument `*args` may fill the `start`, `stop` or `step` formal
      arguments, depending on the length of the list.

      The `args` list is of length `num_formals`, with each element
      corresponding to a formal argument. Each value in the `args` list is a
      list of actual arguments which may fill the formal argument. For
      example, in the function call `range(*args, num)`, `num` may fill the
      `start`, `end` or `step` formal arguments depending on the length of
      `args`, so type-checking needs to consider all of these possibilities.
    * `arg_types`: Type annotation (or inferred type) of each argument. Like
      `args`, this value is a list of lists with an outer list entry for each
      formal argument and an inner list entry for each possible actual
      argument for the formal argument.
    * `arg_kinds`: "Kind" of argument passed to the function call. Argument
      kinds include positional, star (`*args`), named (`x=y`) and star2
      (`**kwargs`) arguments (among others). Like `args`, this value is a list
      of lists.
    * `context`: AST node representing the function call with all available
      type information. Notable attributes include:
      * `args` and `arg_kinds`: Simple list of actual arguments, not mapped to
        formal arguments.
      * `callee`: AST node representing the function being called. Typically
        this is a `NameExpr`. To resolve this node to the function definition
        it references, accessing `callee.node` will usually return either a
        `FuncDef` or `Decorator` node.
    * etc.

    This function infers a more specific type for model-combining functions by
    making certain assumptions about how the function operates based on the
    order of its formal arguments and its return type.

    If the return type is `Model[InT, XY_YZ_OutT]`, the output of each
    argument is expected to be used as the input to the next argument. It's
    therefore necessary to check that the output type of each model is
    compatible with the input type of the following model. The combined model
    has the type `Model[InT, OutT]`, where `InT` is the input type of the
    first model and `OutT` is the output type of the last model.

    If the return type is `Model[InT, XY_XY_OutT]`, all model arguments
    receive input of the same type and are expected to produce output of the
    same type. It's therefore necessary to check that all models have the same
    input types and the same output types. The combined model has the type
    `Model[InT, OutT]`, where `InT` is the input type of all model arguments
    and `OutT` is the output type of all model arguments.

    Raises:
        AssertionError: Raised if a more specific model type couldn't be
            determined, indicating that the default general return type should
            be used.
    """
    # Verify that we have a type-checking API and a default return type (presumably a
    # `thinc.model.Model` instance)
    assert isinstance(ctx.api, TypeChecker)
    assert isinstance(ctx.default_return_type, Instance)

    # Verify that we're inspecting a function call to a callable defined or decorated function
    assert isinstance(ctx.context, CallExpr)
    callee = ctx.context.callee
    assert isinstance(callee, NameExpr)
    callee_node = callee.node
    assert isinstance(callee_node, (FuncDef, Decorator))
    callee_node_type = callee_node.type
    assert isinstance(callee_node_type, CallableType)

    # Verify that the callable returns a `thinc.model.Model`
    # TODO: Use `map_instance_to_supertype` to map subtypes to `Model` instances.
    # (figure out how to look up the `TypeInfo` for a class outside of the module being type-checked)
    callee_return_type = callee_node_type.ret_type
    assert isinstance(callee_return_type, Instance)
    assert callee_return_type.type.fullname == thinc_model_fullname
    assert callee_return_type.args
    assert len(callee_return_type.args) == 2

    # Obtain the output type parameter of the `thinc.model.Model` return type
    # of the called API function
    out_type = callee_return_type.args[1]

    # Check if the `Model`'s output type parameter is one of the "special
    # type variables" defined to represent model composition (chaining) and
    # homogeneous reduction
    assert isinstance(out_type, TypeVarType)
    assert out_type.fullname
    if out_type.fullname not in {intoin_outtoout_out_fullname, chained_out_fullname}:
        return ctx.default_return_type

    # Extract type of each argument used to call the API function, making sure that they are also
    # `thinc.model.Model` instances
    args = list(itertools.chain(*ctx.args))
    arg_types = []
    for arg_type in itertools.chain(*ctx.arg_types):
        # TODO: Use `map_instance_to_supertype` to map subtypes to `Model` instances.
        assert isinstance(arg_type, Instance)
        assert arg_type.type.fullname == thinc_model_fullname
        assert len(arg_type.args) == 2
        arg_types.append(arg_type)

    # Collect neighboring pairs of arguments and their types
    arg_pairs = list(zip(args[:-1], args[1:]))
    arg_types_pairs = list(zip(arg_types[:-1], arg_types[1:]))

    # Determine if passed models will be chained or if they all need to have
    # the same input and output type
    if out_type.fullname == chained_out_fullname:
        # Models will be chained, meaning that the output of each model will
        # be passed as the input to the next model
        # Verify that model inputs and outputs are compatible
        for (arg1, arg2), (type1, type2) in zip(arg_pairs, arg_types_pairs):
            assert isinstance(type1, Instance)
            assert isinstance(type2, Instance)
            assert type1.type.fullname == thinc_model_fullname
            assert type2.type.fullname == thinc_model_fullname
            check_chained(
                l1_arg=arg1, l1_type=type1, l2_arg=arg2, l2_type=type2, api=ctx.api
            )

        # Generated model takes the first model's input and returns the last model's output
        return Instance(
            ctx.default_return_type.type, [arg_types[0].args[0], arg_types[-1].args[1]]
        )
    elif out_type.fullname == intoin_outtoout_out_fullname:
        # Models must have the same input and output types
        # Verify that model inputs and outputs are compatible
        for (arg1, arg2), (type1, type2) in zip(arg_pairs, arg_types_pairs):
            assert isinstance(type1, Instance)
            assert isinstance(type2, Instance)
            assert type1.type.fullname == thinc_model_fullname
            assert type2.type.fullname == thinc_model_fullname
            check_intoin_outtoout(
                l1_arg=arg1, l1_type=type1, l2_arg=arg2, l2_type=type2, api=ctx.api
            )

        # Generated model accepts and returns the same types as all passed models
        return Instance(
            ctx.default_return_type.type, [arg_types[0].args[0], arg_types[0].args[1]]
        )

    # Make sure the default return type is returned if no branch was selected
    assert False, "Thinc mypy plugin error: it should return before this point"


def check_chained(
    *,
    l1_arg: Expression,
    l1_type: Instance,
    l2_arg: Expression,
    l2_type: Instance,
    api: CheckerPluginInterface,
):
    if not is_subtype(l1_type.args[1], l2_type.args[0]):
        api.fail(
            f"Layer outputs type ({l1_type.args[1]}) but the next layer expects ({l2_type.args[0]}) as an input",
            l1_arg,
            code=error_layer_output,
        )
        api.fail(
            f"Layer input type ({l2_type.args[0]}) is not compatible with output ({l1_type.args[1]}) from previous layer",
            l2_arg,
            code=error_layer_input,
        )


def check_intoin_outtoout(
    *,
    l1_arg: Expression,
    l1_type: Instance,
    l2_arg: Expression,
    l2_type: Instance,
    api: CheckerPluginInterface,
):
    if l1_type.args[0] != l2_type.args[0]:
        api.fail(
            f"Layer input ({l1_type.args[0]}) not compatible with next layer input ({l2_type.args[0]})",
            l1_arg,
            code=error_layer_input,
        )
        api.fail(
            f"Layer input ({l2_type.args[0]}) not compatible with previous layer input ({l1_type.args[0]})",
            l2_arg,
            code=error_layer_input,
        )
    if l1_type.args[1] != l2_type.args[1]:
        api.fail(
            f"Layer output ({l1_type.args[1]}) not compatible with next layer output ({l2_type.args[1]})",
            l1_arg,
            code=error_layer_output,
        )
        api.fail(
            f"Layer output ({l2_type.args[1]}) not compatible with previous layer output ({l1_type.args[1]})",
            l2_arg,
            code=error_layer_output,
        )


error_layer_input = ErrorCode("layer-mismatch-input", "Invalid layer input", "Thinc")
error_layer_output = ErrorCode("layer-mismatch-output", "Invalid layer output", "Thinc")


class IntrospectChecker(TypeChecker):
    def __init__(
        self,
        errors: Errors,
        modules: Dict[str, MypyFile],
        options: Options,
        tree: MypyFile,
        path: str,
        plugin: Plugin,
        per_line_checking_time_ns: Dict[int, int],
    ):
        self._error_messages: List[str] = []
        super().__init__(
            errors, modules, options, tree, path, plugin, per_line_checking_time_ns
        )
