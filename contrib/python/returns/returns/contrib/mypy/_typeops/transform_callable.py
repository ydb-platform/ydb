from typing import ClassVar, final

from mypy.nodes import ARG_OPT, ARG_POS, ARG_STAR, ARG_STAR2, ArgKind
from mypy.typeops import get_type_vars
from mypy.types import (
    AnyType,
    CallableType,
    FunctionLike,
    Overloaded,
    TypeOfAny,
    TypeVarType,
)
from mypy.types import Type as MypyType

from returns.contrib.mypy._structures.args import FuncArg


def proper_type(
    case_functions: list[CallableType],
) -> FunctionLike:
    """Returns a ``CallableType`` or ``Overloaded`` based on case functions."""
    if len(case_functions) == 1:
        return case_functions[0]
    return Overloaded(case_functions)


@final
class Intermediate:
    """
    Allows to build a new callable from old one and different options.

    For example, helps to tell which callee arguments
    was already provided in caller.
    """

    #: Positional arguments can be of this kind.
    _positional_kinds: ClassVar[frozenset[ArgKind]] = frozenset((
        ARG_POS,
        ARG_OPT,
        ARG_STAR,
    ))

    def __init__(self, case_function: CallableType) -> None:
        """We only need a callable to work on."""
        self._case_function = case_function

    def with_applied_args(self, applied_args: list[FuncArg]) -> CallableType:
        """
        By calling this method we construct a new callable from its usage.

        This allows use to create an intermediate callable with just used args.
        """
        new_pos_args = self._applied_positional_args(applied_args)
        new_named_args = self._applied_named_args(applied_args)
        return self.with_signature(new_pos_args + new_named_args)

    def with_signature(self, new_args: list[FuncArg]) -> CallableType:
        """Smartly creates a new callable from a given arguments."""
        return detach_callable(
            self._case_function.copy_modified(
                arg_names=[arg.name for arg in new_args],
                arg_types=[arg.type for arg in new_args],
                arg_kinds=[arg.kind for arg in new_args],
            )
        )

    def with_ret_type(self, ret_type: MypyType) -> CallableType:
        """Smartly creates a new callable from a given return type."""
        return self._case_function.copy_modified(ret_type=ret_type)

    def _applied_positional_args(
        self,
        applied_args: list[FuncArg],
    ) -> list[FuncArg]:
        callee_args = list(
            filter(
                lambda name: name.name
                is None,  # TODO: maybe use `kind` instead?
                applied_args,
            )
        )

        new_function_args = []
        for ind, arg in enumerate(FuncArg.from_callable(self._case_function)):
            if arg.kind in self._positional_kinds and ind < len(callee_args):
                new_function_args.append(arg)
        return new_function_args

    def _applied_named_args(
        self,
        applied_args: list[FuncArg],
    ) -> list[FuncArg]:
        callee_args = list(
            filter(
                lambda name: name.name is not None,
                applied_args,
            )
        )

        new_function_args = []
        for arg in FuncArg.from_callable(self._case_function):
            has_named_arg_def = any(
                # Argument can either be used as a named argument
                # or passed to `**kwargs` if it exists.
                arg.name == rdc.name or arg.kind == ARG_STAR2
                for rdc in callee_args
            )
            if callee_args and has_named_arg_def:
                new_function_args.append(arg)
        return new_function_args


@final
class Functions:
    """
    Allows to create new callables based on two existing ones.

    For example, one can need a diff of two callables.
    """

    def __init__(
        self,
        original: CallableType,
        intermediate: CallableType,
    ) -> None:
        """We need two callable to work with."""
        self._original = original
        self._intermediate = intermediate

    def diff(self) -> CallableType:
        """Finds a diff between two functions' arguments."""
        intermediate_names = [
            arg.name for arg in FuncArg.from_callable(self._intermediate)
        ]
        new_function_args = []

        for index, arg in enumerate(FuncArg.from_callable(self._original)):
            should_be_copied = (
                arg.kind in {ARG_STAR, ARG_STAR2}
                or arg.name not in intermediate_names
                or
                # We need to treat unnamed args differently, because python3.8
                # has pos_only_args, all their names are `None`.
                # This is also true for `lambda` functions where `.name`
                # might be missing for some reason.
                (
                    not arg.name
                    and not (
                        index < len(intermediate_names)
                        and
                        # If this is also unnamed arg, then ignoring it.
                        not intermediate_names[index]
                    )
                )
            )
            if should_be_copied:
                new_function_args.append(arg)
        return Intermediate(self._original).with_signature(
            new_function_args,
        )


# TODO: Remove this function once `mypy` order the TypeVars
# by their appearance sequence
def detach_callable(typ: CallableType) -> CallableType:  # noqa: WPS210
    """
    THIS IS A COPY OF `mypy.checker.detach_callable` FUNCTION.

    THE ONLY PURPOSE WE'VE COPIED IS TO GUARANTEE A DETERMINISTIC FOR OUR
    TYPE VARIABLES!
    AS YOU CAN SEE, WE ORDER THE TYPE VARS BY THEIR APPEARANCE SEQUENCE.
    """
    type_list = [*typ.arg_types, typ.ret_type]

    appear_map: dict[str, list[int]] = {}
    for idx, inner_type in enumerate(type_list):
        typevars_available = get_type_vars(inner_type)
        for var in typevars_available:  # noqa: WPS110
            if var.fullname not in appear_map:
                appear_map[var.fullname] = []
            appear_map[var.fullname].append(idx)

    used_type_var_names = appear_map.keys()

    all_type_vars = get_type_vars(typ)
    new_variables = []
    for var in set(all_type_vars):  # noqa: WPS110
        if var.fullname not in used_type_var_names:
            continue
        new_variables.append(
            TypeVarType(
                name=var.name,
                fullname=var.fullname,
                id=var.id,
                values=var.values,
                upper_bound=var.upper_bound,
                variance=var.variance,
                default=AnyType(TypeOfAny.from_omitted_generics),
            ),
        )

    new_variables = sorted(
        new_variables,
        key=lambda item: appear_map[item.fullname][0],  # noqa: WPS110
    )

    return typ.copy_modified(
        variables=new_variables,
        arg_types=type_list[:-1],
        ret_type=type_list[-1],
    )
