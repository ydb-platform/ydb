from typing import List, Union

from mypy.plugin import FunctionContext, MethodContext
from mypy.typeops import make_simplified_union
from mypy.types import AnyType, Instance, LiteralType, TupleType
from mypy.types import Type as MypyType
from mypy.types import TypeVarType, UnboundType, UninhabitedType
from typing_extensions import Final

#: Types that pollute instance args.
_TYPES_TO_FILTER_OUT: Final = (
    TypeVarType,
    UninhabitedType,
    UnboundType,
    AnyType,
)


def add_unique(
    new_instance_type: MypyType,
    existing_instance_type: MypyType,
) -> MypyType:
    """
    Adds new instance type to existing ones.

    It is smart: filters our junk and uses unique and flat ``Union`` types.
    """
    unified = list(filter(
        # We filter our `NoReturn` and other things like `Any`
        # that can break our instances union.
        # TODO: maybe use `has_uninhabited_component`?
        lambda type_: not isinstance(type_, _TYPES_TO_FILTER_OUT),
        [new_instance_type, existing_instance_type],
    ))
    return make_simplified_union(unified)


def mutate_typeclass_def(
    typeclass: Instance,
    definition_fullname: str,
    ctx: Union[FunctionContext, MethodContext],
) -> None:
    """Adds definition fullname to the typeclass type."""
    str_fallback = ctx.api.str_type()  # type: ignore

    typeclass.args = (
        *typeclass.args[:3],
        LiteralType(definition_fullname, str_fallback),
    )


def mutate_typeclass_instance_def(
    instance: Instance,
    *,
    passed_types: List[MypyType],
    typeclass: Instance,
    ctx: Union[MethodContext, FunctionContext],
) -> None:
    """
    Mutates ``TypeClassInstanceDef`` args.

    That's where we fill their values.
    Why? Because we need all types from ``some.instance()`` call.
    Including all passed arguments as a tuple for later checks.
    """
    tuple_type = TupleType(
        # We now store passed arg types in a single tuple:
        passed_types,
        fallback=ctx.api.named_type('builtins.tuple'),  # type: ignore
    )

    instance.args = (
        tuple_type,  # Passed runtime types, like str in `@some.instance(str)`
        typeclass,  # `_TypeClass` instance itself
    )
