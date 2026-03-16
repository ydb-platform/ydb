from typing import Optional

from mypy.erasetype import erase_type
from mypy.plugin import MethodContext
from mypy.sametypes import is_same_type
from mypy.subtypes import is_subtype
from mypy.types import Instance
from mypy.types import Type as MypyType
from mypy.types import TypedDictType
from typing_extensions import Final

from classes.contrib.mypy.typeops import type_queries
from classes.contrib.mypy.typeops.instance_context import InstanceContext

# Messages:

_INSTANCE_INFERRED_MISMATCH_MSG: Final = (
    'Instance "{0}" does not match inferred type "{1}"'
)

_IS_PROTOCOL_MISSING_MSG: Final = (
    'Protocol type "{0}" is passed as a regular type'
)

_IS_PROTOCOL_UNWANTED_MSG: Final = (
    'Regular type "{0}" passed as a protocol'
)

_DELEGATE_STRICT_SUBTYPE_MSG: Final = (
    'Delegate types used for instance annotation "{0}" ' +
    'must be a direct subtype of runtime type "{1}"'
)

_CONCRETE_GENERIC_MSG: Final = (
    'Instance "{0}" has concrete generic type, ' +
    'it is not supported during runtime'
)

_UNBOUND_TYPE_MSG: Final = (
    'Runtime type "{0}" has unbound type, use implicit any'
)

_TUPLE_LENGTH_MSG: Final = (
    'Expected variadic tuple "Tuple[{0}, ...]", got "{1}"'
)


def check_type(
    instance_context: InstanceContext,
) -> bool:
    """
    Checks runtime type.

    We call "runtime types" things that we use at runtime to dispatch our calls.
    For example:

    1. We check that type passed in ``some.instance(...)`` matches
       one defined in a type annotation
    2. We check that types don't have any concrete types
    3. We check that types don't have any unbound type variables
    4. We check that ``protocol`` and ``delegate`` are passed correctly

    """
    return all([
        _check_matching_types(
            instance_context.inferred_type,
            instance_context.instance_type,
            instance_context.inferred_args.delegate,
            instance_context.ctx,
        ),
        _check_protocol_usage(
            instance_context.inferred_args.exact_type,
            instance_context.inferred_args.protocol,
            instance_context.inferred_args.delegate,
            instance_context.ctx,
        ),
        _check_concrete_generics(
            instance_context.inferred_type,
            instance_context.instance_type,
            instance_context.inferred_args.delegate,
            instance_context.ctx,
        ),
    ])


def _check_matching_types(
    inferred_type: MypyType,
    instance_type: MypyType,
    delegate: Optional[MypyType],
    ctx: MethodContext,
) -> bool:
    if delegate is None:
        instance_check = is_same_type(
            erase_type(instance_type),
            erase_type(inferred_type),
        )
    else:
        # We check for delegate a bit different,
        # because we want to support cases like:
        #
        #   class ListOfStr(List[str]):
        #       ...
        #
        #   @some.instance(delegate=ListOfStr)
        #   def _some_list_str(instance: List[str]) -> str:
        #
        #   some(['a', 'b', 'c'])  # noqa: E800
        #
        # Without this check, we would have
        # to always use `ListOfStr` and not `List[str]`.
        # This is annoying for the user.
        instance_check = (
            is_subtype(instance_type, delegate)
            # When `instance` is a `TypedDict`, we need to rotate the compare:
            if isinstance(instance_type, TypedDictType)
            else is_subtype(delegate, instance_type)
        )

    if not instance_check:
        ctx.api.fail(
            _INSTANCE_INFERRED_MISMATCH_MSG.format(
                instance_type,
                inferred_type if delegate is None else delegate,
            ),
            ctx.context,
        )
    return instance_check


def _check_protocol_usage(  # noqa: WPS231
    exact_type: Optional[MypyType],
    protocol: Optional[MypyType],
    delegate: Optional[MypyType],
    ctx: MethodContext,
) -> bool:
    validation_rules = (
        (exact_type, False),
        (protocol, True),
        (delegate, False),
    )

    is_valid = True
    for passed_type, is_protocol in validation_rules:
        if isinstance(passed_type, Instance) and passed_type.type:
            if not is_protocol and passed_type.type.is_protocol:
                ctx.api.fail(
                    _IS_PROTOCOL_MISSING_MSG.format(passed_type), ctx.context,
                )
                is_valid = False
            elif is_protocol and not passed_type.type.is_protocol:
                ctx.api.fail(
                    _IS_PROTOCOL_UNWANTED_MSG.format(passed_type), ctx.context,
                )
                is_valid = False
    return is_valid


def _check_concrete_generics(
    inferred_type: MypyType,
    instance_type: MypyType,
    delegate: Optional[MypyType],
    ctx: MethodContext,
) -> bool:
    has_concrete_type = False
    type_settings = (  # Not a dict, because of `hash` problems
        (instance_type, False),
        (inferred_type, True),
    )

    for type_, forbid_explicit_any in type_settings:
        local_check = type_queries.has_concrete_type(
            type_,
            is_delegate=delegate is not None,
            forbid_explicit_any=forbid_explicit_any,
            ctx=ctx,
        )
        if local_check:
            ctx.api.fail(_CONCRETE_GENERIC_MSG.format(type_), ctx.context)
        has_concrete_type = has_concrete_type or local_check

    if type_queries.has_unbound_type(inferred_type, ctx):
        ctx.api.fail(_UNBOUND_TYPE_MSG.format(inferred_type), ctx.context)
        return False
    return not has_concrete_type
