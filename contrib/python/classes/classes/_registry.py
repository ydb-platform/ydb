from typing import Callable, Dict, NoReturn, Optional, Tuple

from typing_extensions import Final

TypeRegistry = Dict[type, Callable]

#: We use this to exclude `None` as a default value for `exact_type`.
DefaultValue: Final = type('DefaultValueType', (object,), {})

#: Used both in runtime and during mypy type-checking.
INVALID_ARGUMENTS_MSG: Final = (
    'Only a single argument can be applied to `.instance`'
)


def choose_registry(  # noqa: WPS211
    # It has multiple arguments, but I don't see an easy and performant way
    # to refactor it: I don't want to create extra structures
    # and I don't want to create a class with methods.
    exact_type: Optional[type],
    protocol: type,
    delegate: type,
    delegates: TypeRegistry,
    exact_types: TypeRegistry,
    protocols: TypeRegistry,
) -> Tuple[TypeRegistry, type]:
    """
    Returns the appropriate registry to store the passed type.

    It depends on how ``instance`` method is used and also on the type itself.
    """
    passed_args = list(filter(
        _is_not_default_argument_value,
        (exact_type, protocol, delegate),
    ))
    if not passed_args:
        raise ValueError('At least one argument to `.instance` is required')
    if len(passed_args) > 1:
        raise ValueError(INVALID_ARGUMENTS_MSG)

    if _is_not_default_argument_value(delegate):
        return delegates, delegate
    elif _is_not_default_argument_value(protocol):
        return protocols, protocol
    return exact_types, exact_type if exact_type is not None else type(None)


def default_implementation(instance, *args, **kwargs) -> NoReturn:
    """By default raises an exception."""
    raise NotImplementedError(
        'Missing matched typeclass instance for type: {0}'.format(
            type(instance).__qualname__,
        ),
    )


def _is_not_default_argument_value(arg: Optional[type]) -> bool:
    return arg is not DefaultValue
