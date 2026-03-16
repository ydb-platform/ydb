from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, Union

from typing_extensions import get_args, get_origin

from polyfactory.utils.predicates import (
    is_generic_alias,
    is_type_alias,
    is_type_var,
    is_union,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


def normalize_type(type_annotation: Any) -> Any:
    """Convert modern Python 3.12+ type syntax to standard annotations.

    Handles TypeAliasType and GenericAlias types introduced in Python 3.12+,
    converting them to standard type annotations when needed.

    Args:
        type_annotation: Type to normalize (convert if needed or pass through).

    Returns:
        Normalized type annotation with resolved type aliases and substituted parameters.

    Example:
        ```python
        # Python 3.12+
        >> from typing import Annotated
        >> import annotated_types as at

        >> type NegativeInt = Annotated[int, at.Lt(0)]
        >> type NonEmptyList[T] = Annotated[list[T], at.Len(1)]

        >> normalize_type(NonEmptyList[NegativeInt])
        # typing.Annotated[list[typing.Annotated[int, Lt(lt=0)]], Len(min_length=1, max_length=None)]
        ```
    """

    if is_type_alias(type_annotation):
        return type_annotation.__value__

    if not is_generic_alias(type_annotation):
        return type_annotation

    origin = get_origin(type_annotation)
    args = get_args(type_annotation)

    if is_type_alias(origin):
        return __handle_generic_type_alias(origin, args)

    if args:
        normalized_args = tuple(normalize_type(arg) for arg in args)
        if normalized_args != args:
            return origin[normalized_args[0] if len(normalized_args) == 1 else normalized_args]

    return type_annotation


def __handle_generic_type_alias(origin: Any, args: tuple) -> Any:
    """Handle generic type alias with parameters."""
    template = origin.__value__
    type_params = origin.__type_params__

    if not (type_params and args):
        return template

    normalized_args = tuple(normalize_type(arg) for arg in args)
    substitutions = dict(zip(type_params, normalized_args))

    if get_origin(template) is Annotated:
        base_type, *metadata = get_args(template)
        template_result = Annotated[(__apply_substitutions(base_type, substitutions), *metadata)]  # type: ignore[valid-type]
    else:
        template_result = __apply_substitutions(template, substitutions)

    return template_result


def __apply_substitutions(target: Any, subs: Mapping[Any, Any]) -> Any:
    if is_type_var(target):
        return subs.get(target, target)

    if is_union(target):
        args = tuple(__apply_substitutions(arg, subs) for arg in get_args(target))
        return Union[args]

    origin = get_origin(target)
    args = get_args(target)

    if is_type_alias(origin):
        sub_args = tuple(__apply_substitutions(arg, subs) for arg in args) if args else ()
        return normalize_type(origin[sub_args] if sub_args else origin)

    if origin and args:
        sub_args = tuple(__apply_substitutions(arg, subs) for arg in args)
        return origin[sub_args[0] if len(sub_args) == 1 else sub_args]

    return target
