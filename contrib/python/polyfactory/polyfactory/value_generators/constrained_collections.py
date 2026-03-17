from __future__ import annotations

from enum import EnumMeta
from typing import TYPE_CHECKING, Any, Callable, Literal, TypeVar

from polyfactory.exceptions import ParameterException

if TYPE_CHECKING:
    from collections.abc import Mapping

    from polyfactory.factories.base import BaseFactory, BuildContext
    from polyfactory.field_meta import FieldMeta

T = TypeVar("T", list, set, frozenset)


def handle_constrained_collection(  # noqa: C901
    collection_type: Callable[..., T],
    factory: type[BaseFactory[Any]],
    field_meta: FieldMeta,
    item_type: Any,
    max_items: int | None = None,
    min_items: int | None = None,
    unique_items: bool = False,
    field_build_parameters: Any | None = None,
    build_context: BuildContext | None = None,
) -> T:
    """Generate a constrained list or set.

    :param collection_type: A type that can accept type arguments.
    :param factory: A factory.
    :param field_meta: A field meta instance.
    :param item_type: Type of the collection items.
    :param max_items: Maximal number of items.
    :param min_items: Minimal number of items.
    :param unique_items: Whether the items should be unique.
    :param field_build_parameters: Any build parameters passed to the factory as kwarg values.
    :param build_context: BuildContext data for current build.

    :returns: A collection value.
    """
    build_context = factory._get_build_context(build_context)
    if field_meta.annotation in build_context["seen_models"]:
        return collection_type()

    min_items = abs(min_items if min_items is not None else (max_items or 0))
    max_items = abs(max_items) if max_items is not None else min_items

    if max_items < min_items:
        msg = "max_items must be larger or equal to min_items"
        raise ParameterException(msg)

    if collection_type in (frozenset, set) or unique_items:
        max_field_values = max_items
        if hasattr(field_meta.annotation, "__origin__") and field_meta.annotation.__origin__ is Literal:
            if field_meta.children is not None:
                max_field_values = len(field_meta.children)
        elif isinstance(field_meta.annotation, EnumMeta):
            max_field_values = len(field_meta.annotation)
        min_items = min(min_items, max_field_values)
        max_items = min(max_items, max_field_values)

    collection: set[T] | list[T] = set() if (collection_type in (frozenset, set) or unique_items) else []

    try:
        length = factory.__random__.randint(min_items, max_items)
        while (i := len(collection)) < length:
            if field_build_parameters and len(field_build_parameters) > i:
                build_params = field_build_parameters[i]
            else:
                build_params = None

            value = factory.get_field_value(
                field_meta,
                field_build_parameters=build_params,
                build_context=build_context,
            )

            if isinstance(collection, set):
                collection.add(value)
            else:
                collection.append(value)
        return collection_type(collection)
    except TypeError as e:
        msg = f"cannot generate a constrained collection of type: {item_type}"
        raise ParameterException(msg) from e


def handle_constrained_mapping(
    factory: type[BaseFactory[Any]],
    field_meta: FieldMeta,
    max_items: int | None = None,
    min_items: int | None = None,
    field_build_parameters: Any | None = None,
    build_context: BuildContext | None = None,
) -> Mapping[Any, Any]:
    """Generate a constrained mapping.

    :param factory: A factory.
    :param field_meta: A field meta instance.
    :param max_items: Maximal number of items.
    :param min_items: Minimal number of items.
    :param field_build_parameters: Any build parameters passed to the factory as kwarg values.
    :param build_context: BuildContext data for current build.

    :returns: A mapping instance.
    """
    build_context = factory._get_build_context(build_context)
    if field_meta.children is None or any(
        child_meta.annotation in build_context["seen_models"] for child_meta in field_meta.children
    ):
        return {}

    min_items = abs(min_items if min_items is not None else (max_items or 0))
    max_items = abs(max_items) if max_items is not None else min_items

    if max_items < min_items:
        msg = "max_items must be larger or equal to min_items"
        raise ParameterException(msg)

    length = factory.__random__.randint(min_items, max_items)

    collection: dict[Any, Any] = {}

    children = field_meta.children
    key_field_meta = children[0]
    value_field_meta = children[1]
    while len(collection) < length:
        key = factory.get_field_value(
            key_field_meta, field_build_parameters=field_build_parameters, build_context=build_context
        )
        value = factory.get_field_value(
            value_field_meta, field_build_parameters=field_build_parameters, build_context=build_context
        )
        collection[key] = value

    return collection
