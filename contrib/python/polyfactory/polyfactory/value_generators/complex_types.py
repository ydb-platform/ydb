from __future__ import annotations

from collections.abc import Iterable, MutableMapping, MutableSequence
from collections.abc import Set as AbstractSet
from typing import TYPE_CHECKING, Any, cast

from typing_extensions import is_typeddict

from polyfactory.utils.model_coverage import CoverageContainer

if TYPE_CHECKING:
    from polyfactory.factories.base import BaseFactory, BuildContext
    from polyfactory.field_meta import FieldMeta


def handle_collection_type(
    field_meta: FieldMeta,
    container_type: type,
    factory: type[BaseFactory[Any]],
    field_build_parameters: Any | None = None,
    build_context: BuildContext | None = None,
) -> Any:
    """Handle generation of container types recursively.

    :param container_type: A type that can accept type arguments.
    :param factory: A factory.
    :param field_meta: A field meta instance.
    :param field_build_parameters: Any build parameters passed to the factory as kwarg values.
    :param build_context: BuildContext data for current build.

    :returns: A built result.
    """
    container = container_type()
    if field_meta.children is None or any(
        child_meta.annotation in factory._get_build_context(build_context)["seen_models"]
        for child_meta in field_meta.children
    ):
        return container

    if issubclass(container_type, MutableMapping) or is_typeddict(container_type):
        for key_field_meta, value_field_meta in cast(
            "Iterable[tuple[FieldMeta, FieldMeta]]",
            zip(field_meta.children[::2], field_meta.children[1::2]),
        ):
            key = factory.get_field_value(
                key_field_meta, field_build_parameters=field_build_parameters, build_context=build_context
            )
            value = factory.get_field_value(
                value_field_meta, field_build_parameters=field_build_parameters, build_context=build_context
            )
            container[key] = value
        return container

    if issubclass(container_type, MutableSequence):
        container.extend(
            [
                factory.get_field_value(
                    subfield_meta, field_build_parameters=field_build_parameters, build_context=build_context
                )
                for subfield_meta in field_meta.children
            ]
        )
        return container

    if issubclass(container_type, set):
        for subfield_meta in field_meta.children:
            container.add(
                factory.get_field_value(
                    subfield_meta, field_build_parameters=field_build_parameters, build_context=build_context
                )
            )
        return container

    if issubclass(container_type, AbstractSet):
        return container.union(
            handle_collection_type(
                field_meta, set, factory, field_build_parameters=field_build_parameters, build_context=build_context
            )
        )

    if issubclass(container_type, tuple):
        return container_type(
            factory.get_field_value(child, field_build_parameters=field_build_parameters, build_context=build_context)
            for child in field_meta.children
            if child.annotation != Ellipsis
        )

    msg = f"Unsupported container type: {container_type}"
    raise NotImplementedError(msg)


def handle_collection_type_coverage(  # noqa: C901, PLR0911
    field_meta: FieldMeta,
    container_type: type,
    factory: type[BaseFactory[Any]],
    build_context: BuildContext | None = None,
) -> Any:
    """Handle coverage generation of container types recursively.

    :param container_type: A type that can accept type arguments.
    :param factory: A factory.
    :param field_meta: A field meta instance.

    :returns: An unresolved built result.
    """
    container = container_type()
    if not field_meta.children:
        return container

    if issubclass(container_type, MutableMapping) or is_typeddict(container_type):
        for key_field_meta, value_field_meta in cast(
            "Iterable[tuple[FieldMeta, FieldMeta]]",
            zip(field_meta.children[::2], field_meta.children[1::2]),
        ):
            key = CoverageContainer(factory.get_field_value_coverage(key_field_meta, build_context=build_context))
            value = CoverageContainer(factory.get_field_value_coverage(value_field_meta, build_context=build_context))
            container[key] = value
        return container

    if issubclass(container_type, MutableSequence):
        container_instance = container_type()
        for subfield_meta in field_meta.children:
            container_instance.extend(factory.get_field_value_coverage(subfield_meta, build_context=build_context))

        return container_instance

    if issubclass(container_type, set):
        set_instance = container_type()
        for subfield_meta in field_meta.children:
            set_instance = set_instance.union(
                factory.get_field_value_coverage(subfield_meta, build_context=build_context)
            )

        return set_instance

    if issubclass(container_type, AbstractSet):
        return container.union(handle_collection_type_coverage(field_meta, set, factory, build_context=build_context))

    if issubclass(container_type, tuple):
        if field_meta.children[-1].annotation == Ellipsis:
            return (
                CoverageContainer(
                    factory.get_field_value_coverage(
                        field_meta.children[0],
                        build_context=build_context,
                    )
                ),
            )

        return container_type(
            CoverageContainer(factory.get_field_value_coverage(subfield_meta, build_context=build_context))
            for subfield_meta in field_meta.children
        )

    msg = f"Unsupported container type: {container_type}"
    raise NotImplementedError(msg)
