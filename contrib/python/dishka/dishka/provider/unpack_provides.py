from collections.abc import Sequence
from typing import get_args, get_origin

from dishka.dependency_source import (
    Alias,
    Decorator,
    DependencySource,
    Factory,
)
from dishka.entities.key import hint_to_dependency_key
from dishka.entities.provides_marker import ProvideMultiple


def unpack_factory(factory: Factory) -> Sequence[DependencySource]:
    if get_origin(factory.provides.type_hint) is not ProvideMultiple:
        return [factory]

    provides_first, *provides_others = get_args(factory.provides.type_hint)

    res: list[DependencySource] = [
        Alias(
            provides=hint_to_dependency_key(
                provides_other,
            ).with_component(factory.provides.component),
            source=hint_to_dependency_key(
                provides_first,
            ).with_component(factory.provides.component),
            cache=factory.cache,
            when_override=factory.when_override,
            when_active=factory.when_active,
            when_component=factory.when_component,
        )
        for provides_other in provides_others
    ]
    res.append(
        Factory(
            dependencies=factory.dependencies,
            kw_dependencies=factory.kw_dependencies,
            type_=factory.type,
            source=factory.source,
            scope=factory.scope,
            is_to_bind=factory.is_to_bind,
            cache=factory.cache,
            provides=hint_to_dependency_key(
                provides_first,
            ).with_component(factory.provides.component),
            when_override=factory.when_override,
            when_active=factory.when_active,
            when_component=factory.when_component,
            when_dependencies=factory.when_dependencies,
        ),
    )
    return res


def unpack_decorator(decorator: Decorator) -> Sequence[DependencySource]:
    if get_origin(decorator.provides.type_hint) is not ProvideMultiple:
        return [decorator]

    return [
        Decorator(
            factory=decorator.factory,
            provides=hint_to_dependency_key(
                provides,
            ).with_component(decorator.provides.component),
        )
        for provides in get_args(decorator.provides.type_hint)
    ]


def unpack_alias(alias: Alias) -> Sequence[DependencySource]:
    if get_origin(alias.provides.type_hint) is not ProvideMultiple:
        return [alias]

    return [
        Alias(
            provides=hint_to_dependency_key(
                provides,
            ).with_component(alias.provides.component),
            source=alias.source,
            cache=alias.cache,
            when_override=alias.when_override,
            when_active=alias.when_active,
            when_component=alias.when_component,
        )
        for provides in get_args(alias.provides.type_hint)
    ]
