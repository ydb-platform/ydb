from __future__ import annotations

from collections.abc import (
    Mapping,
    Sequence,
)
from enum import Enum
from typing import Any, TypeAlias, TypeVar

from dishka.entities.component import Component
from dishka.entities.factory_type import FactoryData, FactoryType
from dishka.entities.key import DependencyKey
from dishka.entities.marker import BaseMarker, combine_when
from dishka.entities.scope import BaseScope


class Special(Enum):
    OMITTED = "omitted"


T = TypeVar("T")
MayBe: TypeAlias = Special | T

def coalesce(a: MayBe[T], b: T) -> T:
    if a is Special.OMITTED:
        return b
    return a


class Factory(FactoryData):
    __slots__ = (
        "cache",
        "connected_factories",
        "dependencies",
        "is_to_bind",
        "kw_dependencies",
        "when_active",
        "when_component",
        "when_dependencies",
    )

    def __init__(
        self,
        *,
        dependencies: Sequence[DependencyKey],
        kw_dependencies: Mapping[str, DependencyKey],
        source: Any,
        provides: DependencyKey,
        scope: BaseScope | None,
        type_: FactoryType,
        is_to_bind: bool,
        cache: bool,
        when_override: BaseMarker | None,
        when_active: BaseMarker | None,
        when_component: Component | None,
        when_dependencies: Sequence[Factory],
    ) -> None:
        """

        :param dependencies: args of source
        :param kw_dependencies: kwargs of source
        :param source: callable to produce result (or item key in collection)
        :param provides:
        :param scope:
        :param type_:
        :param is_to_bind: use first dependency as class instance on __get__
        :param cache: add result to container cache
        :param when_override:  condition to override
        :param when_active: condition to check availability
        :param when_component: component of conditions
        :param when_dependencies: deps for conditional creation
        """
        super().__init__(
            source=source,
            provides=provides,
            type_=type_,
            scope=scope,
            when_override=when_override,
        )
        self.dependencies = dependencies
        self.kw_dependencies = kw_dependencies
        self.is_to_bind = is_to_bind
        self.cache = cache
        self.when_active = when_active
        self.when_component = when_component
        self.when_dependencies = when_dependencies

    def __get__(self, instance: Any, owner: Any) -> Factory:
        scope = self.scope or getattr(instance, "scope", None)
        provider_when = getattr(instance, "when", None)
        when_active = combine_when(provider_when, self.when_active)
        when_override = combine_when(provider_when, self.when_override)
        if self.is_to_bind:
            source = self.source.__get__(instance, owner)
            dependencies = self.dependencies[1:]
        else:
            source = self.source
            dependencies = self.dependencies[:]
        return Factory(
            dependencies=dependencies,
            kw_dependencies=self.kw_dependencies,
            source=source,
            provides=self.provides,
            scope=scope,
            type_=self.type,
            is_to_bind=False,
            cache=self.cache,
            when_override=when_override,
            when_active=when_active,
            when_component=self.when_component,
            when_dependencies=self.when_dependencies,
        )

    def with_component(self, component: Component) -> Factory:
        return Factory(
            dependencies=[
                d.with_component(component) for d in self.dependencies
            ],
            kw_dependencies={
                name: d.with_component(component)
                for name, d in self.kw_dependencies.items()
            },
            source=self.source,
            provides=self.provides.with_component(component),
            scope=self.scope,
            is_to_bind=self.is_to_bind,
            cache=self.cache,
            type_=self.type,
            when_override=self.when_override,
            when_active=self.when_active,
            when_component=(
                component
                if self.when_component is None
                else self.when_component
            ),
            when_dependencies=self.when_dependencies,
        )

    def with_scope(self, scope: BaseScope) -> Factory:
        return Factory(
            dependencies=tuple(self.dependencies),
            kw_dependencies=dict(self.kw_dependencies),
            source=self.source,
            provides=self.provides,
            scope=self.scope or scope,
            is_to_bind=self.is_to_bind,
            cache=self.cache,
            type_=self.type,
            when_override=self.when_override,
            when_active=self.when_active,
            when_component=self.when_component,
            when_dependencies=self.when_dependencies,
        )

    def replace(
        self,
        scope: MayBe[BaseScope] = Special.OMITTED,
        provides: MayBe[DependencyKey] = Special.OMITTED,
        when_active: MayBe[BaseMarker|None] = Special.OMITTED,
        when_override: MayBe[BaseMarker|None] = Special.OMITTED,
        when_component: MayBe[Component] = Special.OMITTED,
        when_dependencies: MayBe[Sequence[Factory]] = Special.OMITTED,
    ) -> Factory:
        return Factory(
            dependencies=list(self.dependencies),
            kw_dependencies=dict(self.kw_dependencies),
            source=self.source,
            provides=coalesce(provides, self.provides),
            scope=coalesce(scope, self.scope),
            is_to_bind=self.is_to_bind,
            cache=self.cache,
            type_=self.type,
            when_override=coalesce(when_override, self.when_override),
            when_active=coalesce(when_active, self.when_active),
            when_component=coalesce(when_component, self.when_component),
            when_dependencies=coalesce(
                when_dependencies,
                self.when_dependencies,
            ),
        )
