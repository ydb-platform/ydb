from typing import Any

from dishka.entities.component import Component
from dishka.entities.factory_type import FactoryType
from dishka.entities.key import DependencyKey
from dishka.entities.scope import BaseScope
from .factory import Factory


class FactoryUnionMode:
    __slots__ = ("cache", "collect", "provides", "scope", "source")

    def __init__(
            self,
            *,
            source: DependencyKey,
            scope: BaseScope | None,
            collect: bool,
            cache: bool,
            provides: DependencyKey,
    ) -> None:
        self.source = source
        self.scope = scope
        self.collect = collect
        self.cache = cache
        self.provides = provides

    def with_component(self, component: Component) -> "FactoryUnionMode":
        return FactoryUnionMode(
            source=self.source.with_component(component),
            scope=self.scope,
            collect=self.collect,
            cache=self.cache,
            provides=self.provides.with_component(component),
        )

    def __get__(self, instance: Any, owner: Any) -> "FactoryUnionMode":
        return FactoryUnionMode(
            source=self.source,
            scope=self.scope,
            collect=self.collect,
            cache=self.cache,
            provides=self.provides,
        )

    def as_factory(self) -> Factory | None:
        if not self.collect:
            return None
        return Factory(
            source=self.source,
            provides=self.provides,
            scope=self.scope,
            when_active=None,
            when_override=None,
            cache=self.cache,
            when_component=self.provides.component,
            is_to_bind=False,
            type_=FactoryType.COLLECTION,
            when_dependencies=[],
            dependencies=[],
            kw_dependencies={},
        )
