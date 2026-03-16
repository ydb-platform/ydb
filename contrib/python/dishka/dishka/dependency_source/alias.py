from typing import Any

from dishka.entities.component import Component
from dishka.entities.factory_type import FactoryType
from dishka.entities.key import DependencyKey
from dishka.entities.marker import BaseMarker, combine_when
from dishka.entities.scope import BaseScope
from .factory import Factory


def _identity(x: Any) -> Any:
    return x


class Alias:
    __slots__ = (
        "cache",
        "component",
        "override",
        "provides",
        "source",
        "when_active",
        "when_component",
        "when_override",
    )

    def __init__(
            self, *,
            source: DependencyKey,
            provides: DependencyKey,
            cache: bool,
            when_active: BaseMarker | None = None,
            when_override: BaseMarker | None = None,
            when_component: Component | None,
    ) -> None:
        self.source = source
        self.provides = provides
        self.cache = cache
        self.when_override = when_override
        self.when_active = when_active
        self.when_component = when_component

    def as_factory(
            self, scope: BaseScope | None, component: Component | None,
    ) -> Factory:
        return Factory(
            scope=scope,
            source=_identity,
            provides=self.provides.with_component(component),
            is_to_bind=False,
            dependencies=[self.source.with_component(component)],
            kw_dependencies={},
            type_=FactoryType.ALIAS,
            cache=self.cache,
            when_override=self.when_override,
            when_active=self.when_active,
            when_component=(
                component
                if self.when_component is None
                else self.when_component
            ),
            when_dependencies=[],
        )

    def __get__(self, instance: Any, owner: Any) -> "Alias":
        provider_when = getattr(instance, "when", None)
        if provider_when is None:
            return self
        return Alias(
            source=self.source,
            provides=self.provides,
            cache=self.cache,
            when_active=combine_when(provider_when, self.when_active),
            when_override=combine_when(provider_when, self.when_override),
            when_component=self.when_component,
        )
