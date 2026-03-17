from __future__ import annotations

from typing import Any, NoReturn

from dishka.entities.component import DEFAULT_COMPONENT, Component
from dishka.entities.factory_type import FactoryType
from dishka.entities.key import DependencyKey
from dishka.entities.marker import BoolMarker, HasContext
from dishka.entities.scope import BaseScope
from .alias import Alias
from .factory import Factory


def context_stub() -> NoReturn:
    raise NotImplementedError


class ContextVariable:
    __slots__ = ("override", "provides", "scope")

    def __init__(
            self, *,
            provides: DependencyKey,
            scope: BaseScope | None,
            override: bool,
    ) -> None:
        self.provides = provides
        self.scope = scope
        self.override = override

    def as_factory(
            self, component: Component,
    ) -> Factory:
        override = (BoolMarker(True) if self.override else None)

        if component == DEFAULT_COMPONENT:
            return Factory(
                scope=self.scope,
                source=self.provides.type_hint,
                provides=self.provides,
                is_to_bind=False,
                dependencies=[],
                kw_dependencies={},
                type_=FactoryType.CONTEXT,
                cache=False,
                when_override=override,
                when_active=HasContext(self.provides.type_hint),
                when_component=component,
                when_dependencies=[],
            )
        else:
            aliased = Alias(
                source=self.provides.with_component(DEFAULT_COMPONENT),
                cache=False,
                provides=DependencyKey(
                    component=component,
                    type_hint=self.provides.type_hint,
                ),
                when_override=override,
                when_active=HasContext(self.provides.type_hint),
                when_component=component,
            )
            return aliased.as_factory(scope=self.scope, component=component)

    def __get__(self, instance: Any, owner: Any) -> ContextVariable:
        scope = self.scope or getattr(instance, "scope", None)
        return ContextVariable(
            scope=scope,
            provides=self.provides,
            override=self.override,
        )
