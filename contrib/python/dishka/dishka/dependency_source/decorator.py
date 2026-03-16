from typing import Any, TypeVar, get_args, get_origin

from dishka.entities.component import Component
from dishka.entities.key import DependencyKey
from dishka.entities.marker import BaseMarker, combine_when
from dishka.entities.scope import BaseScope
from .factory import Factory
from .type_match import get_typevar_replacement, is_broader_or_same_type


class Decorator:
    __slots__ = ("factory", "generic", "provides", "scope", "when")

    def __init__(
            self,
            factory: Factory,
            provides: DependencyKey | None = None,
            scope: BaseScope | None = None,
            when: BaseMarker | None = None,
    ) -> None:
        self.factory = factory
        if provides:
            self.provides = provides
        else:
            self.provides = factory.provides
        self.scope = scope
        self.generic = self.is_generic()
        self.when = when

    def is_generic(self) -> bool:
        return (
            isinstance(self.provides.type_hint, TypeVar)
            or get_origin(self.provides.type_hint) is not None
        )

    def match_type(self, type_: Any) -> bool:
        return is_broader_or_same_type(self.provides.type_hint, type_)

    def as_factory(
            self, *,
            scope: BaseScope,
            new_dependency: DependencyKey,
            cache: bool,
            component: Component,
    ) -> Factory:
        typevar_replacement = get_typevar_replacement(
            self.provides.type_hint,
            new_dependency.type_hint,
        )
        if self.scope is not None:
            scope = self.scope

        return Factory(
            scope=scope,
            source=self.factory.source,
            provides=self.factory.provides.with_component(component),
            is_to_bind=self.factory.is_to_bind,
            dependencies=[
                self._replace_dep(
                    dep, new_dependency, typevar_replacement,
                ).with_component(component)
                for dep in self.factory.dependencies
            ],
            kw_dependencies={
                name: self._replace_dep(
                    dep, new_dependency, typevar_replacement,
                ).with_component(component)
                for name, dep in self.factory.kw_dependencies.items()
            },
            type_=self.factory.type,
            cache=cache,
            when_override=self.when,
            when_active=self.when,
            when_component=self.factory.when_component or component,
            when_dependencies=[],
        )

    def _replace_dep(
            self,
            old_key: DependencyKey,
            new_key: DependencyKey,
            typevar_replacement: dict[TypeVar, Any],
    ) -> DependencyKey:
        if old_key == self.provides:
            return new_key
        if get_origin(old_key.type_hint):
            args = tuple(
                typevar_replacement.get(t, t)
                for t in get_args(old_key.type_hint)
                if isinstance(t, TypeVar)
            )
            if not args:
                return old_key
            return DependencyKey(
                old_key.type_hint[args],
                self.provides.component,
            )
        return old_key

    def __get__(self, instance: Any, owner: Any) -> "Decorator":
        provider_when = getattr(instance, "when", None)
        combined_when = combine_when(provider_when, self.when)
        return Decorator(
            self.factory.__get__(instance, owner),
            scope=self.scope,
            when=combined_when,
        )
