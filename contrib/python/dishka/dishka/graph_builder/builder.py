import itertools
from collections import defaultdict
from collections.abc import Collection, Sequence
from typing import cast

from dishka.dependency_source import (
    Activator,
    Alias,
    ContextVariable,
    Decorator,
    Factory,
    FactoryUnionMode,
)
from dishka.entities.component import Component
from dishka.entities.factory_type import FactoryData, FactoryType
from dishka.entities.key import DependencyKey
from dishka.entities.marker import unpack_marker
from dishka.entities.scope import BaseScope, InvalidScopes
from dishka.entities.validation_settings import ValidationSettings
from dishka.exception_base import InvalidMarkerError
from dishka.exceptions import (
    ActivatorOverrideError,
    AliasedFactoryNotFoundError,
    CycleDependenciesError,
    GraphMissingFactoryError,
    NoActivatorError,
    UnknownScopeError,
)
from dishka.provider import BaseProvider, ProviderWrapper
from dishka.registry import Registry
from dishka.text_rendering.name import get_source_name
from .moved_objects_tracker import MovedObjectsTracker
from .uniter import (
    CollectionGroupProcessor,
    SelectorGroupProcessor,
)
from .validator import GraphValidator


class GraphBuilder:
    def __init__(
            self,
            *,
            scopes: type[BaseScope],
            container_key: DependencyKey,
            skip_validation: bool,
            validation_settings: ValidationSettings,
    ):
        self.scopes = scopes
        self.container_key = container_key
        self.skip_validation = skip_validation
        self.validation_settings = validation_settings
        self.moved_objects_tracker = MovedObjectsTracker()
        # group processors
        self.selector_group_processor = SelectorGroupProcessor(
            validation_settings=validation_settings,
            skip_validation=skip_validation,
            moved_objects_tracker=self.moved_objects_tracker,
        )
        self.collection_group_processor = CollectionGroupProcessor(
            validation_settings=validation_settings,
            skip_validation=skip_validation,
            moved_objects_tracker=self.moved_objects_tracker,
        )

        # registered objects
        self.components: dict[Component, None] = {}  # keep order
        self.decorator_depth: dict[DependencyKey, int] = {}
        self.factories: dict[DependencyKey, list[Factory]] = defaultdict(list)
        self.requested_markers: dict[
            DependencyKey, list[Factory],
        ] = defaultdict(list)
        # for multicomponent processing
        self.multicomponent_providers: list[BaseProvider] = []
        # for delayed processing
        self.marker_aliases_to: dict[DependencyKey, DependencyKey] = {}
        self.activators: dict[DependencyKey, Activator] = {}
        self.union_modes: dict[DependencyKey, FactoryUnionMode] = {}

    def add_multicomponent_providers(self, *providers: BaseProvider) -> None:
        self.multicomponent_providers.extend(providers)
        for component in self.components:
            for provider in providers:
                self._add_provider(ProviderWrapper(component, provider))

    def add_providers(self, *providers: BaseProvider) -> None:
        for provider in providers:
            self._add_provider(provider)

    def _add_provider(self, provider: BaseProvider) -> None:
        component = provider.component
        self._add_component(component)
        for activation in provider.activators:
            self._process_activator(component, activation)
        for factory in provider.factories:
            self._process_factory(component, factory)
        for alias in provider.aliases:
            self._process_alias(component, alias)
        for context_var in provider.context_vars:
            self._process_context_var(component, context_var)
        for union_mode in provider.factory_union_mode:
            self._process_union_mode(component, union_mode)
        for decorator in provider.decorators:
            self._process_decorator(component, decorator)

    def _add_factory(self, factory: Factory) -> None:
        self.factories[factory.provides].append(factory)
        self._register_factory_markers(factory)

    def _add_component(self, component: Component) -> None:
        if component in self.components:
            return

        self.components[component] = None
        for provider in self.multicomponent_providers:
            self._add_provider(ProviderWrapper(component, provider))

    def _process_alias(self, component: Component, src: Alias) -> None:
        alias_source = src.source.with_component(component)
        if src.provides.is_marker():
            provides = src.provides.with_component(component)
            self.marker_aliases_to[provides] = alias_source
            return
        factory = src.as_factory(None, component)
        self._add_factory(factory)

    def _process_factory(self, component: Component, src: Factory) -> None:
        if not isinstance(src.scope, self.scopes):
            raise UnknownScopeError(src.scope, self.scopes, src)
        for dep in src.dependencies:
            if dep == src.provides:
                raise CycleDependenciesError([src])
        for dep in src.kw_dependencies.values():
            if dep == src.provides:
                raise CycleDependenciesError([src])

        factory = src.with_component(component)
        self._add_factory(factory)

    def _process_context_var(
            self,
            component: Component,
            src: ContextVariable,
    ) -> None:
        factory = src.as_factory(component)
        if not isinstance(src.scope, self.scopes):
            raise UnknownScopeError(src.scope, self.scopes, factory)
        self._add_factory(factory)

    def _collect_decorating_keys(
        self,
        src: Decorator,
        component: Component,
    ) -> list[DependencyKey]:
        provides = src.provides.with_component(component)
        if src.is_generic():
            found = []
            for factory_provides, group in self.factories.items():
                if (
                        not group or
                        factory_provides.depth > 0 or  # inner function
                        factory_provides.component != provides.component or
                        not src.match_type(factory_provides.type_hint)
                ):
                    continue
                found.append(factory_provides)
            if found:
                return found
        elif provides in self.factories:
            return [provides]

        if (
                self.skip_validation or
                not self.validation_settings.nothing_decorated
        ):
            return []
        raise GraphMissingFactoryError(
            requested=provides,
            path=[src.as_factory(
                scope=InvalidScopes.UNKNOWN_SCOPE,
                new_dependency=provides,
                cache=False,
                component=cast(Component, provides.component),  # fixed above
            )],
        )

    def _is_alias_source_decorated(
        self,
        decorator: Decorator,
        component: Component,
        factory: Factory,
        processed_path: list[Factory],
    ) -> bool:
        if factory in processed_path:
            raise CycleDependenciesError(processed_path)
        if factory.type is not FactoryType.ALIAS:
            return False

        processed_path = [*processed_path, factory]
        dependency = factory.dependencies[0]

        # will be decorated
        if (
            decorator.is_generic() and
            decorator.match_type(dependency.type_hint) and
            dependency.component == component
        ):
            return True

        factory_list = self.factories.get(dependency)
        if not factory_list:
            raise AliasedFactoryNotFoundError(dependency, factory)
        factory = factory_list[0]  # any factory
        # already generated
        if factory.source is decorator.factory.source:
            return True
        if factory.type is FactoryType.ALIAS:
            return self._is_alias_source_decorated(
                decorator,
                component,
                factory,
                processed_path,
            )
        return False

    def _decorate_group(
        self,
        decorator: Decorator,
        provides: DependencyKey,
    ) -> None:
        group_replacement = []
        decorated_groups = {}
        old_group = self.factories[provides]
        for old_factory in old_group:
            if self._is_alias_source_decorated(
                decorator=decorator,
                # should be fixes on group creation
                component=cast(Component, provides.component),
                factory=old_factory,
                processed_path=[],
            ):
                return

            decorated_provides = self.moved_objects_tracker.move(provides)
            new_factory = old_factory.replace(
                provides=decorated_provides,
                when_active=None,
                when_override=None,
            )
            decorated_groups[decorated_provides] = [new_factory]
            decorated_factory = decorator.as_factory(
                scope=cast(BaseScope, old_factory.scope),
                new_dependency=decorated_provides,
                cache=old_factory.cache,
                component=cast(Component, provides.component),
            ).replace(
                provides=provides,
                when_active=old_factory.when_active,
                when_override=old_factory.when_override,
                when_component=cast(Component, old_factory.when_component),
            )
            if decorator.when is not None:
                conditional_factory = new_factory.replace(
                    when_override=~decorator.when,
                    when_active=~decorator.when,
                    when_component=cast(Component, provides.component),
                )
                decorated_factory.when_dependencies = [conditional_factory]
            group_replacement.append(decorated_factory)
        self.factories[provides] = group_replacement
        self.factories.update(decorated_groups)

    def _process_decorator(self, component: Component, src: Decorator) -> None:
        if src.scope and not isinstance(src.scope, self.scopes):
            raise UnknownScopeError(src.scope, self.scopes, src.factory)
        to_decorate = self._collect_decorating_keys(src, component)
        for key in to_decorate:
            self._decorate_group(src, key)
            self._register_factory_markers(src.as_factory(
                scope=next(iter(self.scopes)),
                cache=False,
                component=component,
                new_dependency=src.provides,
            ))

    def _process_union_mode(self, component: Component,
                            src: FactoryUnionMode) -> None:
        src = src.with_component(component)
        self.union_modes[src.source] = src

        factory = src.as_factory()
        if factory:
            self.factories[factory.provides].append(factory)

    def _process_activator(self, component: Component, src: Activator) -> None:
        src = src.with_component(component)
        marker = src.marker or src.marker_type
        if marker is None:
            raise InvalidMarkerError(marker)
        key = DependencyKey(marker, src.factory.when_component)
        if key in self.activators:
            raise ActivatorOverrideError(
                marker,
                [src.factory, self.activators[key].factory],
            )
        self.activators[key] = src

    def _get_factory_union_mode(self, key: DependencyKey) -> FactoryUnionMode:
        if key in self.union_modes:
            return self.union_modes[key]
        return FactoryUnionMode(
            scope=None,
            cache=False,
            collect=False,
            provides=key,
            source=key,
        )

    def _register_factory_markers(self, factory: Factory) -> None:
        try:
            markers = {
                *unpack_marker(factory.when_active),
                *unpack_marker(factory.when_override),
            }
        except InvalidMarkerError as e:
            e.source_name = get_source_name(factory)
            raise

        for marker in markers:
            marker_key = DependencyKey(marker, factory.when_component)
            self.requested_markers[marker_key].append(factory)

    def _collect_prepared_factories(self) -> list[Factory]:
        factories = []
        collection_factories: dict[DependencyKey, Factory] = {
            f.source: f
            for group in self.factories.values()
            for f in group
            if f.type is FactoryType.COLLECTION
        }

        # collect existing
        for key, factory_group in self.factories.items():
            mode = self._get_factory_union_mode(key)
            if mode.collect:
                existing_factory = collection_factories.pop(key)
                factories.extend(self.collection_group_processor.unite(
                    union_mode=mode,
                    provides=key,
                    group=factory_group,
                    collection_factory=existing_factory,
                ))
            else:
                factories.extend(
                    self.selector_group_processor.unite(
                        union_mode=mode,
                        provides=key,
                        group=factory_group,
                    ),
                )
        return factories

    def _calc_scope(
            self,
            factory: Factory,
            all_factories: dict[DependencyKey, Factory],
            scopes_cache: dict[DependencyKey, BaseScope],
            path: list[FactoryData],
            requester_scope: BaseScope,
    ) -> BaseScope:
        if factory.scope:
            return factory.scope
        if factory in path:
            raise CycleDependenciesError(path)

        if factory.provides in scopes_cache:
            return scopes_cache[factory.provides]
        sub_factories: list[Factory] = []
        for dep in itertools.chain(
            factory.dependencies,
            factory.kw_dependencies.values(),
        ):
            if dep == self.container_key:
                return requester_scope
            if dep in all_factories:
                sub_factories.append(all_factories[dep])
        sub_factories.extend(factory.when_dependencies)

        scopes = [
            self._calc_scope(
                factory=sub_factory,
                all_factories=all_factories,
                scopes_cache=scopes_cache,
                path=[*path, factory],
                requester_scope=requester_scope,
            )
            for sub_factory in sub_factories
        ]
        if scopes:
            scope = max(scopes)
        else:
            scope = requester_scope
        scopes_cache[factory.provides] = scope
        return scope

    def _make_registries(self, factories: list[Factory]) -> Sequence[Registry]:
        registries: dict[BaseScope, Registry] = {}
        has_fallback = True
        for scope in self.scopes:
            registry = Registry(scope, has_fallback=has_fallback)
            context_var = ContextVariable(
                provides=self.container_key,
                scope=scope,
                override=False,
            )
            for component in self.components:
                registry.add_factory(context_var.as_factory(component))
            registries[scope] = registry
            has_fallback = False

        for factory in factories:
            scope = cast(BaseScope, factory.scope)
            registries[scope].add_factory(factory, factory.provides)
        return tuple(registries.values())

    def _find_activator(
        self,
        key: DependencyKey,
    ) -> tuple[DependencyKey, Activator | None]:
        if key in self.activators:
            return key, self.activators[key]

        if key in self.marker_aliases_to:
            return self._find_activator(
                self.marker_aliases_to[key],
            )

        type_key = DependencyKey(type(key.type_hint), key.component)
        if type_key in self.activators:
            return key, self.activators[type_key]
        if type_key in self.marker_aliases_to:
            # type aliases for markers always keep type, but change component
            new_key = DependencyKey(
                key.type_hint,
                self.marker_aliases_to[type_key].component,
            )
            return self._find_activator(new_key)

        return key, None

    def _check_markers(self) -> None:
        for marker_key, factories in self.requested_markers.items():
            _, activator = self._find_activator(marker_key)
            if not activator and not self.skip_validation:
                raise NoActivatorError(marker_key, factories)

    def _collect_markers(
        self,
        factories: Sequence[Factory],
    ) ->  Collection[tuple[DependencyKey, BaseScope]]:
        # use dict to keep stable order
        processed_markers: dict[tuple[DependencyKey, BaseScope], None] = {}
        for factory in factories:
            for marker in unpack_marker(factory.when_active):
                key = DependencyKey(marker, factory.when_component)
                if factory.scope is None:
                    raise UnknownScopeError(
                        factory.scope,
                        self.scopes,
                        factory,
                    )
                processed_markers[key, factory.scope] = None
            for subfactory in factory.when_dependencies:
                for marker in unpack_marker(subfactory.when_override):
                    if subfactory.scope is None:
                        raise UnknownScopeError(
                            subfactory.scope,
                            self.scopes,
                            subfactory,
                        )
                    key = DependencyKey(marker, factory.when_component)
                    processed_markers[key, subfactory.scope] = None
        return processed_markers

    def _get_activator_factories(
        self,
        factories: Sequence[Factory],
        markers: Collection[tuple[DependencyKey, BaseScope]],
    ) -> list[Factory]:
        factories_dict = {f.provides: f for f in factories}
        activator_factories: list[Factory] = []
        scope_cache: dict[DependencyKey, BaseScope] = {}
        for marker_key, scope in markers:
            found_key, activator = self._find_activator(marker_key)
            if not activator:
                # should be also checked before with all factories info
                raise NoActivatorError(marker_key, [])
            activator_factory = activator.as_factory(
                None,
                found_key.component,
                found_key,
            ).replace(provides=marker_key)
            activator_scope = self._calc_scope(
                factory=activator_factory,
                all_factories=factories_dict,
                scopes_cache=scope_cache,
                path=[],
                requester_scope=scope,
            )
            activator_factory = activator_factory.replace(
                scope=activator_scope,
            )
            activator_factories.append(activator_factory)
        return activator_factories

    def _fix_missing_scopes(
        self,
        factories: dict[DependencyKey, Factory],
    ) -> None:
        root_scope = next(iter(self.scopes))
        factory_scopes: dict[DependencyKey, BaseScope] = {}
        for factory in factories.values():
            factory.scope = self._calc_scope(
                factory=factory,
                all_factories=factories,
                scopes_cache=factory_scopes,
                path=[],
                requester_scope=root_scope,
            )

    def build(self) -> Sequence[Registry]:
        self._check_markers()
        factories: dict[DependencyKey, Factory] = {
            f.provides: f for f in self._collect_prepared_factories()
        }
        self._fix_missing_scopes(factories)
        fixed_factories = list(factories.values())

        found_markers = self._collect_markers(fixed_factories)
        fixed_factories.extend(
            self._get_activator_factories(fixed_factories, found_markers),
        )
        registries = self._make_registries(fixed_factories)
        if not self.skip_validation:
            GraphValidator(registries).validate()
        return registries
