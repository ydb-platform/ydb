import itertools
from collections.abc import Sequence

from dishka.dependency_source import Factory
from dishka.entities.key import DependencyKey
from dishka.exceptions import (
    CycleDependenciesError,
    GraphMissingFactoryError,
    InvalidSubfactoryScopeError,
    NoFactoryError,
)
from dishka.registry import Registry


class GraphValidator:
    def __init__(self, registries: Sequence[Registry]) -> None:
        self.registries = registries
        self.path: dict[DependencyKey, Factory] = {}
        self.valid_keys: dict[DependencyKey, bool] = {}

    def _validate_key(
        self,
        key: DependencyKey,
        registry_index: int,
    ) -> None:
        if key in self.valid_keys:
            return
        if key.is_const():
            return
        if key.type_hint is DependencyKey:
            return
        if key in self.path:
            keys = list(self.path)
            factories = list(self.path.values())[keys.index(key):]
            raise CycleDependenciesError(factories)

        suggest_abstract_factories = []
        suggest_concrete_factories = []
        for index in range(registry_index + 1):
            registry = self.registries[index]
            factory = registry.get_factory(key)
            if factory:
                self._validate_factory(factory, registry_index)
                return

            abstract_factories = registry.get_more_abstract_factories(key)
            concrete_factories = registry.get_more_concrete_factories(key)
            suggest_abstract_factories.extend(abstract_factories)
            suggest_concrete_factories.extend(concrete_factories)

        raise NoFactoryError(
            requested=key,
            suggest_abstract_factories=suggest_abstract_factories,
            suggest_concrete_factories=suggest_concrete_factories,
        )

    def _validate_factory(
            self, factory: Factory, registry_index: int,
    ) -> None:
        self.path[factory.provides] = factory
        if (
            factory.provides in factory.kw_dependencies.values() or
            factory.provides in factory.dependencies
        ):
            raise CycleDependenciesError([factory])

        try:
            for dep in itertools.chain(
                factory.dependencies,
                factory.kw_dependencies.values(),
            ):
                # ignore TypeVar and const parameters
                if not dep.is_type_var() and not dep.is_const():
                    self._validate_key(dep, registry_index)
        except NoFactoryError as e:
            e.add_path(factory)
            raise
        finally:
            self.path.pop(factory.provides)

        if factory.scope is None:
            raise ValueError  # should be checked in builder
        for subfactory in factory.when_dependencies:
            if subfactory.scope is None:
                raise ValueError  # should be checked in builder
            if subfactory.scope > factory.scope:
                raise InvalidSubfactoryScopeError(factory, subfactory)

        self.valid_keys[factory.provides] = True

    def validate(self) -> None:
        for registry_index, registry in enumerate(self.registries):
            factories = tuple(registry.factories.values())
            for factory in factories:
                self.path = {}
                try:
                    self._validate_factory(factory, registry_index)
                except NoFactoryError as e:
                    raise GraphMissingFactoryError(
                        e.requested,
                        e.path,
                        self._find_other_scope(e.requested),
                        self._find_other_component(e.requested),
                        e.suggest_abstract_factories,
                        e.suggest_concrete_factories,
                    ) from None
                except CycleDependenciesError as e:
                    raise e from None

    def _find_other_scope(self, key: DependencyKey) -> list[Factory]:
        found = []
        for registry in self.registries:
            for factory_key, factory in registry.factories.items():
                if factory_key == key:
                    found.append(factory)
        return found

    def _find_other_component(self, key: DependencyKey) -> list[Factory]:
        found = []
        for registry in self.registries:
            for factory_key, factory in registry.factories.items():
                if factory_key.type_hint != key.type_hint:
                    continue
                if factory_key.component == key.component:
                    continue
                found.append(factory)
        return found
