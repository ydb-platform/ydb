from __future__ import annotations

import warnings
from collections.abc import Callable, MutableMapping
from contextlib import AbstractContextManager
from threading import Lock
from types import TracebackType
from typing import Any, TypeVar, cast, overload

from dishka.entities.component import DEFAULT_COMPONENT, Component
from dishka.entities.factory_type import FactoryType
from dishka.entities.key import DependencyKey
from dishka.entities.marker import Has, HasContext
from dishka.entities.scope import BaseScope, Scope
from dishka.provider import Provider, activate
from .container_objects import Exit
from .context_proxy import ContextProxy
from .dependency_source import Factory
from .entities.validation_settings import (
    DEFAULT_VALIDATION,
    ValidationSettings,
)
from .exceptions import (
    ChildScopeNotFoundError,
    ExitError,
    NoActiveFactoryError,
    NoChildScopesError,
    NoFactoryError,
    NoNonSkippedScopesError,
)
from .graph_builder.builder import GraphBuilder
from .provider import BaseProvider, make_root_context_provider
from .registry import Registry

T = TypeVar("T")


class Container:
    __slots__ = (
        "_cache",
        "_context",
        "_exits",
        "child_registries",
        "close_parent",
        "lock",
        "parent_container",
        "registry",
    )

    def __init__(
            self,
            registry: Registry,
            *child_registries: Registry,
            parent_container: Container | None = None,
            context: dict[Any, Any] | None = None,
            lock_factory: Callable[
                [], AbstractContextManager[Any],
            ] | None = None,
            close_parent: bool = False,
    ):
        self.registry = registry
        self.child_registries = child_registries
        self._context = {CONTAINER_KEY: self}
        if context:
            self._context.update(context)
        self._cache = {CONTAINER_KEY: self}
        self.parent_container = parent_container

        self.lock: AbstractContextManager[Any] | None
        if lock_factory:
            self.lock = lock_factory()
        else:
            self.lock = None
        self._exits: list[Exit] = []
        self.close_parent = close_parent

    @property
    def scope(self) -> BaseScope:
        return self.registry.scope

    @property
    def context(self) -> MutableMapping[DependencyKey, Any]:
        warnings.warn(
            "`container.context` is deprecated",
            DeprecationWarning,
            stacklevel=2,
        )
        return ContextProxy(cache=self._cache, context=self._context)

    def __call__(
            self,
            context: dict[Any, Any] | None = None,
            lock_factory: Callable[
                [], AbstractContextManager[Any],
            ] | None = None,
            scope: BaseScope | None = None,
    ) -> ContextWrapper:
        """
        Prepare container for entering the inner scope.
        :param context: Data which will available in inner scope
        :param lock_factory: Callable to create lock instance or None
        :param scope: target scope or None to enter next non-skipped scope
        :return: context manager for inner scope
        """
        if not self.child_registries:
            raise NoChildScopesError
        child = Container(
            *self.child_registries,
            parent_container=self,
            context=context,
            lock_factory=lock_factory,
        )
        if scope is None:
            while child.registry.scope.skip:
                if not child.child_registries:
                    raise NoNonSkippedScopesError
                child = Container(
                    *child.child_registries,
                    parent_container=child,
                    context=context,
                    lock_factory=lock_factory,
                    close_parent=True,
                )
        else:
            while child.registry.scope is not scope:
                if not child.child_registries:
                    raise ChildScopeNotFoundError(scope, self.registry.scope)
                child = Container(
                    *child.child_registries,
                    parent_container=child,
                    context=context,
                    lock_factory=lock_factory,
                    close_parent=True,
                )
        return ContextWrapper(child)

    @overload
    def get(
            self,
            dependency_type: type[T],
            component: Component | None = DEFAULT_COMPONENT,
    ) -> T:
        ...

    @overload
    def get(
            self,
            dependency_type: Any,
            component: Component | None = DEFAULT_COMPONENT,
    ) -> Any:
        ...

    def get(
            self,
            dependency_type: Any,
            component: Component | None = DEFAULT_COMPONENT,
    ) -> Any:
        lock = self.lock
        key = DependencyKey(dependency_type, component)
        try:
            if not lock:
                return self._get_unlocked(key)
            with lock:
                return self._get_unlocked(key)
        except (NoFactoryError, NoActiveFactoryError) as e:
            e.scope = self.scope
            raise

    def _get(self, key: DependencyKey) -> Any:
        lock = self.lock
        if not lock:
            return self._get_unlocked(key)
        with lock:
            return self._get_unlocked(key)

    def _get_unlocked(self, key: DependencyKey) -> Any:
        if key in self._cache:
            return self._cache[key]
        compiled = self.registry.get_compiled(key)
        if not compiled:
            if not self.parent_container:
                abstract_dependencies = (
                    self.registry.get_more_abstract_factories(key)
                )
                concrete_dependencies = (
                    self.registry.get_more_concrete_factories(key)
                )

                raise NoFactoryError(
                    key,
                    suggest_abstract_factories=abstract_dependencies,
                    suggest_concrete_factories=concrete_dependencies,
                )
            try:
                return self.parent_container._get(key)  # noqa: SLF001
            except NoFactoryError as ex:
                abstract_dependencies = (
                    self.registry.get_more_abstract_factories(key)
                )
                concrete_dependencies = (
                    self.registry.get_more_concrete_factories(key)
                )
                ex.suggest_abstract_factories.extend(abstract_dependencies)
                ex.suggest_concrete_factories.extend(concrete_dependencies)
                raise

        try:
            return compiled(
                self._get_unlocked,
                self._exits,
                self._cache,
                self._context,
            )
        except NoFactoryError as e:
            # cast is needed because registry.get_factory will always
            # return Factory. This happens because registry.get_compiled
            # uses the same method and returns None if the factory is not found
            # If None is returned, then go to the parent container
            e.add_path(cast(Factory, self.registry.get_factory(key)))
            raise
        except NoActiveFactoryError as e:
            e.add_path(cast(Factory, self.registry.get_factory(key)))
            raise

    def close(self, exception: BaseException | None = None) -> None:
        errors = []
        for exit_generator in self._exits[::-1]:
            try:
                if exit_generator.type is FactoryType.GENERATOR:
                    exit_generator.callable.send(exception)  # type: ignore[attr-defined]
            except StopIteration:  # noqa: PERF203
                pass
            except Exception as err:  # noqa: BLE001
                errors.append(err)
        self._cache = {CONTAINER_KEY: self}
        if self.close_parent and self.parent_container:
            try:
                self.parent_container.close(exception)
            except Exception as err:  # noqa: BLE001
                errors.append(err)

        if errors:
            raise ExitError("Cleanup context errors", errors)  # noqa: TRY003

    def _has(self, marker: DependencyKey) -> bool:
        compiled = self.registry.get_compiled_activation(marker)
        if not compiled:
            if not self.parent_container:
                return False
            return self.parent_container._has(marker)  # noqa: SLF001
        return bool(compiled(
            self._get_unlocked,
            self._exits,
            self._cache,
            self._context,
        ))

    def _has_context(self, marker: Any) -> bool:
        return marker in self._context


class ContextWrapper:
    __slots__ = ("container",)

    def __init__(self, container: Container):
        self.container = container

    def __enter__(self) -> Container:
        return self.container

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        self.container.close(exception=exc_val)


class HasProvider(Provider):
    @activate(Has)
    def has(
        self,
        marker: DependencyKey,
        container: Container,
    ) -> bool:
        key = DependencyKey(marker.type_hint.value, marker.component)
        return container._has(key)  # noqa: SLF001

    @activate(HasContext)
    def has_context(
        self,
        marker: HasContext,
        container: Container,
    ) -> bool:
        return container._has_context(marker.value)   # noqa: SLF001


def make_container(
        *providers: BaseProvider,
        scopes: type[BaseScope] = Scope,
        context: dict[Any, Any] | None = None,
        lock_factory: Callable[[], AbstractContextManager[Any]] | None = Lock,
        skip_validation: bool = False,
        start_scope: BaseScope | None = None,
        validation_settings: ValidationSettings = DEFAULT_VALIDATION,
) -> Container:
    context_provider = make_root_context_provider(providers, context, scopes)
    has_provider = HasProvider()
    builder = GraphBuilder(
        scopes=scopes,
        container_key=CONTAINER_KEY,
        skip_validation=skip_validation,
        validation_settings=validation_settings,
    )
    builder.add_multicomponent_providers(has_provider)
    builder.add_providers(*providers)
    builder.add_providers(context_provider)
    registries = builder.build()
    container = Container(
        *registries,
        context=context,
        lock_factory=lock_factory,
    )
    if start_scope is None:
        while container.registry.scope.skip:
            container = Container(
                *container.child_registries,
                parent_container=container,
                context=context,
                lock_factory=lock_factory,
                close_parent=True,
            )
    else:
        while container.registry.scope is not start_scope:
            container = Container(
                *container.child_registries,
                parent_container=container,
                context=context,
                lock_factory=lock_factory,
                close_parent=True,
            )
    return container


CONTAINER_KEY = DependencyKey(Container, DEFAULT_COMPONENT)
