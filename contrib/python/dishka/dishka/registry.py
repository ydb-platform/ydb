from abc import ABC, ABCMeta
from enum import Enum
from typing import (
    Any,
    Final,
    Generic,
    Protocol,
    TypeAlias,
    TypeVar,
    get_args,
    get_origin,
)

from ._adaptix.type_tools.basic_utils import is_generic
from ._adaptix.type_tools.fundamentals import get_type_vars
from .code_tools.factory_compiler import compile_activation, compile_factory
from .container_objects import CompiledFactory
from .dependency_source import (
    Factory,
)
from .dependency_source.type_match import (
    get_typevar_replacement,
    is_broader_or_same_type,
)
from .entities.factory_type import FactoryType
from .entities.key import DependencyKey
from .entities.marker import Marker
from .entities.scope import BaseScope

IGNORE_TYPES: Final = (
    type,
    object,
    Enum,
    ABC,
    ABCMeta,
    Generic,
    Protocol,
    Exception,
    BaseException,
)

CompiledFactories: TypeAlias = dict[DependencyKey, CompiledFactory]


class Registry:
    __slots__ = (
        "compiled",
        "compiled_activation",
        "compiled_activation_async",
        "compiled_async",
        "factories",
        "has_fallback",
        "scope",
    )

    def __init__(self, scope: BaseScope, *, has_fallback: bool) -> None:
        self.scope = scope
        self.factories: dict[DependencyKey, Factory] = {}
        self.compiled: CompiledFactories = {}
        self.compiled_async: CompiledFactories = {}
        self.compiled_activation: CompiledFactories = {}
        self.compiled_activation_async: CompiledFactories = {}
        self.has_fallback = has_fallback

    def add_factory(
            self,
            factory: Factory,
            provides: DependencyKey | None = None,
    ) -> None:
        if provides is None:
            provides = factory.provides
        self.factories[provides] = factory
        if is_generic(factory.provides.type_hint):
            origin = get_origin(factory.provides.type_hint)
            origin_key = DependencyKey(
                origin,
                factory.provides.component,
                factory.provides.depth,
            )
            self.factories[origin_key] = factory

    def get_compiled(
            self, dependency: DependencyKey,
    ) -> CompiledFactory | None:
        try:
            return self.compiled[dependency]
        except KeyError:
            factory = self.get_factory(dependency)
            if not factory:
                return None
            compiled = compile_factory(factory=factory, is_async=False)
            self.compiled[dependency] = compiled
            return compiled

    def get_compiled_async(
            self, dependency: DependencyKey,
    ) -> CompiledFactory | None:
        try:
            return self.compiled_async[dependency]
        except KeyError:
            factory = self.get_factory(dependency)
            if not factory:
                return None
            compiled = compile_factory(factory=factory, is_async=True)
            self.compiled_async[dependency] = compiled
            return compiled

    def get_compiled_activation(
            self, dependency: DependencyKey,
    ) -> CompiledFactory | None:
        try:
            return self.compiled_activation[dependency]
        except KeyError:
            factory = self.get_factory(dependency)
            if not factory:
                return None
            compiled = compile_activation(factory=factory, is_async=False)
            self.compiled_activation[dependency] = compiled
            return compiled

    def get_compiled_activation_async(
            self, dependency: DependencyKey,
    ) -> CompiledFactory | None:
        try:
            return self.compiled_activation_async[dependency]
        except KeyError:
            factory = self.get_factory(dependency)
            if not factory:
                return None
            compiled = compile_activation(factory=factory, is_async=True)
            self.compiled_activation_async[dependency] = compiled
            return compiled

    def get_factory(self, dependency: DependencyKey) -> Factory | None:
        try:
            return self.factories[dependency]
        except KeyError:
            if isinstance(dependency.type_hint, Marker):
                return None

            origin = get_origin(dependency.type_hint)
            if not origin:
                return None

            if (origin is type) and self.has_fallback:
                return self._get_type_var_factory(dependency)

            origin_key = DependencyKey(
                origin,
                dependency.component,
                dependency.depth,
            )
            factory = self.factories.get(origin_key)

            if (
                not factory or
                not is_broader_or_same_type(
                    factory.provides.type_hint,
                    dependency.type_hint,
                )
            ):
                return None
            factory = self._specialize_generic(factory, dependency)
            self.factories[dependency] = factory
            return factory

    def get_more_abstract_factories(
        self,
        dependency: DependencyKey,
    ) -> list[Factory]:
        abstract_dependencies: list[Factory] = []
        try:
            abstract_classes = dependency.type_hint.__bases__
        except AttributeError:
            abstract_classes = ()

        for abstract_class in abstract_classes:
            abstract_dependency = DependencyKey(
                abstract_class,
                dependency.component,
            )

            factory = self.factories.get(abstract_dependency)
            if factory is not None:
                abstract_dependencies.append(factory)

        return abstract_dependencies

    def get_more_concrete_factories(
        self,
        dependency: DependencyKey,
    ) -> list[Factory]:
        concrete_factories: list[Factory] = []

        check_type_hint = dependency.type_hint

        if check_type_hint in IGNORE_TYPES:
            return concrete_factories

        try:
            subclasses: list[Any] = check_type_hint.__subclasses__()
        except AttributeError:
            subclasses = []

        for subclass in subclasses:
            concrete_dependency = DependencyKey(
                subclass,
                dependency.component,
            )
            factory = self.factories.get(concrete_dependency)
            if factory is not None:
                concrete_factories.append(factory)

        return concrete_factories

    def _get_type_var_factory(self, dependency: DependencyKey) -> Factory:
        args = get_args(dependency.type_hint)
        if args:
            typevar = args[0]
        else:
            typevar = Any
        return Factory(
            scope=self.scope,
            dependencies=[],
            kw_dependencies={},
            provides=DependencyKey(type[typevar], dependency.component),
            type_=FactoryType.FACTORY,
            is_to_bind=False,
            cache=False,
            source=lambda: typevar,
            when_override=None,
            when_active=None,
            when_component=None,
            when_dependencies=[],
        )

    def _specialize_generic(
            self, factory: Factory, dependency_key: DependencyKey,
    ) -> Factory:
        params_replacement = get_typevar_replacement(
            factory.provides.type_hint,
            dependency_key.type_hint,
        )
        new_dependencies: list[DependencyKey] = []
        for source_dependency in factory.dependencies:
            hint = source_dependency.type_hint
            if isinstance(hint, TypeVar):
                hint = params_replacement[hint]
            elif get_origin(hint) and (type_vars := get_type_vars(hint)):
                hint = hint[tuple(
                    params_replacement[param]
                    for param in type_vars
                )]
            new_dependencies.append(DependencyKey(
                hint, source_dependency.component, source_dependency.depth,
            ))
        new_kw_dependencies: dict[str, DependencyKey] = {}
        for name, source_dependency in factory.kw_dependencies.items():
            hint = source_dependency.type_hint
            if isinstance(hint, TypeVar):
                hint = params_replacement[hint]
            elif get_origin(hint) and (type_vars := get_type_vars(hint)):
                hint = hint[tuple(
                    params_replacement[param]
                    for param in type_vars
                )]
            new_kw_dependencies[name] = DependencyKey(
                hint, source_dependency.component, source_dependency.depth,
            )
        return Factory(
            source=factory.source,
            provides=dependency_key,
            dependencies=new_dependencies,
            kw_dependencies=new_kw_dependencies,
            is_to_bind=factory.is_to_bind,
            type_=factory.type,
            scope=factory.scope,
            cache=factory.cache,
            when_override=factory.when_override,
            when_active=factory.when_active,
            when_component=factory.when_component,
            when_dependencies=factory.when_dependencies,
        )
