from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, TypeAlias, cast

from dishka.code_tools.code_builder import CodeBuilder
from dishka.container_objects import CompiledFactory, Exit
from dishka.dependency_source import Factory
from dishka.entities.component import Component
from dishka.entities.factory_type import FactoryType
from dishka.entities.key import DependencyKey
from dishka.entities.marker import (
    AndMarker,
    BaseMarker,
    BoolMarker,
    NotMarker,
    OrMarker,
)
from dishka.exceptions import (
    NoActiveFactoryError,
    NoContextValueError,
    UnsupportedFactoryError,
)
from dishka.text_rendering import get_name


class FactoryBuilder(CodeBuilder):
    def __init__(self, *, is_async: bool, getter_prefix: str):
        super().__init__(is_async=is_async)
        self.provides_name = ""
        self.getter_name = ""
        self.getter_prefix = getter_prefix

    def global_(self, obj: Any, preferred_name: str | None = None) -> str:
        if preferred_name is None and isinstance(obj, DependencyKey):
            type_name = get_name(obj.type_hint, include_module=False)
            preferred_name = f"key_{type_name}"
            if obj.component:
                preferred_name += f"_{obj.component}"
            if obj.depth:
                preferred_name += f"_{obj.depth}"
        return super().global_(obj, preferred_name)

    def register_provides(self, provides: DependencyKey) -> None:
        self.provides_name = self.global_(provides)

    def make_getter(self) -> AbstractContextManager[None]:
        raw_provides_name = self.provides_name.removeprefix("key_")
        self.getter_name = self.getter_prefix + raw_provides_name
        return self.def_(
            self.getter_name,
            ["getter", "exits", "cache", "context"],
        )

    def getter(self, obj: DependencyKey) -> str:
        if obj.is_const():
            return self.global_(obj.get_const_value())
        if obj.type_hint is DependencyKey:
            return self.provides_name
        return self.await_(self.call("getter", self.global_(obj)))

    def cache(self) -> None:
        self.assign_expr(f"cache[{self.provides_name}]", "solved")

    def assign_solved(self, expr: str) -> None:
        self.assign_local("solved", expr)

    def when(
        self,
        marker: BaseMarker | None,
        component: Component | None,
    ) -> str:
        match marker:
            case None | BoolMarker(True):
                return ""
            case AndMarker():
                return self.and_(
                    self.when(marker.left, component),
                    self.when(marker.right, component),
                )
            case OrMarker():
                return self.or_(
                    self.when(marker.left, component),
                    self.when(marker.right, component),
                )
            case NotMarker():
                return self.not_(self.when(marker.marker, component))
            case BoolMarker(False):
                return self.global_(marker.value)
            case _:
                if component is None:
                    raise TypeError(  # noqa: TRY003
                        f"Component is None, cannot generate when condition"
                        f" with marker {marker}",
                    )
                return self.getter(DependencyKey(marker, component))

    def build_getter(self) -> CompiledFactory:
        name = f"<{self.getter_name}{'_async' if self.async_str else ''}>"
        return cast(CompiledFactory, self.compile(name)[self.getter_name])


def _sync_factory_body(
    builder: FactoryBuilder, source_call: str, factory: Factory,
) -> None:
    builder.assign_solved(source_call)


def _async_factory_body(
    builder: FactoryBuilder, source_call: str, factory: Factory,
) -> None:
    builder.assign_solved(builder.await_(source_call))


def _generator_body(
    builder: FactoryBuilder, source_call: str, factory: Factory,
) -> None:
    builder.assign_local("generator", source_call)
    builder.assign_solved(
        builder.call("next", "generator"),
    )
    builder.statement(
        builder.call(
            "exits.append",
            builder.call(
                builder.global_(Exit),
                builder.global_(factory.type, "factory_type"),
                "generator",
            ),
        ),
    )


def _async_generator_body(
    builder: FactoryBuilder, source_call: str, factory: Factory,
) -> None:
    builder.assign_local("generator", source_call)
    builder.assign_solved(
        builder.await_(builder.call("anext", "generator")),
    )
    builder.statement(
        builder.call(
            "exits.append",
            builder.call(
                builder.global_(Exit),
                builder.global_(factory.type, "factory_type"),
                "generator",
            ),
        ),
    )


def _value_factory_body(
    builder: FactoryBuilder, source_call: str, factory: Factory,
) -> None:
    builder.assign_solved(builder.global_(factory.source))


def _alias_factory_body(
    builder: FactoryBuilder, source_call: str, factory: Factory,
) -> None:
    builder.assign_solved(builder.getter(factory.dependencies[0]))


def _context_factory_body(
    builder: FactoryBuilder, source_call: str, factory: Factory,
) -> None:
    source = builder.global_(factory.source)
    with builder.try_():
        builder.assign_solved(f"context[{source}]")
    with builder.except_(KeyError):
        builder.raise_(
            builder.call(
                builder.global_(NoContextValueError),
                source,
            ),
        )

def _selector_factory_body(
    builder: FactoryBuilder, source_call: str, factory: Factory,
) -> None:
    error_call = builder.call(
        builder.global_(NoActiveFactoryError),
        builder.global_(factory.provides),
        builder.global_(factory.when_dependencies, "when_dependencies"),
    )
    builder.raise_(error_call)

def _collection_factory_body(
    builder: FactoryBuilder,
    factory: Factory,
) -> None:
    unconditional_factories: list[Factory] = []
    assigned = False
    for variant in factory.when_dependencies:
        condition = builder.when(variant.when_override, variant.when_component)
        if condition:
            if not assigned:
                builder.assign_solved(builder.list_literal(*(
                    builder.getter(f.provides)
                    for f in unconditional_factories
                )))
                assigned = True
            with builder.if_(condition):
                builder.statement(builder.call(
                    "solved.append",
                    builder.getter(variant.provides),
                ))
        elif assigned:
            builder.statement(builder.call(
                "solved.append",
                builder.getter(variant.provides),
            ))
        else:
            unconditional_factories.append(variant)
    if not assigned:
        builder.assign_solved(builder.list_literal(*(
            builder.getter(f.provides)
            for f in unconditional_factories
        )))


ASYNC_TYPES = (FactoryType.ASYNC_FACTORY, FactoryType.ASYNC_GENERATOR)
BodyGenerator: TypeAlias = Callable[[FactoryBuilder, str, Factory], None]
BODY_GENERATORS: dict[FactoryType, BodyGenerator] = {
    FactoryType.FACTORY: _sync_factory_body,
    FactoryType.ASYNC_FACTORY: _async_factory_body,
    FactoryType.GENERATOR: _generator_body,
    FactoryType.ASYNC_GENERATOR: _async_generator_body,
    FactoryType.CONTEXT: _context_factory_body,
    FactoryType.VALUE: _value_factory_body,
    FactoryType.ALIAS: _alias_factory_body,
    FactoryType.SELECTOR: _selector_factory_body,
    # special case, value not used
    FactoryType.COLLECTION: lambda _, __, ___: None,
}


def _select_when_dependency(
    builder: FactoryBuilder,
    factory: Factory,
) -> bool:
    """return True if there is assignment in any case"""
    first = True
    for variant in factory.when_dependencies:
        condition = builder.when(variant.when_override, factory.when_component)
        solved_value = builder.getter(variant.provides)
        if first and not condition:
            builder.assign_solved(solved_value)
            return True
        elif first:
            with builder.if_(condition):
                builder.assign_solved(solved_value)
        elif not condition:
            with builder.else_():
                builder.assign_solved(solved_value)
            return True
        else:
            with builder.elif_(condition):
                builder.assign_solved(solved_value)
        first = False
    return False


def compile_factory(*, factory: Factory, is_async: bool) -> CompiledFactory:
    if not is_async and factory.type in ASYNC_TYPES:
        raise UnsupportedFactoryError(factory)
    if factory.type not in BODY_GENERATORS:
        raise UnsupportedFactoryError(factory)

    builder = FactoryBuilder(is_async=is_async, getter_prefix="get_")
    builder.register_provides(factory.provides)

    with builder.make_getter():
        if factory.type is FactoryType.COLLECTION:
            _collection_factory_body(builder, factory)
        else:
            has_default = _select_when_dependency(builder, factory)
            if not has_default:
                source_call = builder.call(
                    builder.global_(factory.source),
                    *(builder.getter(dep) for dep in factory.dependencies),
                    **{
                        name: builder.getter(dep)
                        for name, dep in factory.kw_dependencies.items()
                    },
                )
                body_generator = BODY_GENERATORS[factory.type]
                if factory.when_dependencies:  # conditions generated
                    with builder.else_():
                        body_generator(builder, source_call, factory)
                else:  # no options at all
                    body_generator(builder, source_call, factory)

        if factory.cache:
            builder.cache()
        builder.return_("solved")

    return builder.build_getter()


def compile_activation(*, factory: Factory, is_async: bool) -> CompiledFactory:
    builder = FactoryBuilder(is_async=is_async, getter_prefix="is_active_")
    builder.register_provides(factory.provides)
    with builder.make_getter():
        condition = builder.when(factory.when_active, factory.when_component)
        if not condition:
            builder.return_(builder.global_(True))
        else:
            with builder.if_(condition):
                builder.return_(builder.global_(True))
            builder.return_(builder.global_(False))

    return builder.build_getter()
