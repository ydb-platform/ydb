from __future__ import annotations

from typing import (
    Annotated,
    Any,
    Literal,
    NamedTuple,
    TypeVar,
    get_args,
    get_origin,
)

from .component import DEFAULT_COMPONENT, Component
from .marker import Marker


class _FromComponent(NamedTuple):
    component: Component


def FromComponent(  # noqa: N802
    component: Component = DEFAULT_COMPONENT,
) -> _FromComponent:
    return _FromComponent(component)


class DependencyKey(NamedTuple):
    type_hint: Any  # type hint or marker instance
    component: Component | None
    depth: int = 0  # counter to distinguish decorated/united factories

    def with_component(self, component: Component | None) -> DependencyKey:
        if self.component is not None:
            return self
        return DependencyKey(
            type_hint=self.type_hint,
            component=component,
            depth=self.depth,
        )

    def __str__(self) -> str:
        if self.depth == 0:
            return f"({self.type_hint}, component={self.component!r})"
        return (f"({self.type_hint},"
                f" component={self.component!r},"
                f" depth={self.depth})")

    def is_const(self) -> bool:
        return (
            get_origin(self.type_hint) is Literal and
            len(get_args(self.type_hint)) == 1
        )

    def is_type_var(self) -> bool:
        return isinstance(self.type_hint, TypeVar)

    def get_const_value(self) -> Any:
        return get_args(self.type_hint)[0]

    def is_marker(self) -> bool:
        return isinstance(self.type_hint, Marker) or (
            isinstance(self.type_hint, type) and
            issubclass(self.type_hint, Marker)
        )


def const_dependency_key(value: Any) -> DependencyKey:
    return DependencyKey(Literal[value], DEFAULT_COMPONENT)


def dependency_key_to_hint(key: DependencyKey) -> Any:
    if key.component is None:
        return key.type_hint
    return Annotated[key.type_hint, FromComponent(key.component)]


def hint_to_dependency_key(hint: Any) -> DependencyKey:
    if get_origin(hint) is not Annotated:
        return DependencyKey(hint, None)
    args = get_args(hint)
    from_component = next(
        (arg for arg in args if isinstance(arg, _FromComponent)),
        None,
    )
    if from_component is None:
        return DependencyKey(args[0], None)
    return DependencyKey(args[0], from_component.component)
