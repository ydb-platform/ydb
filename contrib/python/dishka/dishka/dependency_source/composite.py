from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

from .activator import Activator
from .alias import Alias
from .context_var import ContextVariable
from .decorator import Decorator
from .factory import Factory
from .factory_union_mode import FactoryUnionMode

DependencySource = (
    Alias |
    Factory |
    Decorator |
    ContextVariable |
    Activator |
    FactoryUnionMode
)


class CompositeDependencySource:
    _instances: ClassVar[int] = 0

    def __init__(
            self,
            origin: Any,
            dependency_sources: Sequence[DependencySource] = (),
            number: int | None = None,
    ) -> None:
        self.dependency_sources = list(dependency_sources)
        self.origin = origin
        if number is None:
            self.number = self._instances
            CompositeDependencySource._instances += 1
        else:
            self.number = number

    def __get__(self, instance: Any, owner: Any) -> CompositeDependencySource:
        try:
            origin = self.origin.__get__(instance, owner)
        except AttributeError:  # not a valid descriptor
            origin = self.origin
        return CompositeDependencySource(
            origin=origin,
            dependency_sources=[
                s.__get__(instance, owner) for s in self.dependency_sources
            ],
            number=self.number,
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.origin(*args, **kwargs)

    def __add__(
            self, other: CompositeDependencySource,
    ) -> CompositeDependencySource:
        return CompositeDependencySource(
            origin=None,
            dependency_sources=(
                self.dependency_sources
                + other.dependency_sources
            ),
        )


def ensure_composite(origin: Any) -> CompositeDependencySource:
    if isinstance(origin, CompositeDependencySource):
        return origin
    else:
        return CompositeDependencySource(origin)
