from typing import Any

from dishka.dependency_source import (
    CompositeDependencySource,
    FactoryUnionMode,
)
from dishka.entities.key import hint_to_dependency_key
from dishka.entities.scope import BaseScope


def collect(
    source: Any,
    *,
    scope: BaseScope | None = None,
    cache: bool = True,
    provides: Any = None,
) -> CompositeDependencySource:
    src = CompositeDependencySource(source)
    key = hint_to_dependency_key(source)
    if provides is None:
        provides = list[key.type_hint]  # type: ignore[name-defined]
    provides_key = hint_to_dependency_key(provides)
    src.dependency_sources.append(FactoryUnionMode(
        source=key,
        collect=True,
        cache=cache,
        scope=scope,
        provides=provides_key,
    ))
    return src
