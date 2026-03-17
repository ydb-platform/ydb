from typing import Any

from dishka.dependency_source import (
    Alias,
    CompositeDependencySource,
    ensure_composite,
)
from dishka.entities.component import Component
from dishka.entities.key import hint_to_dependency_key
from dishka.entities.marker import Marker
from dishka.exception_base import DishkaError
from dishka.exceptions import WhenOverrideConflictError
from .make_factory import calc_override
from .unpack_provides import unpack_alias


class InvalidMarkerAliasError(DishkaError):
    def __init__(self, source: Any, provides: Any) -> None:
        self.source = source
        self.provides = provides

    def __str__(self) -> str:
        return (
            f"Cannot make alias between {self.source!r} and {self.provides!r}."
            " When aliasing activation markers you can use"
            " instances or the same marker type."
        )


def alias(
        source: Any,
        *,
        provides: Any | None = None,
        cache: bool = True,
        component: Component | None = None,
        override: bool = False,
        when: Marker | None = None,
) -> CompositeDependencySource:
    if component is provides is None:
        raise ValueError(  # noqa: TRY003
            "Either component or provides must be set in alias",
        )
    if provides is None:
        provides = source
    elif (
        isinstance(source, type) and
        issubclass(source, Marker) and
        source is not provides
    ) or (
        isinstance(source, Marker) and not isinstance(provides, Marker)
    ):
        raise InvalidMarkerAliasError(source, provides)

    if when and override:
        raise WhenOverrideConflictError

    composite = ensure_composite(source)
    alias_instance = Alias(
        source=hint_to_dependency_key(source).with_component(component),
        provides=hint_to_dependency_key(provides),
        cache=cache,
        when_override=calc_override(when=when, override=override),
        when_active=when,
        when_component=None,
    )
    composite.dependency_sources.extend(unpack_alias(alias_instance))
    return composite
