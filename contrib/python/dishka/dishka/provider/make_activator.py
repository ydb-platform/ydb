from collections.abc import Callable
from typing import Any, overload

from dishka.dependency_source import (
    Activator,
    CompositeDependencySource,
    ensure_composite,
)
from dishka.entities.marker import Marker
from dishka.exception_base import DishkaError
from .make_factory import make_factory


class NoMarkerError(DishkaError):
    def __str__(self) -> str:
        return "At least one marker must be specified"


def _activate(
    source: Callable[..., Any],
    *markers: Marker | type[Marker],
    is_in_class: bool = True,
) -> CompositeDependencySource:
    if not markers:
        raise NoMarkerError

    composite = ensure_composite(source)

    factory = make_factory(
        provides=bool,
        scope=None,
        source=source,
        cache=True,
        is_in_class=is_in_class,
        override=False,
        when=None,
    )
    if factory.provides.type_hint is not bool:
        raise ValueError("Activator must return bool")  # noqa: TRY003

    for marker in markers:
        if isinstance(marker, type):
            composite.dependency_sources.append(Activator(
                factory=factory,
                marker=None,
                marker_type=marker,
            ))
        else:
            composite.dependency_sources.append(Activator(
                factory=factory,
                marker=marker,
                marker_type=type(marker),
            ))
    return composite


@overload
def activate(
    source: Marker | type[Marker] | None = None,
    *markers: Marker | type[Marker],
) -> Callable[
    [Callable[..., Any]], CompositeDependencySource,
]:
    ...


@overload
def activate(
    source: Callable[..., Any],
    *markers: Marker | type[Marker],
) -> CompositeDependencySource:
    ...


def activate(
    source: Callable[..., Any] | type[Marker] | Marker | None = None,
    *markers: Marker | type[Marker],
) -> CompositeDependencySource | Callable[
    [Callable[..., Any]], CompositeDependencySource,
]:
    """
    Register an activation function for one or more markers.

    It determines whether a dependency with a specific marker
    should be used or not.

    The function should return bool.

    Activators can depend on other dependencies and will be called during
    graph compilation or resolution depending on whether they are static
    or dynamic.
    """
    if (
        isinstance(source, Marker) or
        (isinstance(source, type) and issubclass(source, Marker))
    ):
        def decorator(func: Callable[..., Any]) -> CompositeDependencySource:
            return _activate(func, source, *markers, is_in_class=True)
        return decorator
    if source is None:
        raise NoMarkerError
    return _activate(source, *markers, is_in_class=True)


def activate_on_instance(
    source: Callable[..., Any],
    *markers: Marker | type,
) -> CompositeDependencySource:
    """Register an activation function on a provider instance."""
    return _activate(source, *markers, is_in_class=False)
