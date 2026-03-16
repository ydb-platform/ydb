__all__ = [
    "DEFAULT_COMPONENT",
    "STRICT_VALIDATION",
    "AnyOf",
    "AsyncContainer",
    "BaseScope",
    "Component",
    "Container",
    "DependencyKey",
    "FromComponent",
    "FromDishka",
    "Has",
    "Marker",
    "Provider",
    "Scope",
    "ValidationSettings",
    "WithParents",
    "activate",
    "alias",
    "collect",
    "decorate",
    "from_context",
    "make_async_container",
    "make_container",
    "new_scope",
    "provide",
    "provide_all",
]

from .async_container import AsyncContainer, make_async_container
from .container import Container, make_container
from .entities.component import DEFAULT_COMPONENT, Component
from .entities.depends_marker import FromDishka
from .entities.key import DependencyKey, FromComponent
from .entities.marker import Has, Marker
from .entities.provides_marker import AnyOf
from .entities.scope import BaseScope, Scope, new_scope
from .entities.validation_settings import STRICT_VALIDATION, ValidationSettings
from .entities.with_parents import WithParents
from .provider import (
    Provider,
    activate,
    alias,
    collect,
    decorate,
    from_context,
    provide,
    provide_all,
)
