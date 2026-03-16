from collections.abc import Iterable
from typing import Any

from dishka.entities.component import DEFAULT_COMPONENT
from dishka.entities.scope import BaseScope
from .base_provider import BaseProvider
from .provider import Provider


def make_root_context_provider(
        providers: Iterable[BaseProvider],
        context: dict[Any, Any] | None,
        scopes: type[BaseScope],
) -> BaseProvider:
    """Automatically add missing `from_context`."""
    # in non-default component, context vars are aliases for it
    # use only those, which declared in Default component provider
    existing_context_vars = {
        var.provides.type_hint
        for provider in providers
        if provider.component == DEFAULT_COMPONENT
        for var in provider.context_vars
    }
    p = Provider()
    if not context:
        return p
    root_scope = next(iter(scopes))
    for type_hint in context:
        if type_hint not in existing_context_vars:
            p.from_context(provides=type_hint, scope=root_scope)
    return p
