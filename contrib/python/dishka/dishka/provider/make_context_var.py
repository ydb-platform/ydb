from typing import Any

from dishka.dependency_source import (
    Alias,
    CompositeDependencySource,
    ContextVariable,
    context_stub,
)
from dishka.entities.component import DEFAULT_COMPONENT
from dishka.entities.key import DependencyKey
from dishka.entities.scope import BaseScope
from dishka.entities.type_alias_type import (
    is_type_alias_type,
    unwrap_type_alias,
)
from dishka.provider.make_factory import calc_override


def from_context(
        provides: Any,
        *,
        scope: BaseScope | None = None,
        override: bool = False,
) -> CompositeDependencySource:
    composite = CompositeDependencySource(origin=context_stub)
    composite.dependency_sources.append(
        ContextVariable(
            scope=scope,
            override=override,
            provides=DependencyKey(
                type_hint=provides,
                component=DEFAULT_COMPONENT,
            ),
        ),
    )

    if is_type_alias_type(provides):
        base_type = unwrap_type_alias(provides)
        composite.dependency_sources.append(
            Alias(
                source=DependencyKey(provides, DEFAULT_COMPONENT),
                provides=DependencyKey(base_type, DEFAULT_COMPONENT),
                cache=True,
                when_override=calc_override(when=None, override=override),
                when_active=None,
                when_component=None,
            ),
        )
    return composite
