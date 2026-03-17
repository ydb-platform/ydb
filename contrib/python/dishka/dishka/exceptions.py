from collections.abc import Sequence

from dishka.dependency_source.factory import Factory
from dishka.entities.factory_type import FactoryData
from dishka.entities.marker import Marker
from dishka.exception_base import DishkaError
from dishka.text_rendering import get_name
from dishka.text_rendering.path import PathRenderer
from dishka.text_rendering.suggestion import render_suggestions_for_missing
from .entities.key import DependencyKey
from .entities.scope import BaseScope
from .text_rendering.name import get_source_name

try:
    from builtins import (  # type: ignore[attr-defined, unused-ignore]
        ExceptionGroup,
    )

except ImportError:
    from exceptiongroup import (  # type: ignore[no-redef, import-not-found, unused-ignore]
        ExceptionGroup,
    )

_cycle_renderer = PathRenderer(cycle=True)
_linear_renderer = PathRenderer(cycle=False)


class ExitError(
    ExceptionGroup[Exception],  # type: ignore[misc, unused-ignore]
    DishkaError,
):
    pass


class NoContextValueError(DishkaError):
    pass


class UnsupportedFactoryError(DishkaError):
    def __init__(self, factory_type: FactoryData) -> None:
        self.factory_type = factory_type

    def __str__(self) -> str:
        return f"Unsupported factory type {self.factory_type}."


class InvalidGraphError(DishkaError):
    pass


class InvalidSubfactoryScopeError(InvalidGraphError):
    def __init__(self, factory: Factory, subfactory: Factory) -> None:
        self.factory = factory
        self.subfactory = subfactory

    def __str__(self) -> str:
        name = get_source_name(self.factory)
        sub_name = get_source_name(self.subfactory)
        return (
            f"`{name}` with scope {self.factory.scope} cannot use "
            f"`{sub_name}` with scope {self.subfactory.scope}"
        )

class NoActivatorError(InvalidGraphError):
    def __init__(
        self,
        marker_key: DependencyKey,
        requesting_factories: Sequence[Factory] = (),
    ) -> None:
        self.marker_key = marker_key
        self.requesting_factories = requesting_factories

    def _get_when_expression(self, factory: Factory) -> str:
        when = factory.when_active or factory.when_override
        return repr(when) if when else ""

    def __str__(self) -> str:
        msg = (
            f"Cannot find activator for {self.marker_key.type_hint}"
            f" at component {self.marker_key.component!r}."
        )
        if self.requesting_factories:
            msg += "\nUsed in:"
            for factory in self.requesting_factories:
                source = get_source_name(factory)
                when_expr = self._get_when_expression(factory)
                msg += f"\n  - {source}: {when_expr}"
        return msg


class ActivatorOverrideError(DishkaError):
    def __init__(
        self,
        marker: Marker | type[Marker],
        activators: Sequence[FactoryData],
    ) -> None:
        self.marker = marker
        self.activators = activators

    def __str__(self) -> str:
        return (
            f"Multiple activators found for {self.marker}: "
            f"{', '.join(map(get_source_name, self.activators))}"
        )


class WhenOverrideConflictError(DishkaError):
    def __str__(self) -> str:
        return "Cannot have both `when` and `override` set. "


class NoFactoryError(DishkaError):
    def __init__(
        self,
        requested: DependencyKey,
        path: Sequence[FactoryData] = (),
        suggest_other_scopes: Sequence[FactoryData] = (),
        suggest_other_components: Sequence[FactoryData] = (),
        suggest_abstract_factories: Sequence[FactoryData] = (),
        suggest_concrete_factories: Sequence[FactoryData] = (),
    ) -> None:
        self.requested = requested
        self.path = list(path)
        self.suggest_other_scopes = suggest_other_scopes
        self.suggest_other_components = suggest_other_components
        self.suggest_abstract_factories = list(
            suggest_abstract_factories,
        )
        self.suggest_concrete_factories = list(
            suggest_concrete_factories,
        )
        self.scope: BaseScope | None = None

    def add_path(self, requested_by: FactoryData) -> None:
        self.path.insert(0, requested_by)

    def __str__(self) -> str:
        suggestion = render_suggestions_for_missing(
            requested_for=self.path[-1] if self.path else None,
            requested_key=self.requested,
            suggest_other_scopes=self.suggest_other_scopes,
            suggest_other_components=self.suggest_other_components,
            suggest_abstract_factories=self.suggest_abstract_factories,
            suggest_concrete_factories=self.suggest_concrete_factories,
        )
        if suggestion:
            suggestion = f" Hint:{suggestion}"
        requested_name = get_name(
            self.requested.type_hint, include_module=False,
        )
        if self.path:
            requested_repr = (
                f"({requested_name}, "
                f"component={self.requested.component!r})"
            )
            return (
                f"Cannot find factory for {requested_repr}. "
                f"It is missing or has invalid scope.\n"
            ) + _linear_renderer.render(self.path, self.requested) + suggestion
        else:
            requested_repr = (
                f"({requested_name}, "
                f"component={self.requested.component!r}, "
                f"scope={self.scope!s})"
            )
            return (
                f"Cannot find factory for {requested_repr}. "
                f"Check scopes in your providers. "
                f"It is missing or has invalid scope."
            ) + suggestion


class NoActiveFactoryError(DishkaError):
    def __init__(
        self,
        requested: DependencyKey,
        variants: Sequence[FactoryData],
        path: Sequence[FactoryData] = (),
    ) -> None:
        self.requested = requested
        self.variants = variants
        self.path = list(path)
        self.scope: BaseScope | None = None

    def add_path(self, requested_by: FactoryData) -> None:
        self.path.insert(0, requested_by)

    def __str__(self) -> str:
        requested_name = get_name(
            self.requested.type_hint, include_module=False,
        )
        requested_repr = (
            f"({requested_name}, "
            f"component={self.requested.component!r})"
        )
        return (
            f"Cannot select active factory for {requested_repr}. "
            f"All variants are not active.\n"
        ) + _linear_renderer.render(self.path, None, self.variants)


class AliasedFactoryNotFoundError(ValueError, DishkaError):
    def __init__(
            self, dependency: DependencyKey, alias: FactoryData,
    ) -> None:
        self.dependency = dependency
        self.alias_provider = alias.provides

    def __str__(self) -> str:
        return (
            f"Factory for {self.dependency} "
            f"aliased from {self.alias_provider} is not found"
        )


class NoChildScopesError(ValueError, DishkaError):
    def __str__(self) -> str:
        return "No child scopes found"


class NoNonSkippedScopesError(ValueError, DishkaError):
    def __str__(self) -> str:
        return "No non-skipped scopes found."


class ChildScopeNotFoundError(ValueError, DishkaError):
    def __init__(
            self,
            assumed_child_scope: BaseScope | None,
            current_scope: BaseScope | None,
    ) -> None:
        self.child_scope = assumed_child_scope
        self.current_scope = current_scope

    def __str__(self) -> str:
        return (
            f"Cannot find {self.child_scope} as a "
            f"child of current {self.current_scope}"
        )


class UnknownScopeError(InvalidGraphError):
    def __init__(
            self,
            scope: BaseScope | None,
            expected: type[BaseScope],
            factory: FactoryData,
            extend_message: str = "",
    ) -> None:
        self.scope = scope
        self.expected = expected
        self.factory = factory
        self.extend_message = extend_message

    def __str__(self) -> str:
        name = get_name(self.factory.source, include_module=False)
        return " ".join((
            f"Scope {self.scope} at `{name}` is unknown, "
            f"expected one of {self.expected}.",
            self.extend_message,
        ))


class CycleDependenciesError(InvalidGraphError):
    def __init__(self, path: Sequence[FactoryData]) -> None:
        self.path = path

    def __str__(self) -> str:
        if len(self.path) == 1:
            hint = " Did you mean @decorate instead of @provide?"
        else:
            hint = ""
        details = _cycle_renderer.render(self.path)
        return f"Cycle dependencies detected.{hint}\n{details}"


class GraphMissingFactoryError(NoFactoryError, InvalidGraphError):
    pass


class ImplicitOverrideDetectedError(InvalidGraphError):
    def __init__(self, new: FactoryData, existing: FactoryData) -> None:
        self.new = new
        self.existing = existing

    def __str__(self) -> str:
        new_name = get_name(self.new.source, include_module=False)
        existing_name = get_name(self.existing.source, include_module=False)
        return (
            f"Detected multiple factories for {self.new.provides} "
            f"while `override` flag is not set.\n"
            "Hint:\n"
            f" * Try specifying `override=True` for {new_name}\n"
            f" * Try removing factory {existing_name} or {new_name}\n"
        )


class NothingOverriddenError(InvalidGraphError):
    def __init__(self, factory: FactoryData) -> None:
        self.factory = factory

    def __str__(self) -> str:
        name = get_name(self.factory.source, include_module=False)
        return (
            f"Overriding factory found for {self.factory.provides}, "
            "but there is nothing to override.\n"
            "Hint:\n"
            f" * Try removing override=True from {name}\n"
            f" * Check the order of providers\n"
        )
