from dishka.dependency_source import (
    Activator,
    Alias,
    ContextVariable,
    Decorator,
    Factory,
)
from dishka.dependency_source.factory_union_mode import FactoryUnionMode
from dishka.entities.component import Component


class BaseProvider:
    def __init__(self, component: Component | None) -> None:
        if component is not None:
            self.component = component
        self.factories: list[Factory] = []
        self.aliases: list[Alias] = []
        self.decorators: list[Decorator] = []
        self.context_vars: list[ContextVariable] = []
        self.activators: list[Activator] = []
        self.factory_union_mode: list[FactoryUnionMode] = []


class ProviderWrapper(BaseProvider):
    def __init__(self, component: Component, provider: BaseProvider) -> None:
        super().__init__(component)
        self.factories.extend(provider.factories)
        self.aliases.extend(provider.aliases)
        self.decorators.extend(provider.decorators)
        self.activators.extend(provider.activators)
        self.factory_union_mode.extend(provider.factory_union_mode)
