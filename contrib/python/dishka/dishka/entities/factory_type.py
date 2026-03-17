from enum import Enum
from typing import Any

from .key import DependencyKey
from .marker import BaseMarker
from .scope import BaseScope


class FactoryType(Enum):
    GENERATOR = "generator"
    ASYNC_GENERATOR = "async_generator"
    FACTORY = "factory"
    ASYNC_FACTORY = "async_factory"
    VALUE = "value"
    ALIAS = "alias"
    CONTEXT = "context"
    SELECTOR = "selector"
    COLLECTION = "collection"


class FactoryData:
    __slots__ = ("provides", "scope", "source", "type", "when_override")

    def __init__(
            self,
            *,
            source: Any,
            provides: DependencyKey,
            scope: BaseScope | None,
            type_: FactoryType,
            when_override: BaseMarker | None = None,
    ) -> None:
        self.source = source
        self.provides = provides
        self.scope = scope
        self.type = type_
        self.when_override = when_override
