from dataclasses import dataclass
from enum import Enum
from itertools import count
from typing import Any


@dataclass(slots=True, frozen=True)
class _ScopeValue:
    name: str
    skip: bool
    order: int


global_order_counter = count()


def new_scope(value: str, *, skip: bool = False) -> _ScopeValue:
    return _ScopeValue(value, skip, next(global_order_counter))


class BaseScope(Enum):
    __slots__ = ("name", "order", "skip")

    def __init__(self, value: _ScopeValue) -> None:
        self.name = value.name  # type: ignore[misc]
        self.skip = value.skip
        self.order = value.order

    def __lt__(self, other: "BaseScope") -> bool:
        return self.order < other.order

    def __gt__(self, other: "BaseScope") -> bool:
        return self.order > other.order

    def __le__(self, other: "BaseScope") -> bool:
        return self.order <= other.order

    def __ge__(self, other: "BaseScope") -> bool:
        return self.order >= other.order


class Scope(BaseScope):
    RUNTIME = new_scope("RUNTIME", skip=True)
    APP = new_scope("APP")
    SESSION = new_scope("SESSION", skip=True)
    REQUEST = new_scope("REQUEST")
    ACTION = new_scope("ACTION")
    STEP = new_scope("STEP")


class InvalidScopes(BaseScope):
    UNKNOWN_SCOPE = new_scope("<unknown scope>", skip=True)

    def __str__(self) -> Any:
        return str(self.value.name)
