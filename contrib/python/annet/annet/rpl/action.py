from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar


class ActionType(Enum):
    SET = "set"
    ADD = "add"
    REMOVE = "delete"

    CUSTOM = "custom"


ValueT = TypeVar("ValueT")


@dataclass
class SingleAction(Generic[ValueT]):
    field: str
    type: ActionType
    value: ValueT


class Action:
    def __init__(self) -> None:
        self.actions: list[SingleAction[Any]] = []

    def append(self, action: SingleAction[Any]) -> None:
        self.actions.append(action)

    def __repr__(self):
        actions = ", ".join(repr(c) for c in self.actions)
        return f"Action({actions})"

    def __getitem__(self, item: str) -> SingleAction[Any]:
        return next(c for c in self.actions if c.field == item)

    def __contains__(self, item: str) -> bool:
        return any(c.field == item for c in self.actions)

    def __len__(self) -> int:
        return len(self.actions)

    def __iter__(self) -> Iterator[SingleAction[Any]]:
        return iter(self.actions)

    def find_all(self, item: str) -> Iterable[SingleAction[Any]]:
        for action in self.actions:
            if action.field == item:
                yield action
