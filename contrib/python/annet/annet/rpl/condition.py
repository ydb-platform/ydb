from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, Sequence, TypeVar, Union


class ConditionOperator(Enum):
    EQ = "=="
    GE = ">="
    GT = ">"
    LE = "<="
    LT = "<"
    BETWEEN_INCLUDED = "BETWEEN_INCLUDED"

    HAS = "has"
    HAS_ANY = "has_any"

    CUSTOM = "custom"


ValueT = TypeVar("ValueT")


@dataclass(frozen=True)
class SingleCondition(Generic[ValueT]):
    field: str
    operator: ConditionOperator
    value: ValueT

    def __and__(self, other: "Condition") -> "Condition":
        return AndCondition(self, other)

    def merge(self, other: "SingleCondition[Any]") -> "SingleCondition[Any]":
        if other.field != self.field:
            raise ValueError(f"Cannot merge conditions with different fields: {self.field} != {other.field}")
        if self.operator is ConditionOperator.LE:
            if other.operator is ConditionOperator.GE:
                return SingleCondition(self.field, ConditionOperator.BETWEEN_INCLUDED, (other.value, self.value))
            elif other.operator is ConditionOperator.LE:
                return SingleCondition(self.field, ConditionOperator.LE, min(self.value, other.value))
        elif self.operator is ConditionOperator.GE:
            if other.operator is ConditionOperator.LE:
                return other.merge(self)
            elif other.operator is ConditionOperator.GE:
                return SingleCondition(self.field, ConditionOperator.GE, max(self.value, other.value))
        elif self.operator is ConditionOperator.LT:
            if other.operator is ConditionOperator.LT:
                return SingleCondition(self.field, ConditionOperator.LT, min(self.value, other.value))
        elif self.operator is ConditionOperator.GT:
            if other.operator is ConditionOperator.GT:
                return SingleCondition(self.field, ConditionOperator.GT, max(self.value, other.value))
        raise ValueError(f"Cannot merge condition with operator {self.operator} and {other.operator}")


Condition = Union[SingleCondition, "AndCondition"]


class AndCondition:
    def __init__(self, *conditions: Condition):
        self.conditions: list[SingleCondition[Any]] = []
        for c in conditions:
            self.conditions.extend(self._unpack(c))

    def _unpack(self, other: Condition) -> Sequence[SingleCondition]:
        if isinstance(other, AndCondition):
            return other.conditions
        return [other]

    def __and__(self, other: Condition) -> "AndCondition":
        return AndCondition(*self.conditions, other)

    def __iadd__(self, other):
        self.conditions.extend(self._unpack(other))

    def __repr__(self):
        conditions = ", ".join(repr(c) for c in self.conditions)
        return f"AndCondition({conditions})"

    def __getitem__(self, item: str) -> SingleCondition[Any]:
        return next(c for c in self.conditions if c.field == item)

    def __contains__(self, item: str) -> bool:
        return any(c.field == item for c in self.conditions)

    def __len__(self) -> int:
        return len(self.conditions)

    def __iter__(self) -> Iterator[SingleCondition[Any]]:
        return iter(self.conditions)

    def find_all(self, item: str) -> Iterable[SingleCondition[Any]]:
        for condition in self.conditions:
            if condition.field == item:
                yield condition
