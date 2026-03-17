import enum
from typing import List, Tuple


Diff = List[Tuple]


# Операции отмечающие роль команды в дифе
# XXX надо отдельно переделать на enum
class Op:
    ADDED = "added"
    REMOVED = "removed"
    AFFECTED = "affected"
    MOVED = "moved"
    UNCHANGED = "unchanged"


class GeneratorType(enum.Enum):
    PARTIAL = "partial"
    ENTIRE = "entire"
    JSON_FRAGMENT = "json_fragment"

    @staticmethod
    def fromstring(value: str) -> "GeneratorType":
        return GeneratorType(value)

    def tostring(self) -> str:
        return self.value

    def __lt__(self, other: "GeneratorType") -> bool:
        return self.value < other.value

    def __le__(self, other: "GeneratorType") -> bool:
        if self != other:
            return self.value < other.value
        return True
