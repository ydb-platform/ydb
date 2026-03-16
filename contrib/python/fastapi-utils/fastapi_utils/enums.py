from __future__ import annotations

from enum import Enum
from typing import List

from .camelcase import snake2camel


class StrEnum(str, Enum):
    """
    StrEnum subclasses that create variants using `auto()` will have values equal to their names
    Enums inheriting from this class that set values using `enum.auto()` will have variant values equal to their names
    """

    @staticmethod
    def _generate_next_value_(name: str, start: int, count: int, last_values: List[str]) -> str:
        """
        Uses the name as the automatic value, rather than an integer

        See https://docs.python.org/3/library/enum.html#using-automatic-values for reference
        """
        return name

    def __format__(self, format_spec):
        return self.value


class CamelStrEnum(str, Enum):
    """
    CamelStrEnum subclasses that create variants using `auto()` will have values equal to their camelCase names
    """

    @staticmethod
    def _generate_next_value_(name: str, start: int, count: int, last_values: List[str]) -> str:
        """
        Uses the camelCase name as the automatic value, rather than an integer

        See https://docs.python.org/3/library/enum.html#using-automatic-values for reference
        """
        return snake2camel(name, start_lower=True)

    def __format__(self, format_spec):
        return self.value
