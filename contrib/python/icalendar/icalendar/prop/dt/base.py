from datetime import date
from typing import ClassVar

from icalendar.parser import Parameters


class TimeBase:
    """Make classes with a datetime/date comparable."""

    default_value: ClassVar[str]
    params: Parameters
    ignore_for_equality = {"TZID", "VALUE"}

    def __eq__(self, other):
        """self == other"""
        if isinstance(other, date):
            return self.dt == other
        if isinstance(other, TimeBase):
            default = object()
            for key in (
                set(self.params) | set(other.params)
            ) - self.ignore_for_equality:
                if key[:2].lower() != "x-" and self.params.get(
                    key, default
                ) != other.params.get(key, default):
                    return False
            return self.dt == other.dt
        from .list import vDDDLists

        if isinstance(other, vDDDLists):
            return other == self
        return False

    def __hash__(self):
        return hash(self.dt)

    from icalendar.param import RANGE, RELATED, TZID

    def __repr__(self):
        """String representation."""
        return f"{self.__class__.__name__}({self.dt}, {self.params})"


__all__ = ["TimeBase"]
