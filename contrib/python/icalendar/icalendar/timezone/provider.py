"""The interface for timezone implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime, tzinfo

    from dateutil.rrule import rrule

    from icalendar import prop


class TZProvider(ABC):
    """Interface for timezone implementations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the implementation."""

    @abstractmethod
    def localize_utc(self, dt: datetime) -> datetime:
        """Return the datetime in UTC."""

    @abstractmethod
    def localize(self, dt: datetime, tz: tzinfo) -> datetime:
        """Localize a datetime to a timezone."""

    @abstractmethod
    def knows_timezone_id(self, tzid: str) -> bool:
        """Whether the timezone is already cached by the implementation."""

    @abstractmethod
    def fix_rrule_until(self, rrule: rrule, ical_rrule: prop.vRecur) -> None:
        """Make sure the until value works for rrules generated from the ical_rrule."""

    @abstractmethod
    def create_timezone(self, name: str, transition_times, transition_info) -> tzinfo:
        """Create a pytz timezone file given information."""

    @abstractmethod
    def timezone(self, name: str) -> tzinfo | None:
        """Return a timezone with a name or None if we cannot find it."""

    @abstractmethod
    def uses_pytz(self) -> bool:
        """Whether we use pytz."""

    @abstractmethod
    def uses_zoneinfo(self) -> bool:
        """Whether we use zoneinfo."""


__all__ = ["TZProvider"]
