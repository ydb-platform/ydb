"""Combined property type for the date and time properties from :rfc:`5545`."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias

from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.timezone import tzp
from icalendar.tools import is_date, is_datetime, to_datetime

from .base import TimeBase
from .date import vDate
from .datetime import vDatetime
from .duration import vDuration
from .period import vPeriod
from .time import vTime

if TYPE_CHECKING:
    from icalendar.compatibility import Self

DT_TYPE: TypeAlias = (
    datetime
    | date
    | timedelta
    | time
    | tuple[datetime, datetime]
    | tuple[datetime, timedelta]
)


class vDDDTypes(TimeBase):
    """A combined Datetime, Date or Duration parser/generator. Their format
    cannot be confused, and often values can be of either types.
    So this is practical.
    """

    default_value: ClassVar[str] = "DATE-TIME"
    params: Parameters
    dt: DT_TYPE

    def __init__(self, dt, params: dict[str, Any] | None = None):
        if params is None:
            params = {}
        if not isinstance(dt, (datetime, date, timedelta, time, tuple)):
            raise TypeError(
                "You must use datetime, date, timedelta, time or tuple (for periods)"
            )
        self.dt = dt
        if is_date(dt):
            params.update({"value": "DATE"})
        elif isinstance(dt, time):
            params.update({"value": "TIME"})
        elif isinstance(dt, tuple):
            params.update({"value": "PERIOD"})
        self.params = Parameters(params)
        self.params.update_tzid_from(dt)

    def to_property_type(self) -> vDatetime | vDate | vDuration | vTime | vPeriod:
        """Convert to a property type.

        Raises:
            ValueError: If the type is unknown.
        """
        dt = self.dt
        if isinstance(dt, datetime):
            result = vDatetime(dt)
        elif isinstance(dt, date):
            result = vDate(dt)
        elif isinstance(dt, timedelta):
            result = vDuration(dt)
        elif isinstance(dt, time):
            result = vTime(dt)
        elif isinstance(dt, tuple) and len(dt) == 2:
            result = vPeriod(dt)
        else:
            raise ValueError(f"Unknown date type: {type(dt)}")
        result.params = self.params
        return result

    def to_ical(self) -> bytes:
        """Return the ical representation."""
        return self.to_property_type().to_ical()

    @classmethod
    def from_ical(cls, ical, timezone=None):
        if isinstance(ical, cls):
            return ical.dt
        u = ical.upper()
        if u.startswith(("P", "-P", "+P")):
            return vDuration.from_ical(ical)
        if "/" in u:
            return vPeriod.from_ical(ical, timezone=timezone)

        if len(ical) in (15, 16):
            return vDatetime.from_ical(ical, timezone=timezone)
        if len(ical) == 8:
            if timezone:
                tzinfo = tzp.timezone(timezone)
                if tzinfo is not None:
                    return to_datetime(vDate.from_ical(ical)).replace(tzinfo=tzinfo)
            return vDate.from_ical(ical)
        if len(ical) in (6, 7):
            return vTime.from_ical(ical)
        raise ValueError(f"Expected datetime, date, or time. Got: '{ical}'")

    @property
    def td(self) -> timedelta:
        """Compatibility property returning ``self.dt``.

        This class is used to replace different time components.
        Some of them contain a datetime or date (``.dt``).
        Some of them contain a timedelta (``.td``).
        This property allows interoperability.
        """
        return self.dt

    @property
    def dts(self) -> list:
        """Compatibility method to return a list of datetimes."""
        return [self]

    @classmethod
    def examples(cls) -> list[vDDDTypes]:
        """Examples of vDDDTypes."""
        return [cls(date(2025, 11, 10)), cls(timedelta(days=1, hours=5))]

    def _get_value(self) -> str | None:
        """Determine the VALUE parameter."""
        return self.to_property_type().VALUE

    from icalendar.param import VALUE

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        return self.to_property_type().to_jcal(name)

    @classmethod
    def parse_jcal_value(cls, jcal: str | list) -> timedelta:
        """Parse a jCal value.

        Raises:
            ~error.JCalParsingError: If the value can't be parsed as either a date,
                 time, date-time, duration, or period.
        """
        if isinstance(jcal, list):
            return vPeriod.parse_jcal_value(jcal)
        JCalParsingError.validate_value_type(jcal, str, cls)
        if "/" in jcal:
            return vPeriod.parse_jcal_value(jcal)
        for jcal_type in (vDatetime, vDate, vTime, vDuration):
            try:
                return jcal_type.parse_jcal_value(jcal)
            except JCalParsingError:
                pass
        raise JCalParsingError(
            "Cannot parse date, time, date-time, duration, or period.", cls, value=jcal
        )

    @classmethod
    def from_jcal(cls, jcal_property: list) -> Self:
        """Parse jCal from :rfc:`7265`.

        Parameters:
            jcal_property: The jCal property to parse.

        Raises:
            ~error.JCalParsingError: If the provided jCal is invalid.
        """
        JCalParsingError.validate_property(jcal_property, cls)
        with JCalParsingError.reraise_with_path_added(3):
            dt = cls.parse_jcal_value(jcal_property[3])
        params = Parameters.from_jcal_property(jcal_property)
        if params.tzid:
            if isinstance(dt, tuple):
                # period
                start = tzp.localize(dt[0], params.tzid)
                end = tzp.localize(dt[1], params.tzid) if is_datetime(dt[1]) else dt[1]
                dt = (start, end)
            else:
                dt = tzp.localize(dt, params.tzid)
        return cls(
            dt,
            params=params,
        )


__all__ = ["DT_TYPE", "vDDDTypes"]
