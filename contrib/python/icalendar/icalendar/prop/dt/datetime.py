"""DATE-TIME property type from :rfc:`5545`."""

from datetime import datetime
from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.timezone import tzp
from icalendar.timezone.tzid import is_utc

from .base import TimeBase


class vDatetime(TimeBase):
    """Date-Time

    Value Name:
        DATE-TIME

    Purpose:
        This value type is used to identify values that specify a
        precise calendar date and time of day. The format is based on
        the ISO.8601.2004 complete representation.

    Format Definition:
        This value type is defined by the following notation:

        .. code-block:: text

            date-time  = date "T" time

            date       = date-value
            date-value         = date-fullyear date-month date-mday
            date-fullyear      = 4DIGIT
            date-month         = 2DIGIT        ;01-12
            date-mday          = 2DIGIT        ;01-28, 01-29, 01-30, 01-31
                                               ;based on month/year
            time               = time-hour time-minute time-second [time-utc]
            time-hour          = 2DIGIT        ;00-23
            time-minute        = 2DIGIT        ;00-59
            time-second        = 2DIGIT        ;00-60
            time-utc           = "Z"

        The following is the representation of the date-time format.

        .. code-block:: text

            YYYYMMDDTHHMMSS

    Description:
        vDatetime is timezone aware and uses a timezone library.
        When a vDatetime object is created from an
        ical string, you can pass a valid timezone identifier. When a
        vDatetime object is created from a Python :py:mod:`datetime` object, it uses the
        tzinfo component, if present. Otherwise a timezone-naive object is
        created. Be aware that there are certain limitations with timezone naive
        DATE-TIME components in the icalendar standard.

    Example:
        The following represents March 2, 2021 at 10:15 AM with local time:

        .. code-block:: pycon

            >>> from icalendar import vDatetime
            >>> datetime = vDatetime.from_ical("20210302T101500")
            >>> datetime.tzname()
            >>> datetime.year
            2021
            >>> datetime.minute
            15

        The following represents March 2, 2021 at 10:15 AM in New York:

        .. code-block:: pycon

            >>> datetime = vDatetime.from_ical("20210302T101500", 'America/New_York')
            >>> datetime.tzname()
            'EST'

        The following represents March 2, 2021 at 10:15 AM in Berlin:

        .. code-block:: pycon

            >>> from zoneinfo import ZoneInfo
            >>> timezone = ZoneInfo("Europe/Berlin")
            >>> vDatetime.from_ical("20210302T101500", timezone)
            datetime.datetime(2021, 3, 2, 10, 15, tzinfo=ZoneInfo(key='Europe/Berlin'))
    """

    default_value: ClassVar[str] = "DATE-TIME"
    params: Parameters

    def __init__(self, dt, /, params: dict[str, Any] | None = None):
        self.dt = dt
        self.params = Parameters(params)
        self.params.update_tzid_from(dt)

    def to_ical(self):
        dt = self.dt

        s = (
            f"{dt.year:04}{dt.month:02}{dt.day:02}"
            f"T{dt.hour:02}{dt.minute:02}{dt.second:02}"
        )
        if self.is_utc():
            s += "Z"
        return s.encode("utf-8")

    @staticmethod
    def from_ical(ical, timezone=None):
        """Create a datetime from the RFC string."""
        tzinfo = None
        if isinstance(timezone, str):
            tzinfo = tzp.timezone(timezone)
        elif timezone is not None:
            tzinfo = timezone

        try:
            timetuple = (
                int(ical[:4]),  # year
                int(ical[4:6]),  # month
                int(ical[6:8]),  # day
                int(ical[9:11]),  # hour
                int(ical[11:13]),  # minute
                int(ical[13:15]),  # second
            )
            if tzinfo:
                return tzp.localize(datetime(*timetuple), tzinfo)
            if not ical[15:]:
                return datetime(*timetuple)
            if ical[15:16] == "Z":
                return tzp.localize_utc(datetime(*timetuple))
        except Exception as e:
            raise ValueError(f"Wrong datetime format: {ical}") from e
        raise ValueError(f"Wrong datetime format: {ical}")

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vDatetime."""
        return [cls(datetime(2025, 11, 10, 16, 52))]

    from icalendar.param import VALUE

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        value = self.dt.strftime("%Y-%m-%dT%H:%M:%S")
        if self.is_utc():
            value += "Z"
        return [name, self.params.to_jcal(exclude_utc=True), self.VALUE.lower(), value]

    def is_utc(self) -> bool:
        """Whether this datetime is UTC."""
        return self.params.is_utc() or is_utc(self.dt)

    @classmethod
    def parse_jcal_value(cls, jcal: str) -> datetime:
        """Parse a jCal string to a :py:class:`datetime.datetime`.

        Raises:
            ~error.JCalParsingError: If it can't parse a date-time value.
        """
        JCalParsingError.validate_value_type(jcal, str, cls)
        utc = jcal.endswith("Z")
        if utc:
            jcal = jcal[:-1]
        try:
            dt = datetime.strptime(jcal, "%Y-%m-%dT%H:%M:%S")
        except ValueError as e:
            raise JCalParsingError("Cannot parse date-time.", cls, value=jcal) from e
        if utc:
            return tzp.localize_utc(dt)
        return dt

    @classmethod
    def from_jcal(cls, jcal_property: list) -> Self:
        """Parse jCal from :rfc:`7265`.

        Parameters:
            jcal_property: The jCal property to parse.

        Raises:
            ~error.JCalParsingError: If the provided jCal is invalid.
        """
        JCalParsingError.validate_property(jcal_property, cls)
        params = Parameters.from_jcal_property(jcal_property)
        with JCalParsingError.reraise_with_path_added(3):
            dt = cls.parse_jcal_value(jcal_property[3])
        if params.tzid:
            dt = tzp.localize(dt, params.tzid)
        return cls(
            dt,
            params=params,
        )


__all__ = ["vDatetime"]
