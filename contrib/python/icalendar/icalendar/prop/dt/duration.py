"""DURATION property type from :rfc:`5545`."""

import re
from datetime import timedelta
from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import InvalidCalendar, JCalParsingError
from icalendar.parser import Parameters

from .base import TimeBase

DURATION_REGEX = re.compile(
    r"([-+]?)P(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$"
)


class vDuration(TimeBase):
    """Duration

    Value Name:
        DURATION

    Purpose:
        This value type is used to identify properties that contain
        a duration of time.

    Format Definition:
        This value type is defined by the following notation:

        .. code-block:: text

            dur-value  = (["+"] / "-") "P" (dur-date / dur-time / dur-week)

            dur-date   = dur-day [dur-time]
            dur-time   = "T" (dur-hour / dur-minute / dur-second)
            dur-week   = 1*DIGIT "W"
            dur-hour   = 1*DIGIT "H" [dur-minute]
            dur-minute = 1*DIGIT "M" [dur-second]
            dur-second = 1*DIGIT "S"
            dur-day    = 1*DIGIT "D"

    Description:
        If the property permits, multiple "duration" values are
        specified by a COMMA-separated list of values.  The format is
        based on the [ISO.8601.2004] complete representation basic format
        with designators for the duration of time.  The format can
        represent nominal durations (weeks and days) and accurate
        durations (hours, minutes, and seconds).  Note that unlike
        [ISO.8601.2004], this value type doesn't support the "Y" and "M"
        designators to specify durations in terms of years and months.
        The duration of a week or a day depends on its position in the
        calendar.  In the case of discontinuities in the time scale, such
        as the change from standard time to daylight time and back, the
        computation of the exact duration requires the subtraction or
        addition of the change of duration of the discontinuity.  Leap
        seconds MUST NOT be considered when computing an exact duration.
        When computing an exact duration, the greatest order time
        components MUST be added first, that is, the number of days MUST
        be added first, followed by the number of hours, number of
        minutes, and number of seconds.

    Example:
        A duration of 15 days, 5 hours, and 20 seconds would be:

        .. code-block:: text

            P15DT5H0M20S

        A duration of 7 weeks would be:

        .. code-block:: text

            P7W

        .. code-block:: pycon

            >>> from icalendar.prop import vDuration
            >>> duration = vDuration.from_ical('P15DT5H0M20S')
            >>> duration
            datetime.timedelta(days=15, seconds=18020)
            >>> duration = vDuration.from_ical('P7W')
            >>> duration
            datetime.timedelta(days=49)
    """

    default_value: ClassVar[str] = "DURATION"
    params: Parameters

    def __init__(self, td: timedelta | str, /, params: dict[str, Any] | None = None):
        if isinstance(td, str):
            td = vDuration.from_ical(td)
        if not isinstance(td, timedelta):
            raise TypeError("Value MUST be a timedelta instance")
        self.td = td
        self.params = Parameters(params)

    def to_ical(self):
        sign = ""
        td = self.td
        if td.days < 0:
            sign = "-"
            td = -td
        timepart = ""
        if td.seconds:
            timepart = "T"
            hours = td.seconds // 3600
            minutes = td.seconds % 3600 // 60
            seconds = td.seconds % 60
            if hours:
                timepart += f"{hours}H"
            if minutes or (hours and seconds):
                timepart += f"{minutes}M"
            if seconds:
                timepart += f"{seconds}S"
        if td.days == 0 and timepart:
            return str(sign).encode("utf-8") + b"P" + str(timepart).encode("utf-8")
        return (
            str(sign).encode("utf-8")
            + b"P"
            + str(abs(td.days)).encode("utf-8")
            + b"D"
            + str(timepart).encode("utf-8")
        )

    @staticmethod
    def from_ical(ical):
        match = DURATION_REGEX.match(ical)
        if not match:
            raise InvalidCalendar(f"Invalid iCalendar duration: {ical}")

        sign, weeks, days, hours, minutes, seconds = match.groups()
        value = timedelta(
            weeks=int(weeks or 0),
            days=int(days or 0),
            hours=int(hours or 0),
            minutes=int(minutes or 0),
            seconds=int(seconds or 0),
        )

        if sign == "-":
            value = -value

        return value

    @property
    def dt(self) -> timedelta:
        """The time delta for compatibility."""
        return self.td

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vDuration."""
        return [cls(timedelta(1, 99))]

    from icalendar.param import VALUE

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        return [
            name,
            self.params.to_jcal(),
            self.VALUE.lower(),
            self.to_ical().decode(),
        ]

    @classmethod
    def parse_jcal_value(cls, jcal: str) -> timedelta:
        """Parse a jCal string to a :py:class:`datetime.timedelta`.

        Raises:
            ~error.JCalParsingError: If it can't parse a duration."""
        JCalParsingError.validate_value_type(jcal, str, cls)
        try:
            return cls.from_ical(jcal)
        except (ValueError, InvalidCalendar) as e:
            raise JCalParsingError("Cannot parse duration.", cls, value=jcal) from e

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
            duration = cls.parse_jcal_value(jcal_property[3])
        return cls(
            duration,
            Parameters.from_jcal_property(jcal_property),
        )


__all__ = ["vDuration"]
