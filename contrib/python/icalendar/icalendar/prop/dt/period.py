"""PERIOD property type from :rfc:`5545`."""

from datetime import date, datetime, timedelta
from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.timezone import tzp
from icalendar.tools import is_datetime, normalize_pytz

from .base import TimeBase
from .datetime import vDatetime
from .duration import vDuration


class vPeriod(TimeBase):
    """Period of Time

    Value Name:
        PERIOD

    Purpose:
        This value type is used to identify values that contain a
        precise period of time.

    Format Definition:
        This value type is defined by the following notation:

        .. code-block:: text

            period     = period-explicit / period-start

           period-explicit = date-time "/" date-time
           ; [ISO.8601.2004] complete representation basic format for a
           ; period of time consisting of a start and end.  The start MUST
           ; be before the end.

           period-start = date-time "/" dur-value
           ; [ISO.8601.2004] complete representation basic format for a
           ; period of time consisting of a start and positive duration
           ; of time.

    Description:
        If the property permits, multiple "period" values are
        specified by a COMMA-separated list of values.  There are two
        forms of a period of time.  First, a period of time is identified
        by its start and its end.  This format is based on the
        [ISO.8601.2004] complete representation, basic format for "DATE-
        TIME" start of the period, followed by a SOLIDUS character
        followed by the "DATE-TIME" of the end of the period.  The start
        of the period MUST be before the end of the period.  Second, a
        period of time can also be defined by a start and a positive
        duration of time.  The format is based on the [ISO.8601.2004]
        complete representation, basic format for the "DATE-TIME" start of
        the period, followed by a SOLIDUS character, followed by the
        [ISO.8601.2004] basic format for "DURATION" of the period.

    Example:
        The period starting at 18:00:00 UTC, on January 1, 1997 and
        ending at 07:00:00 UTC on January 2, 1997 would be:

        .. code-block:: text

            19970101T180000Z/19970102T070000Z

        The period start at 18:00:00 on January 1, 1997 and lasting 5 hours
        and 30 minutes would be:

        .. code-block:: text

            19970101T180000Z/PT5H30M

        .. code-block:: pycon

            >>> from icalendar.prop import vPeriod
            >>> period = vPeriod.from_ical('19970101T180000Z/19970102T070000Z')
            >>> period = vPeriod.from_ical('19970101T180000Z/PT5H30M')
    """

    default_value: ClassVar[str] = "PERIOD"
    params: Parameters
    by_duration: bool
    start: datetime
    end: datetime
    duration: timedelta

    def __init__(
        self,
        per: tuple[datetime, datetime | timedelta],
        params: dict[str, Any] | None = None,
    ):
        start, end_or_duration = per
        if not (isinstance(start, (datetime, date))):
            raise TypeError("Start value MUST be a datetime or date instance")
        if not (isinstance(end_or_duration, (datetime, date, timedelta))):
            raise TypeError(
                "end_or_duration MUST be a datetime, date or timedelta instance"
            )
        by_duration = isinstance(end_or_duration, timedelta)
        if by_duration:
            duration = end_or_duration
            end = normalize_pytz(start + duration)
        else:
            end = end_or_duration
            duration = normalize_pytz(end - start)
        if start > end:
            raise ValueError("Start time is greater than end time")

        self.params = Parameters(params or {"value": "PERIOD"})
        # set the timezone identifier
        # does not support different timezones for start and end
        self.params.update_tzid_from(start)

        self.start = start
        self.end = end
        self.by_duration = by_duration
        self.duration = duration

    def overlaps(self, other):
        if self.start > other.start:
            return other.overlaps(self)
        return self.start <= other.start < self.end

    def to_ical(self):
        if self.by_duration:
            return (
                vDatetime(self.start).to_ical()
                + b"/"
                + vDuration(self.duration).to_ical()
            )
        return vDatetime(self.start).to_ical() + b"/" + vDatetime(self.end).to_ical()

    @staticmethod
    def from_ical(ical, timezone=None):
        from icalendar.prop.dt.types import vDDDTypes

        try:
            start, end_or_duration = ical.split("/")
            start = vDDDTypes.from_ical(start, timezone=timezone)
            end_or_duration = vDDDTypes.from_ical(end_or_duration, timezone=timezone)
        except Exception as e:
            raise ValueError(f"Expected period format, got: {ical}") from e
        return (start, end_or_duration)

    def __repr__(self):
        p = (self.start, self.duration) if self.by_duration else (self.start, self.end)
        return f"vPeriod({p!r})"

    @property
    def dt(self):
        """Make this cooperate with the other vDDDTypes."""
        return (self.start, (self.duration if self.by_duration else self.end))

    from icalendar.param import FBTYPE

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vPeriod."""
        return [
            vPeriod((datetime(2025, 11, 10, 16, 35), timedelta(hours=1, minutes=30))),
            vPeriod((datetime(2025, 11, 10, 16, 35), datetime(2025, 11, 10, 18, 5))),
        ]

    from icalendar.param import VALUE

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        value = [vDatetime(self.start).to_jcal(name)[-1]]
        if self.by_duration:
            value.append(vDuration(self.duration).to_jcal(name)[-1])
        else:
            value.append(vDatetime(self.end).to_jcal(name)[-1])
        return [name, self.params.to_jcal(exclude_utc=True), self.VALUE.lower(), value]

    @classmethod
    def parse_jcal_value(
        cls, jcal: str | list
    ) -> tuple[datetime, datetime] | tuple[datetime, timedelta]:
        """Parse a jCal value.

        Raises:
            ~error.JCalParsingError: If the period is not a list with exactly two items,
                or it can't parse a date-time or duration.
        """
        if isinstance(jcal, str) and "/" in jcal:
            # only occurs in the example of RFC7265, Section B.2.2.
            jcal = jcal.split("/")
        if not isinstance(jcal, list) or len(jcal) != 2:
            raise JCalParsingError(
                "A period must be a list with exactly 2 items.", cls, value=jcal
            )
        with JCalParsingError.reraise_with_path_added(0):
            start = vDatetime.parse_jcal_value(jcal[0])
        with JCalParsingError.reraise_with_path_added(1):
            JCalParsingError.validate_value_type(jcal[1], str, cls)
            if jcal[1].startswith(("P", "-P", "+P")):
                end_or_duration = vDuration.parse_jcal_value(jcal[1])
            else:
                try:
                    end_or_duration = vDatetime.parse_jcal_value(jcal[1])
                except JCalParsingError as e:
                    raise JCalParsingError(
                        "Cannot parse date-time or duration.",
                        cls,
                        value=jcal[1],
                    ) from e
        return start, end_or_duration

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
            start, end_or_duration = cls.parse_jcal_value(jcal_property[3])
        params = Parameters.from_jcal_property(jcal_property)
        tzid = params.tzid

        if tzid:
            start = tzp.localize(start, tzid)
            if is_datetime(end_or_duration):
                end_or_duration = tzp.localize(end_or_duration, tzid)

        return cls((start, end_or_duration), params=params)


__all__ = ["vPeriod"]
