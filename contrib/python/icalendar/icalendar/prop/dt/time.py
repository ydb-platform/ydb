"""TIME property type from :rfc:`5545`."""

import re
from datetime import datetime, time, timezone
from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.timezone.tzid import is_utc

from .base import TimeBase

TIME_JCAL_REGEX = re.compile(
    r"^(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})(?P<utc>Z)?$"
)


class vTime(TimeBase):
    """Time

    Value Name:
        TIME

    Purpose:
        This value type is used to identify values that contain a
        time of day.

    Format Definition:
        This value type is defined by the following notation:

        .. code-block:: text

            time         = time-hour time-minute time-second [time-utc]

            time-hour    = 2DIGIT        ;00-23
            time-minute  = 2DIGIT        ;00-59
            time-second  = 2DIGIT        ;00-60
            ;The "60" value is used to account for positive "leap" seconds.

            time-utc     = "Z"

    Description:
        If the property permits, multiple "time" values are
        specified by a COMMA-separated list of values.  No additional
        content value encoding (i.e., BACKSLASH character encoding, see
        vText) is defined for this value type.

        The "TIME" value type is used to identify values that contain a
        time of day.  The format is based on the [ISO.8601.2004] complete
        representation, basic format for a time of day.  The text format
        consists of a two-digit, 24-hour of the day (i.e., values 00-23),
        two-digit minute in the hour (i.e., values 00-59), and two-digit
        seconds in the minute (i.e., values 00-60).  The seconds value of
        60 MUST only be used to account for positive "leap" seconds.
        Fractions of a second are not supported by this format.

        In parallel to the "DATE-TIME" definition above, the "TIME" value
        type expresses time values in three forms:

        The form of time with UTC offset MUST NOT be used.  For example,
        the following is not valid for a time value:

        .. code-block:: text

            230000-0800        ;Invalid time format

        **FORM #1 LOCAL TIME**

        The local time form is simply a time value that does not contain
        the UTC designator nor does it reference a time zone.  For
        example, 11:00 PM:

        .. code-block:: text

            230000

        Time values of this type are said to be "floating" and are not
        bound to any time zone in particular.  They are used to represent
        the same hour, minute, and second value regardless of which time
        zone is currently being observed.  For example, an event can be
        defined that indicates that an individual will be busy from 11:00
        AM to 1:00 PM every day, no matter which time zone the person is
        in.  In these cases, a local time can be specified.  The recipient
        of an iCalendar object with a property value consisting of a local
        time, without any relative time zone information, SHOULD interpret
        the value as being fixed to whatever time zone the "ATTENDEE" is
        in at any given moment.  This means that two "Attendees", may
        participate in the same event at different UTC times; floating
        time SHOULD only be used where that is reasonable behavior.

        In most cases, a fixed time is desired.  To properly communicate a
        fixed time in a property value, either UTC time or local time with
        time zone reference MUST be specified.

        The use of local time in a TIME value without the "TZID" property
        parameter is to be interpreted as floating time, regardless of the
        existence of "VTIMEZONE" calendar components in the iCalendar
        object.

        **FORM #2: UTC TIME**

        UTC time, or absolute time, is identified by a LATIN CAPITAL
        LETTER Z suffix character, the UTC designator, appended to the
        time value.  For example, the following represents 07:00 AM UTC:

        .. code-block:: text

            070000Z

        The "TZID" property parameter MUST NOT be applied to TIME
        properties whose time values are specified in UTC.

        **FORM #3: LOCAL TIME AND TIME ZONE REFERENCE**

        The local time with reference to time zone information form is
        identified by the use the "TZID" property parameter to reference
        the appropriate time zone definition.

        Example:
            The following represents 8:30 AM in New York in winter,
            five hours behind UTC, in each of the three formats:

        .. code-block:: text

            083000
            133000Z
            TZID=America/New_York:083000
    """

    default_value: ClassVar[str] = "TIME"
    params: Parameters

    def __init__(self, *args, params: dict[str, Any] | None = None):
        if len(args) == 1:
            if not isinstance(args[0], (time, datetime)):
                raise ValueError(f"Expected a datetime.time, got: {args[0]}")
            self.dt = args[0]
        else:
            self.dt = time(*args)
        self.params = Parameters(params or {})
        self.params.update_tzid_from(self.dt)

    def to_ical(self):
        value = self.dt.strftime("%H%M%S")
        if self.is_utc():
            value += "Z"
        return value

    def is_utc(self) -> bool:
        """Whether this time is UTC."""
        return self.params.is_utc() or is_utc(self.dt)

    @staticmethod
    def from_ical(ical):
        # TODO: timezone support
        try:
            timetuple = (int(ical[:2]), int(ical[2:4]), int(ical[4:6]))
            return time(*timetuple)
        except Exception as e:
            raise ValueError(f"Expected time, got: {ical}") from e

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vTime."""
        return [cls(time(12, 30))]

    from icalendar.param import VALUE

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        value = self.dt.strftime("%H:%M:%S")
        if self.is_utc():
            value += "Z"
        return [name, self.params.to_jcal(exclude_utc=True), self.VALUE.lower(), value]

    @classmethod
    def parse_jcal_value(cls, jcal: str) -> time:
        """Parse a jCal string to a :py:class:`datetime.time`.

        Raises:
            ~error.JCalParsingError: If it can't parse a time.
        """
        JCalParsingError.validate_value_type(jcal, str, cls)
        match = TIME_JCAL_REGEX.match(jcal)
        if match is None:
            raise JCalParsingError("Cannot parse time.", cls, value=jcal)
        hour = int(match.group("hour"))
        minute = int(match.group("minute"))
        second = int(match.group("second"))
        utc = bool(match.group("utc"))
        return time(hour, minute, second, tzinfo=timezone.utc if utc else None)

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
            value = cls.parse_jcal_value(jcal_property[3])
        return cls(
            value,
            params=Parameters.from_jcal_property(jcal_property),
        )


__all__ = ["vTime"]
