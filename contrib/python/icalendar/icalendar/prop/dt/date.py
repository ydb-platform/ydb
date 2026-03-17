"""DATE property type from :rfc:`5545`."""

from datetime import date, datetime
from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters

from .base import TimeBase


class vDate(TimeBase):
    """Date

    Value Name:
        DATE

    Purpose:
        This value type is used to identify values that contain a
        calendar date.

    Format Definition:
        This value type is defined by the following notation:

        .. code-block:: text

            date               = date-value

            date-value         = date-fullyear date-month date-mday
            date-fullyear      = 4DIGIT
            date-month         = 2DIGIT        ;01-12
            date-mday          = 2DIGIT        ;01-28, 01-29, 01-30, 01-31
                                               ;based on month/year

    Description:
        If the property permits, multiple "date" values are
        specified as a COMMA-separated list of values.  The format for the
        value type is based on the [ISO.8601.2004] complete
        representation, basic format for a calendar date.  The textual
        format specifies a four-digit year, two-digit month, and two-digit
        day of the month.  There are no separator characters between the
        year, month, and day component text.

    Example:
        The following represents July 14, 1997:

        .. code-block:: text

            19970714

        .. code-block:: pycon

            >>> from icalendar.prop import vDate
            >>> date = vDate.from_ical('19970714')
            >>> date.year
            1997
            >>> date.month
            7
            >>> date.day
            14
    """

    default_value: ClassVar[str] = "DATE"
    params: Parameters

    def __init__(self, dt, params: dict[str, Any] | None = None):
        if not isinstance(dt, date):
            raise TypeError("Value MUST be a date instance")
        self.dt = dt
        self.params = Parameters(params or {})

    def to_ical(self):
        s = f"{self.dt.year:04}{self.dt.month:02}{self.dt.day:02}"
        return s.encode("utf-8")

    @staticmethod
    def from_ical(ical):
        try:
            timetuple = (
                int(ical[:4]),  # year
                int(ical[4:6]),  # month
                int(ical[6:8]),  # day
            )
            return date(*timetuple)
        except Exception as e:
            raise ValueError(f"Wrong date format {ical}") from e

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vDate."""
        return [cls(date(2025, 11, 10))]

    from icalendar.param import VALUE

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        return [
            name,
            self.params.to_jcal(),
            self.VALUE.lower(),
            self.dt.strftime("%Y-%m-%d"),
        ]

    @classmethod
    def parse_jcal_value(cls, jcal: str) -> date:
        """Parse a jCal string to a :py:class:`datetime.date`.

        Raises:
            ~error.JCalParsingError: If it can't parse a date.
        """
        JCalParsingError.validate_value_type(jcal, str, cls)
        try:
            return datetime.strptime(jcal, "%Y-%m-%d").date()
        except ValueError as e:
            raise JCalParsingError("Cannot parse date.", cls, value=jcal) from e

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


__all__ = ["vDate"]
