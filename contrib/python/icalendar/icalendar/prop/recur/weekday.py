"""BYWEEKDAY, BYDAY, and WKST value type of RECUR from :rfc:`5545`."""

import re
from typing import Any

from icalendar.caselessdict import CaselessDict
from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.parser_tools import DEFAULT_ENCODING, to_unicode

WEEKDAY_RULE = re.compile(
    r"(?P<signal>[+-]?)(?P<relative>[\d]{0,2})(?P<weekday>[\w]{2})$"
)


class vWeekday(str):
    """Either a ``weekday`` or a ``weekdaynum``.

    .. code-block:: pycon

        >>> from icalendar import vWeekday
        >>> vWeekday("MO") # Simple weekday
        'MO'
        >>> vWeekday("2FR").relative # Second friday
        2
        >>> vWeekday("2FR").weekday
        'FR'
        >>> vWeekday("-1SU").relative # Last Sunday
        -1

    Definition from :rfc:`5545#section-3.3.10`:

    .. code-block:: text

        weekdaynum = [[plus / minus] ordwk] weekday
        plus        = "+"
        minus       = "-"
        ordwk       = 1*2DIGIT       ;1 to 53
        weekday     = "SU" / "MO" / "TU" / "WE" / "TH" / "FR" / "SA"
        ;Corresponding to SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY,
        ;FRIDAY, and SATURDAY days of the week.

    """

    params: Parameters
    __slots__ = ("params", "relative", "weekday")

    week_days = CaselessDict(
        {
            "SU": 0,
            "MO": 1,
            "TU": 2,
            "WE": 3,
            "TH": 4,
            "FR": 5,
            "SA": 6,
        }
    )

    def __new__(
        cls,
        value,
        encoding=DEFAULT_ENCODING,
        /,
        params: dict[str, Any] | None = None,
    ):
        value = to_unicode(value, encoding=encoding)
        self = super().__new__(cls, value)
        match = WEEKDAY_RULE.match(self)
        if match is None:
            raise ValueError(f"Expected weekday abbreviation, got: {self}")
        match = match.groupdict()
        sign = match["signal"]
        weekday = match["weekday"]
        relative = match["relative"]
        if weekday not in vWeekday.week_days or sign not in "+-":
            raise ValueError(f"Expected weekday abbreviation, got: {self}")
        self.weekday = weekday or None
        self.relative = (relative and int(relative)) or None
        if sign == "-" and self.relative:
            self.relative *= -1
        self.params = Parameters(params)
        return self

    def to_ical(self):
        return self.encode(DEFAULT_ENCODING).upper()

    @classmethod
    def from_ical(cls, ical):
        try:
            return cls(ical.upper())
        except Exception as e:
            raise ValueError(f"Expected weekday abbreviation, got: {ical}") from e

    @classmethod
    def parse_jcal_value(cls, value: Any) -> Self:
        """Parse a jCal value for vWeekday.

        Raises:
            ~error.JCalParsingError: If the value is not a valid weekday.
        """
        JCalParsingError.validate_value_type(value, str, cls)
        try:
            return cls(value)
        except ValueError as e:
            raise JCalParsingError(
                "The value must be a valid weekday.", cls, value=value
            ) from e


__all__ = ["vWeekday"]
