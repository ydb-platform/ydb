from datetime import date, datetime
from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.parser_tools import from_unicode

from .base import TimeBase
from .types import vDDDTypes


class vDDDLists:
    """A list of vDDDTypes values."""

    default_value: ClassVar[str] = "DATE-TIME"
    params: Parameters
    dts: list[vDDDTypes]

    def __init__(self, dt_list, params: dict[str, Any] | None = None):
        if params is None:
            params = {}
        if not hasattr(dt_list, "__iter__"):
            dt_list = [dt_list]
        vddd = []
        tzid = None
        for dt_l in dt_list:
            dt = vDDDTypes(dt_l) if not isinstance(dt_l, vDDDTypes) else dt_l
            vddd.append(dt)
            if "TZID" in dt.params:
                tzid = dt.params["TZID"]

        if tzid:
            # NOTE: no support for multiple timezones here!
            params["TZID"] = tzid
        self.params = Parameters(params)
        self.dts = vddd

    def to_ical(self):
        dts_ical = (from_unicode(dt.to_ical()) for dt in self.dts)
        return b",".join(dts_ical)

    @staticmethod
    def from_ical(ical, timezone=None):
        out = []
        ical_dates = ical.split(",")
        for ical_dt in ical_dates:
            out.append(vDDDTypes.from_ical(ical_dt, timezone=timezone))
        return out

    def __eq__(self, other):
        if isinstance(other, vDDDLists):
            return self.dts == other.dts
        if isinstance(other, (TimeBase, date)):
            return self.dts == [other]
        return False

    def __repr__(self):
        """String representation."""
        return f"{self.__class__.__name__}({self.dts})"

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vDDDLists."""
        return [vDDDLists([datetime(2025, 11, 10, 16, 50)])]

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        return [
            name,
            self.params.to_jcal(),
            self.VALUE.lower(),
            *[dt.to_jcal(name)[3] for dt in self.dts],
        ]

    def _get_value(self) -> str | None:
        return None if not self.dts else self.dts[0].VALUE

    from icalendar.param import VALUE

    @classmethod
    def from_jcal(cls, jcal_property: list) -> Self:
        """Parse jCal from :rfc:`7265`.

        Parameters:
            jcal_property: The jCal property to parse.

        Raises:
            ~error.JCalParsingError: If the jCal provided is invalid.
        """
        JCalParsingError.validate_property(jcal_property, cls)
        values = jcal_property[3:]
        prop = jcal_property[:3]
        dts = []
        for value in values:
            dts.append(vDDDTypes.from_jcal(prop + [value]))
        return cls(
            dts,
            params=Parameters.from_jcal_property(jcal_property),
        )

    __hash__ = None


__all__ = ["vDDDLists"]
