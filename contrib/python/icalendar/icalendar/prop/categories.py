"""CATEGORIES property values from :rfc:`5545`."""

from collections.abc import Iterator
from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.parser_tools import to_unicode
from icalendar.prop.text import vText


class vCategory:
    default_value: ClassVar[str] = "TEXT"
    params: Parameters

    def __init__(
        self, c_list: list[str] | str, /, params: dict[str, Any] | None = None
    ):
        if not hasattr(c_list, "__iter__") or isinstance(c_list, str):
            c_list = [c_list]
        self.cats: list[vText | str] = [vText(c) for c in c_list]
        self.params = Parameters(params)

    def __iter__(self) -> Iterator[vText | str]:
        return iter(self.cats)

    def to_ical(self) -> bytes:
        return b",".join(
            [
                c.to_ical() if hasattr(c, "to_ical") else vText(c).to_ical()
                for c in self.cats
            ]
        )

    @staticmethod
    def from_ical(ical: list[str] | str) -> list[str]:
        """Parse a CATEGORIES value from iCalendar format.

        This helper is normally called by :meth:`Component.from_ical`, which
        already splits the CATEGORIES property into a list of unescaped
        category strings. New code should therefore pass a list of strings.

        Passing a single comma-separated string is supported only for
        backwards compatibility with older parsing code and is considered
        legacy behavior.

        Parameters:
            ical: A list of category strings (preferred, as provided by
                :meth:`Component.from_ical`), or a single comma-separated
                string from a legacy caller.

        Returns:
            A list of category strings.
        """
        if isinstance(ical, list):
            # Already split by Component.from_ical()
            return ical
        # Legacy: simple comma split (no escaping handled)
        ical = to_unicode(ical)
        return ical.split(",")

    def __eq__(self, other: object) -> bool:
        """self == other"""
        return isinstance(other, vCategory) and self.cats == other.cats

    def __hash__(self) -> int:
        """Hash of the vCategory object."""
        return hash(tuple(self.cats))

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}({self.cats}, params={self.params})"

    def to_jcal(self, name: str) -> list:
        """The jCal representation for categories."""
        result = [name, self.params.to_jcal(), self.VALUE.lower()]
        result.extend(map(str, self.cats))
        if not self.cats:
            result.append("")
        return result

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vCategory."""
        return [cls(["HOME", "COSY"])]

    from icalendar.param import VALUE

    @classmethod
    def from_jcal(cls, jcal_property: list) -> Self:
        """Parse jCal from :rfc:`7265`.

        Parameters:
            jcal_property: The jCal property to parse.

        Raises:
            ~error.JCalParsingError: If the provided jCal is invalid.
        """
        JCalParsingError.validate_property(jcal_property, cls)
        for i, category in enumerate(jcal_property[3:], start=3):
            JCalParsingError.validate_value_type(category, str, cls, i)
        return cls(
            jcal_property[3:],
            Parameters.from_jcal_property(jcal_property),
        )

    @property
    def ical_value(self) -> list[str]:
        """The list of categories as strings."""
        return [str(cat) for cat in self.cats]


__all__ = ["vCategory"]
