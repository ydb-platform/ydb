"""Parsing error value preservation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from icalendar.error import BrokenCalendarProperty
from icalendar.parser_tools import DEFAULT_ENCODING
from icalendar.prop.text import vText

if TYPE_CHECKING:
    from icalendar.compatibility import Self
    from icalendar.parser import Parameters


class vBroken(vText):
    """Property that failed to parse, preserving raw value as text.

    Represents property values that failed to parse with their expected
    type. The raw iCalendar string is preserved for round-trip serialization.
    """

    default_value: ClassVar[str] = "TEXT"
    __slots__ = ("expected_type", "parse_error", "property_name")

    def __new__(
        cls,
        value: str | bytes,
        encoding: str = DEFAULT_ENCODING,
        /,
        params: dict[str, Any] | None = None,
        expected_type: str | None = None,
        property_name: str | None = None,
        parse_error: BaseException | None = None,
    ) -> Self:
        self = super().__new__(cls, value, encoding, params=params)
        self.expected_type = expected_type
        self.property_name = property_name
        self.parse_error = parse_error
        return self

    def __getattr__(self, name: str):
        """Raise BrokenCalendarProperty for attribute access.

        Attributes like ``.dt`` that the expected type would have
        raise :class:`~icalendar.error.BrokenCalendarProperty`
        with the original parse error chained. Private attributes
        (starting with ``_``) raise normal ``AttributeError`` to
        preserve ``getattr(..., default)`` patterns.
        """
        if name.startswith("_"):
            raise AttributeError(name)
        raise BrokenCalendarProperty(
            f"Cannot access {name!r} on broken property "
            f"{self.property_name!r} (expected {self.expected_type!r}): "
            f"{self.parse_error}"
        ) from self.parse_error

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({str(self)!r}, "
            f"expected_type={self.expected_type!r}, "
            f"property_name={self.property_name!r})"
        )

    @classmethod
    def from_parse_error(
        cls,
        raw_value: str,
        params: Parameters,
        property_name: str,
        expected_type: str,
        error: Exception,
    ) -> Self:
        """Create vBroken from parse failure."""
        return cls(
            raw_value,
            params=params,
            expected_type=expected_type,
            property_name=property_name,
            parse_error=error,
        )

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vBroken."""
        return [
            cls(
                "INVALID-DATE",
                expected_type="date-time",
                property_name="DTSTART",
                parse_error=ValueError("Invalid date format"),
            )
        ]


__all__ = ["vBroken"]
