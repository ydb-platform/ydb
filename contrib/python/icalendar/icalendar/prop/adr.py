"""ADR property of :rfc:`6350`."""

from typing import Any, ClassVar, NamedTuple

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.parser_tools import DEFAULT_ENCODING, to_unicode


class AdrFields(NamedTuple):
    """Named fields for vCard ADR (Address) property per :rfc:`6350#section-6.3.1`.

    Provides named access to the seven address components.
    """

    po_box: str
    """Post office box."""
    extended: str
    """Extended address (e.g., apartment or suite number)."""
    street: str
    """Street address."""
    locality: str
    """Locality (e.g., city)."""
    region: str
    """Region (e.g., state or province)."""
    postal_code: str
    """Postal code."""
    country: str
    """Country name (full name)."""


class vAdr:
    """vCard ADR (Address) structured property per :rfc:`6350#section-6.3.1`.

    The ADR property represents a delivery address as a single text value.
    The structured type value consists of a sequence of seven address components.
    The component values must be specified in their corresponding position.

    -   post office box
    -   extended address (e.g., apartment or suite number)
    -   street address
    -   locality (e.g., city)
    -   region (e.g., state or province)
    -   postal code
    -   country name (full name)

    When a component value is missing, the associated component separator
    MUST still be specified.

    Semicolons are field separators and are NOT escaped.
    Commas and backslashes within field values ARE escaped per :rfc:`6350`.

    Examples:
        .. code-block:: pycon

            >>> from icalendar.prop import vAdr
            >>> adr = vAdr(("", "", "123 Main St", "Springfield", "IL", "62701", "USA"))
            >>> adr.to_ical()
            b';;123 Main St;Springfield;IL;62701;USA'
            >>> vAdr.from_ical(";;123 Main St;Springfield;IL;62701;USA")
            AdrFields(po_box='', extended='', street='123 Main St', locality='Springfield', region='IL', postal_code='62701', country='USA')
    """

    default_value: ClassVar[str] = "TEXT"
    params: Parameters
    fields: AdrFields

    def __init__(
        self,
        fields: tuple[str, ...] | list[str] | str | AdrFields,
        /,
        params: dict[str, Any] | None = None,
    ):
        """Initialize ADR with seven fields or parse from vCard format string.

        Parameters:
            fields: Either an AdrFields, tuple, or list of seven strings, one per field,
                    or a vCard format string with semicolon-separated fields
            params: Optional property parameters
        """
        if isinstance(fields, str):
            fields = self.from_ical(fields)
        if isinstance(fields, AdrFields):
            self.fields = fields
        else:
            if len(fields) != 7:
                raise ValueError(f"ADR must have exactly 7 fields, got {len(fields)}")
            self.fields = AdrFields(*(str(f) for f in fields))
        self.params = Parameters(params)

    def to_ical(self) -> bytes:
        """Generate vCard format with semicolon-separated fields."""
        # Each field is vText (handles comma/backslash escaping)
        # but we join with unescaped semicolons (field separators)
        from icalendar.prop.text import vText

        parts = [vText(f).to_ical().decode(DEFAULT_ENCODING) for f in self.fields]
        return ";".join(parts).encode(DEFAULT_ENCODING)

    @staticmethod
    def from_ical(ical: str | bytes) -> AdrFields:
        """Parse vCard ADR format into an AdrFields named tuple.

        Parameters:
            ical: vCard format string with semicolon-separated fields

        Returns:
            AdrFields named tuple with seven field values.
        """
        from icalendar.parser import split_on_unescaped_semicolon

        ical = to_unicode(ical)
        fields = split_on_unescaped_semicolon(ical)
        if len(fields) != 7:
            raise ValueError(
                f"ADR must have exactly 7 fields, got {len(fields)}: {ical}"
            )
        return AdrFields(*fields)

    def __eq__(self, other: object) -> bool:
        """self == other"""
        return isinstance(other, vAdr) and self.fields == other.fields

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}({self.fields}, params={self.params})"

    @property
    def ical_value(self) -> AdrFields:
        """The address fields as a named tuple."""
        return self.fields

    from icalendar.param import VALUE

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        result = [name, self.params.to_jcal(), self.VALUE.lower()]
        result.extend(self.fields)
        return result

    @classmethod
    def from_jcal(cls, jcal_property: list) -> Self:
        """Parse jCal from :rfc:`7265`.

        Parameters:
            jcal_property: The jCal property to parse.

        Raises:
            ~error.JCalParsingError: If the provided jCal is invalid.
        """
        JCalParsingError.validate_property(jcal_property, cls)
        if len(jcal_property) != 10:  # name, params, value_type, 7 fields
            raise JCalParsingError(
                f"ADR must have 10 elements (name, params, value_type, 7 fields), "
                f"got {len(jcal_property)}"
            )
        for i, field in enumerate(jcal_property[3:], start=3):
            JCalParsingError.validate_value_type(field, str, cls, i)
        return cls(
            tuple(jcal_property[3:]),
            Parameters.from_jcal_property(jcal_property),
        )

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vAdr."""
        return [cls(("", "", "123 Main St", "Springfield", "IL", "62701", "USA"))]


__all__ = ["AdrFields", "vAdr"]
