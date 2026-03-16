"""The ORG property from :rfc:`6350`."""

from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.parser_tools import DEFAULT_ENCODING, to_unicode


class vOrg:
    r"""vCard ORG (Organization) structured property per :rfc:`6350#section-6.6.4`.

    The ORG property specifies the organizational name and units
    associated with the vCard.

    Its value is a structured type consisting of components separated
    by semicolons. The components are the organization name, followed
    by zero or more levels of organizational unit names:

    .. code-block:: text

        organization-name; organizational-unit-1; organizational-unit-2; ...

    Semicolons are field separators and are NOT escaped.
    Commas and backslashes within field values ARE escaped per :rfc:`6350`.

    Examples:
        A property value consisting of an organizational name,
        organizational unit #1 name, and organizational unit #2 name.

        .. code-block:: text

            ORG:ABC\, Inc.;North American Division;Marketing

        The same example in icalendar.

        .. code-block:: pycon

            >>> from icalendar.prop import vOrg
            >>> org = vOrg(("ABC, Inc.", "North American Division", "Marketing"))
            >>> org.to_ical()
            b'ABC\\, Inc.;North American Division;Marketing'
            >>> vOrg.from_ical(r"ABC\, Inc.;North American Division;Marketing")
            ('ABC, Inc.', 'North American Division', 'Marketing')
    """

    default_value: ClassVar[str] = "TEXT"
    params: Parameters
    fields: tuple[str, ...]

    def __init__(
        self,
        fields: tuple[str, ...] | list[str] | str,
        /,
        params: dict[str, Any] | None = None,
    ):
        """Initialize ORG with variable fields or parse from vCard format string.

        Parameters:
            fields: Either a tuple or list of one or more strings, or a
                    vCard format string with semicolon-separated fields
            params: Optional property parameters
        """
        if isinstance(fields, str):
            fields = self.from_ical(fields)
        if len(fields) < 1:
            raise ValueError("ORG must have at least 1 field (organization name)")
        self.fields = tuple(str(f) for f in fields)
        self.params = Parameters(params)

    def to_ical(self) -> bytes:
        """Generate vCard format with semicolon-separated fields."""
        from icalendar.prop.text import vText

        parts = [vText(f).to_ical().decode(DEFAULT_ENCODING) for f in self.fields]
        return ";".join(parts).encode(DEFAULT_ENCODING)

    @staticmethod
    def from_ical(ical: str | bytes) -> tuple[str, ...]:
        """Parse vCard ORG format into a tuple of fields.

        Parameters:
            ical: vCard format string with semicolon-separated fields

        Returns:
            Tuple of field values with one or more fields
        """
        from icalendar.parser import split_on_unescaped_semicolon

        ical = to_unicode(ical)
        fields = split_on_unescaped_semicolon(ical)
        if len(fields) < 1:
            raise ValueError(f"ORG must have at least 1 field: {ical}")
        return tuple(fields)

    def __eq__(self, other: object) -> bool:
        """self == other"""
        return isinstance(other, vOrg) and self.fields == other.fields

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}({self.fields}, params={self.params})"

    @property
    def name(self) -> str:
        """The organization name (first field)."""
        return self.fields[0]

    @property
    def units(self) -> tuple[str, ...]:
        """The organizational unit names (remaining fields after the name)."""
        return self.fields[1:]

    @property
    def ical_value(self) -> tuple[str, ...]:
        """The organization fields as a tuple."""
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
        if len(jcal_property) < 4:  # name, params, value_type, at least 1 field
            raise JCalParsingError(
                "ORG must have at least 4 elements"
                " (name, params, value_type, org name),"
                f" got {len(jcal_property)}"
            )
        for i, field in enumerate(jcal_property[3:], start=3):
            JCalParsingError.validate_value_type(field, str, cls, i)
        return cls(
            tuple(jcal_property[3:]),
            Parameters.from_jcal_property(jcal_property),
        )

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vOrg."""
        return [cls(("ABC Inc.", "North American Division", "Marketing"))]


__all__ = ["vOrg"]
