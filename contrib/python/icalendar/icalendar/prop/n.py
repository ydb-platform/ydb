"""N property from :rfc:`6350`."""

from typing import Any, ClassVar, NamedTuple

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.parser_tools import DEFAULT_ENCODING, to_unicode


class NFields(NamedTuple):
    """Named fields for vCard N (Name) property per :rfc:`6350#section-6.2.2`.

    Provides named access to the five name components.
    """

    family: str
    """Family names (also known as surnames)."""
    given: str
    """Given names."""
    additional: str
    """Additional names."""
    prefix: str
    """Honorific prefixes."""
    suffix: str
    """Honorific suffixes."""


class vN:
    r"""vCard N (Name) structured property per :rfc:`6350#section-6.2.2`.

    The N property represents a person's name.
    It consists of a single structured text value.
    Each component in the structure may have multiple values, separated by commas.

    The structured property value corresponds, in sequence, to the following fields:

    -   family names (also known as surnames)
    -   given names
    -   additional names
    -   honorific prefixes
    -   honorific suffixes

    Semicolons are field separators and are NOT escaped.
    Commas and backslashes within field values ARE escaped per :rfc:`6350`.

    Examples:

        .. code-block:: pycon

            >>> from icalendar.prop import vN
            >>> n = vN(("Doe", "John", "M.", "Dr.", "Jr.,M.D.,A.C.P."))
            >>> n.to_ical()
            b'Doe;John;M.;Dr.;Jr.\\,M.D.\\,A.C.P.'
            >>> vN.from_ical(r"Doe;John;M.;Dr.;Jr.\,M.D.\,A.C.P.")
            NFields(family='Doe', given='John', additional='M.', prefix='Dr.', suffix='Jr.,M.D.,A.C.P.')
    """

    default_value: ClassVar[str] = "TEXT"
    params: Parameters
    fields: NFields

    def __init__(
        self,
        fields: tuple[str, ...] | list[str] | str | NFields,
        /,
        params: dict[str, Any] | None = None,
    ):
        """Initialize N with five fields or parse from vCard format string.

        Parameters:
            fields: Either an NFields, tuple, or list of five strings, one per field,
                    or a vCard format string with semicolon-separated fields
            params: Optional property parameters
        """
        if isinstance(fields, str):
            fields = self.from_ical(fields)
        if isinstance(fields, NFields):
            self.fields = fields
        else:
            if len(fields) != 5:
                raise ValueError(f"N must have exactly 5 fields, got {len(fields)}")
            self.fields = NFields(*(str(f) for f in fields))
        self.params = Parameters(params)

    def to_ical(self) -> bytes:
        """Generate vCard format with semicolon-separated fields."""
        from icalendar.prop.text import vText

        parts = [vText(f).to_ical().decode(DEFAULT_ENCODING) for f in self.fields]
        return ";".join(parts).encode(DEFAULT_ENCODING)

    @staticmethod
    def from_ical(ical: str | bytes) -> NFields:
        """Parse vCard N format into an NFields named tuple.

        Parameters:
            ical: vCard format string with semicolon-separated fields

        Returns:
            NFields named tuple with five field values.
        """
        from icalendar.parser import split_on_unescaped_semicolon

        ical = to_unicode(ical)
        fields = split_on_unescaped_semicolon(ical)
        if len(fields) != 5:
            raise ValueError(f"N must have exactly 5 fields, got {len(fields)}: {ical}")
        return NFields(*fields)

    def __eq__(self, other: object) -> bool:
        """self == other"""
        return isinstance(other, vN) and self.fields == other.fields

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}({self.fields}, params={self.params})"

    @property
    def ical_value(self) -> NFields:
        """The name fields as a named tuple."""
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
        if len(jcal_property) != 8:  # name, params, value_type, 5 fields
            raise JCalParsingError(
                f"N must have 8 elements (name, params, value_type, 5 fields), "
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
        """Examples of vN."""
        return [cls(("Doe", "John", "M.", "Dr.", "Jr."))]


__all__ = ["NFields", "vN"]
