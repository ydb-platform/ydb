"""URI values from :rfc:`5545`."""

from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.parser_tools import DEFAULT_ENCODING, to_unicode


class vUri(str):
    """URI

    Value Name:
        URI

    Purpose:
        This value type is used to identify values that contain a
        uniform resource identifier (URI) type of reference to the
        property value.

    Format Definition:
        This value type is defined by the following notation:

        .. code-block:: text

            uri = scheme ":" hier-part [ "?" query ] [ "#" fragment ]

    Description:
        This value type might be used to reference binary
        information, for values that are large, or otherwise undesirable
        to include directly in the iCalendar object.

        Property values with this value type MUST follow the generic URI
        syntax defined in [RFC3986].

        When a property parameter value is a URI value type, the URI MUST
        be specified as a quoted-string value.

    Examples:
        The following is a URI for a network file:

        .. code-block:: text

            http://example.com/my-report.txt

        .. code-block:: pycon

            >>> from icalendar.prop import vUri
            >>> uri = vUri.from_ical('http://example.com/my-report.txt')
            >>> uri
            vUri('http://example.com/my-report.txt')
            >>> uri.uri
            'http://example.com/my-report.txt'
    """

    default_value: ClassVar[str] = "URI"
    params: Parameters
    __slots__ = ("params",)

    def __new__(
        cls,
        value: str,
        encoding: str = DEFAULT_ENCODING,
        /,
        params: dict[str, Any] | None = None,
    ) -> Self:
        value = to_unicode(value, encoding=encoding)
        self = super().__new__(cls, value)
        self.params = Parameters(params)
        return self

    def to_ical(self) -> bytes:
        return self.encode(DEFAULT_ENCODING)

    @classmethod
    def from_ical(cls, ical: str | bytes) -> Self:
        try:
            return cls(ical)
        except Exception as e:
            raise ValueError(f"Expected , got: {ical}") from e

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vUri."""
        return [cls("http://example.com/my-report.txt")]

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        return [name, self.params.to_jcal(), self.VALUE.lower(), str(self)]

    @classmethod
    def from_jcal(cls, jcal_property: list) -> Self:
        """Parse jCal from :rfc:`7265`.

        Parameters:
            jcal_property: The jCal property to parse.

        Raises:
            ~error.JCalParsingError: If the provided jCal is invalid.
        """
        JCalParsingError.validate_property(jcal_property, cls)
        return cls(
            jcal_property[3],
            Parameters.from_jcal_property(jcal_property),
        )

    @property
    def ical_value(self) -> str:
        """The URI."""
        return self.uri

    @property
    def uri(self) -> str:
        """The URI."""
        return str(self)

    def __repr__(self) -> str:
        """repr(self)"""
        return f"{self.__class__.__name__}({self.uri!r})"

    from icalendar.param import FMTTYPE, GAP, LABEL, LANGUAGE, LINKREL, RELTYPE, VALUE


__all__ = ["vUri"]
