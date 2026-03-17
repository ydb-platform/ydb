"""CAL-ADDRESS values from :rfc:`5545`."""

from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.parser_tools import DEFAULT_ENCODING, to_unicode


class vCalAddress(str):
    r"""Calendar User Address

    Value Name:
        CAL-ADDRESS

    Purpose:
        This value type is used to identify properties that contain a
        calendar user address.

    Description:
        The value is a URI as defined by [RFC3986] or any other
        IANA-registered form for a URI.  When used to address an Internet
        email transport address for a calendar user, the value MUST be a
        mailto URI, as defined by [RFC2368].

    Example:
        ``mailto:`` is in front of the address.

        .. code-block:: text

            mailto:jane_doe@example.com

        Parsing:

        .. code-block:: pycon

            >>> from icalendar import vCalAddress
            >>> cal_address = vCalAddress.from_ical('mailto:jane_doe@example.com')
            >>> cal_address
            vCalAddress('mailto:jane_doe@example.com')

        Encoding:

        .. code-block:: pycon

            >>> from icalendar import vCalAddress, Event
            >>> event = Event()
            >>> jane = vCalAddress("mailto:jane_doe@example.com")
            >>> jane.name = "Jane"
            >>> event["organizer"] = jane
            >>> print(event.to_ical().decode().replace('\\r\\n', '\\n').strip())
            BEGIN:VEVENT
            ORGANIZER;CN=Jane:mailto:jane_doe@example.com
            END:VEVENT
    """

    default_value: ClassVar[str] = "CAL-ADDRESS"
    params: Parameters
    __slots__ = ("params",)

    def __new__(
        cls,
        value: str | bytes,
        encoding: str = DEFAULT_ENCODING,
        /,
        params: dict[str, Any] | None = None,
    ) -> Self:
        value = to_unicode(value, encoding=encoding)
        self = super().__new__(cls, value)
        self.params = Parameters(params)
        return self

    def __repr__(self) -> str:
        return f"vCalAddress('{self}')"

    def to_ical(self) -> bytes:
        return self.encode(DEFAULT_ENCODING)

    @classmethod
    def from_ical(cls, ical: str | bytes) -> Self:
        return cls(ical)

    @property
    def ical_value(self) -> str:
        """The ``mailto:`` part of the address."""
        return str(self)

    @property
    def email(self) -> str:
        """The email address without ``mailto:`` at the start."""
        if self.lower().startswith("mailto:"):
            return self[7:]
        return str(self)

    from icalendar.param import (
        CN,
        CUTYPE,
        DELEGATED_FROM,
        DELEGATED_TO,
        DIR,
        LANGUAGE,
        PARTSTAT,
        ROLE,
        RSVP,
        SENT_BY,
        VALUE,
    )

    name = CN

    @staticmethod
    def _get_email(email: str) -> str:
        """Extract email and add mailto: prefix if needed.

        Handles case-insensitive mailto: prefix checking.

        Parameters:
            email: Email string that may or may not have mailto: prefix

        Returns:
            Email string with mailto: prefix
        """
        if not email.lower().startswith("mailto:"):
            return f"mailto:{email}"
        return email

    @classmethod
    def new(
        cls,
        email: str,
        /,
        cn: str | None = None,
        cutype: str | None = None,
        delegated_from: str | None = None,
        delegated_to: str | None = None,
        directory: str | None = None,
        language: str | None = None,
        partstat: str | None = None,
        role: str | None = None,
        rsvp: bool | None = None,  # noqa: FBT001, RUF100
        sent_by: str | None = None,
    ) -> Self:
        """Create a new vCalAddress with RFC 5545 parameters.

        Creates a vCalAddress instance with automatic mailto: prefix handling
        and support for all standard RFC 5545 parameters.

        Parameters:
            email: The email address (mailto: prefix added automatically if missing)
            cn: Common Name parameter
            cutype: Calendar user type (INDIVIDUAL, GROUP, RESOURCE, ROOM)
            delegated_from: Email of the calendar user that delegated
            delegated_to: Email of the calendar user that was delegated to
            directory: Reference to directory information
            language: Language for text values
            partstat: Participation status (NEEDS-ACTION, ACCEPTED, DECLINED, etc.)
            role: Role (REQ-PARTICIPANT, OPT-PARTICIPANT, NON-PARTICIPANT, CHAIR)
            rsvp: Whether RSVP is requested
            sent_by: Email of the calendar user acting on behalf of this user

        Returns:
            vCalAddress: A new calendar address with specified parameters

        Raises:
            TypeError: If email is not a string

        Examples:
            Basic usage:

            >>> from icalendar.prop import vCalAddress
            >>> addr = vCalAddress.new("test@test.com")
            >>> str(addr)
            'mailto:test@test.com'

            With parameters:

            >>> addr = vCalAddress.new("test@test.com", cn="Test User", role="CHAIR")
            >>> addr.params["CN"]
            'Test User'
            >>> addr.params["ROLE"]
            'CHAIR'
        """
        if not isinstance(email, str):
            raise TypeError(f"Email must be a string, not {type(email).__name__}")

        # Handle mailto: prefix (case-insensitive)
        email_with_prefix = cls._get_email(email)

        # Create the address
        addr = cls(email_with_prefix)

        # Set parameters if provided
        if cn is not None:
            addr.params["CN"] = cn
        if cutype is not None:
            addr.params["CUTYPE"] = cutype
        if delegated_from is not None:
            addr.params["DELEGATED-FROM"] = cls._get_email(delegated_from)
        if delegated_to is not None:
            addr.params["DELEGATED-TO"] = cls._get_email(delegated_to)
        if directory is not None:
            addr.params["DIR"] = directory
        if language is not None:
            addr.params["LANGUAGE"] = language
        if partstat is not None:
            addr.params["PARTSTAT"] = partstat
        if role is not None:
            addr.params["ROLE"] = role
        if rsvp is not None:
            addr.params["RSVP"] = "TRUE" if rsvp else "FALSE"
        if sent_by is not None:
            addr.params["SENT-BY"] = cls._get_email(sent_by)

        return addr

    def to_jcal(self, name: str) -> list:
        """Return this property in jCal format."""
        return [name, self.params.to_jcal(), self.VALUE.lower(), self.ical_value]

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vCalAddress."""
        return [cls.new("you@example.org", cn="You There")]

    @classmethod
    def from_jcal(cls, jcal_property: list) -> Self:
        """Parse jCal from :rfc:`7265`.

        Parameters:
            jcal_property: The jCal property to parse.

        Raises:
            ~error.JCalParsingError: If the provided jCal is invalid.
        """
        JCalParsingError.validate_property(jcal_property, cls)
        JCalParsingError.validate_value_type(jcal_property[3], str, cls, 3)
        return cls(
            jcal_property[3],
            params=Parameters.from_jcal_property(jcal_property),
        )


__all__ = ["vCalAddress"]
