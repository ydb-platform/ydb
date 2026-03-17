"""INT values from :rfc:`5545`."""

from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters
from icalendar.parser_tools import ICAL_TYPE


class vInt(int):
    """Integer

    Value Name:
        INTEGER

    Purpose:
        This value type is used to identify properties that contain a
        signed integer value.

    Format Definition:
        This value type is defined by the following notation:

        .. code-block:: text

            integer    = (["+"] / "-") 1*DIGIT

    Description:
        If the property permits, multiple "integer" values are
        specified by a COMMA-separated list of values.  The valid range
        for "integer" is -2147483648 to 2147483647.  If the sign is not
        specified, then the value is assumed to be positive.

    The ``__new__`` method creates a vInt instance:

    Parameters:
        value: Integer value to encode. Can be positive or negative within
            the range -2147483648 to 2147483647.
        params: Optional parameter dictionary for the property.

    Returns:
        vInt instance

    Examples:

        .. code-block:: text

            1234567890
            -1234567890
            +1234567890
            432109876

        .. code-block:: pycon

            >>> from icalendar.prop import vInt
            >>> integer = vInt.from_ical('1234567890')
            >>> integer
            1234567890
            >>> integer = vInt.from_ical('-1234567890')
            >>> integer
            -1234567890
            >>> integer = vInt.from_ical('+1234567890')
            >>> integer
            1234567890
            >>> integer = vInt.from_ical('432109876')
            >>> integer
            432109876

        Create a PRIORITY property (1 = highest priority):

        .. code-block:: pycon

            >>> priority = vInt(1)
            >>> priority
            1
            >>> priority.to_ical()
            b'1'

        Create SEQUENCE property (for versioning):

        .. code-block:: pycon

            >>> sequence = vInt(3)
            >>> sequence.to_ical()
            b'3'
    """

    default_value: ClassVar[str] = "INTEGER"
    params: Parameters

    def __new__(cls, *args, params: dict[str, Any] | None = None, **kwargs):
        self = super().__new__(cls, *args, **kwargs)
        self.params = Parameters(params)
        return self

    def to_ical(self) -> bytes:
        return str(self).encode("utf-8")

    @classmethod
    def from_ical(cls, ical: ICAL_TYPE):
        try:
            return cls(ical)
        except Exception as e:
            raise ValueError(f"Expected int, got: {ical}") from e

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vInt."""
        return [vInt(1000), vInt(-42)]

    from icalendar.param import VALUE

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        return [name, self.params.to_jcal(), self.VALUE.lower(), int(self)]

    @classmethod
    def from_jcal(cls, jcal_property: list) -> Self:
        """Parse jCal from :rfc:`7265`.

        Parameters:
            jcal_property: The jCal property to parse.

        Raises:
            ~error.JCalParsingError: If the provided jCal is invalid.
        """
        JCalParsingError.validate_property(jcal_property, cls)
        JCalParsingError.validate_value_type(jcal_property[3], int, cls, 3)
        return cls(
            jcal_property[3],
            params=Parameters.from_jcal_property(jcal_property),
        )

    @classmethod
    def parse_jcal_value(cls, value: Any) -> int:
        """Parse a jCal value for vInt.

        Raises:
            ~error.JCalParsingError: If the value is not an int.
        """
        JCalParsingError.validate_value_type(value, int, cls)
        return cls(value)


__all__ = ["vInt"]
