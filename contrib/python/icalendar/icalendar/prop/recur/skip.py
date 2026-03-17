"""SKIP value type of RECUR from :rfc:`7529`."""

from typing import Any

from icalendar.compatibility import Self
from icalendar.enums import Enum
from icalendar.error import JCalParsingError
from icalendar.prop.text import vText


class vSkip(vText, Enum):
    """Skip values for RRULE.

    These are defined in :rfc:`7529`.

    OMIT  is the default value.

    Examples:

    .. code-block:: pycon

        >>> from icalendar import vSkip
        >>> vSkip.OMIT
        vSkip('OMIT')
        >>> vSkip.FORWARD
        vSkip('FORWARD')
        >>> vSkip.BACKWARD
        vSkip('BACKWARD')
    """

    OMIT = "OMIT"
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"

    __reduce_ex__ = Enum.__reduce_ex__

    def __repr__(self):
        return f"{self.__class__.__name__}({self._name_!r})"

    @classmethod
    def parse_jcal_value(cls, value: Any) -> Self:
        """Parse a jCal value for vSkip.

        Raises:
            ~error.JCalParsingError: If the value is not a valid skip value.
        """
        JCalParsingError.validate_value_type(value, str, cls)
        try:
            return cls[value.upper()]
        except KeyError as e:
            raise JCalParsingError(
                "The value must be a valid skip value.", cls, value=value
            ) from e


__all__ = ["vSkip"]
