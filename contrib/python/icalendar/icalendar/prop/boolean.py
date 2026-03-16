"""BOOLEAN values from :rfc:`5545`."""

from typing import Any, ClassVar

from icalendar.caselessdict import CaselessDict
from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters


class vBoolean(int):
    """Boolean

    Value Name:  BOOLEAN

    Purpose:  This value type is used to identify properties that contain
      either a "TRUE" or "FALSE" Boolean value.

    Format Definition:  This value type is defined by the following
      notation:

    .. code-block:: text

        boolean    = "TRUE" / "FALSE"

    Description:  These values are case-insensitive text.  No additional
      content value encoding is defined for this value type.

    Example:  The following is an example of a hypothetical property that
      has a BOOLEAN value type:

    .. code-block:: python

        TRUE

    .. code-block:: pycon

        >>> from icalendar.prop import vBoolean
        >>> boolean = vBoolean.from_ical('TRUE')
        >>> boolean
        True
        >>> boolean = vBoolean.from_ical('FALSE')
        >>> boolean
        False
        >>> boolean = vBoolean.from_ical('True')
        >>> boolean
        True
    """

    default_value: ClassVar[str] = "BOOLEAN"
    params: Parameters

    BOOL_MAP = CaselessDict({"true": True, "false": False})

    def __new__(
        cls, *args: Any, params: dict[str, Any] | None = None, **kwargs: Any
    ) -> Self:
        self = super().__new__(cls, *args, **kwargs)
        self.params = Parameters(params)
        return self

    def to_ical(self) -> bytes:
        return b"TRUE" if self else b"FALSE"

    @classmethod
    def from_ical(cls, ical: str) -> bool:
        try:
            return cls.BOOL_MAP[ical]
        except Exception as e:
            raise ValueError(f"Expected 'TRUE' or 'FALSE'. Got {ical}") from e

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vBoolean."""
        return [
            cls(True),
            cls(False),
        ]

    from icalendar.param import VALUE

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        return [name, self.params.to_jcal(), self.VALUE.lower(), bool(self)]

    @classmethod
    def from_jcal(cls, jcal_property: list) -> Self:
        """Parse jCal from :rfc:`7265` to a vBoolean.

        Parameters:
            jcal_property: The jCal property to parse.

        Raises:
            ~error.JCalParsingError: If the provided jCal is invalid.
        """
        JCalParsingError.validate_property(jcal_property, cls)
        JCalParsingError.validate_value_type(jcal_property[3], bool, cls, 3)
        return cls(
            jcal_property[3],
            params=Parameters.from_jcal_property(jcal_property),
        )


__all__ = ["vBoolean"]
