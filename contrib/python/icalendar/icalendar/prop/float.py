"""FLOAT values from :rfc:`5545`."""

from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters


class vFloat(float):
    """Float

    Value Name:
        FLOAT

    Purpose:
        This value type is used to identify properties that contain
        a real-number value.

    Format Definition:
        This value type is defined by the following notation:

        .. code-block:: text

            float      = (["+"] / "-") 1*DIGIT ["." 1*DIGIT]

    Description:
        If the property permits, multiple "float" values are
        specified by a COMMA-separated list of values.

        Example:

        .. code-block:: text

            1000000.0000001
            1.333
            -3.14

        .. code-block:: pycon

            >>> from icalendar.prop import vFloat
            >>> float = vFloat.from_ical('1000000.0000001')
            >>> float
            1000000.0000001
            >>> float = vFloat.from_ical('1.333')
            >>> float
            1.333
            >>> float = vFloat.from_ical('+1.333')
            >>> float
            1.333
            >>> float = vFloat.from_ical('-3.14')
            >>> float
            -3.14
    """

    default_value: ClassVar[str] = "FLOAT"
    params: Parameters

    def __new__(
        cls, *args: Any, params: dict[str, Any] | None = None, **kwargs: Any
    ) -> Self:
        self = super().__new__(cls, *args, **kwargs)
        self.params = Parameters(params)
        return self

    def to_ical(self) -> bytes:
        return str(self).encode("utf-8")

    @classmethod
    def from_ical(cls, ical: str | float) -> Self:
        try:
            return cls(ical)
        except Exception as e:
            raise ValueError(f"Expected float value, got: {ical}") from e

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vFloat."""
        return [cls(3.1415)]

    from icalendar.param import VALUE

    def to_jcal(self, name: str) -> list:
        """The jCal representation of this property according to :rfc:`7265`."""
        return [name, self.params.to_jcal(), self.VALUE.lower(), float(self)]

    @classmethod
    def from_jcal(cls, jcal_property: list) -> Self:
        """Parse jCal from :rfc:`7265`.

        Parameters:
            jcal_property: The jCal property to parse.

        Raises:
            ~error.JCalParsingError: If the jCal provided is invalid.
        """
        JCalParsingError.validate_property(jcal_property, cls)
        if jcal_property[0].upper() == "GEO":
            from icalendar.prop import vGeo

            return vGeo.from_jcal(jcal_property)
        JCalParsingError.validate_value_type(jcal_property[3], float, cls, 3)
        return cls(
            jcal_property[3],
            params=Parameters.from_jcal_property(jcal_property),
        )


__all__ = ["vFloat"]
