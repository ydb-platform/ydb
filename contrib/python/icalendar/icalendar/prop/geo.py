"""GEO property values from :rfc:`5545`."""

from typing import Any, ClassVar

from icalendar.compatibility import Self
from icalendar.error import JCalParsingError
from icalendar.parser import Parameters


class vGeo:
    """Geographic Position

    Property Name:
        GEO

    Purpose:
        This property specifies information related to the global
        position for the activity specified by a calendar component.

    Value Type:
        FLOAT.  The value MUST be two SEMICOLON-separated FLOAT values.

    Property Parameters:
        IANA and non-standard property parameters can be specified on
        this property.

    Conformance:
        This property can be specified in "VEVENT" or "VTODO"
        calendar components.

    Description:
        This property value specifies latitude and longitude,
        in that order (i.e., "LAT LON" ordering).  The longitude
        represents the location east or west of the prime meridian as a
        positive or negative real number, respectively.  The longitude and
        latitude values MAY be specified up to six decimal places, which
        will allow for accuracy to within one meter of geographical
        position.  Receiving applications MUST accept values of this
        precision and MAY truncate values of greater precision.

        Example:

        .. code-block:: text

            GEO:37.386013;-122.082932

        Parse vGeo:

        .. code-block:: pycon

            >>> from icalendar.prop import vGeo
            >>> geo = vGeo.from_ical('37.386013;-122.082932')
            >>> geo
            (37.386013, -122.082932)

        Add a geo location to an event:

        .. code-block:: pycon

            >>> from icalendar import Event
            >>> event = Event()
            >>> latitude = 37.386013
            >>> longitude = -122.082932
            >>> event.add('GEO', (latitude, longitude))
            >>> event['GEO']
            vGeo((37.386013, -122.082932))
    """

    default_value: ClassVar[str] = "FLOAT"
    params: Parameters

    def __init__(
        self,
        geo: tuple[float | str | int, float | str | int],
        /,
        params: dict[str, Any] | None = None,
    ):
        """Create a new vGeo from a tuple of (latitude, longitude).

        Raises:
            ValueError: if geo is not a tuple of (latitude, longitude)
        """
        try:
            latitude, longitude = (geo[0], geo[1])
            latitude = float(latitude)
            longitude = float(longitude)
        except Exception as e:
            raise ValueError(
                "Input must be (float, float) for latitude and longitude"
            ) from e
        self.latitude = latitude
        self.longitude = longitude
        self.params = Parameters(params)

    def to_ical(self) -> str:
        return f"{self.latitude};{self.longitude}"

    @staticmethod
    def from_ical(ical: str) -> tuple[float, float]:
        try:
            latitude, longitude = ical.split(";")
            return (float(latitude), float(longitude))
        except Exception as e:
            raise ValueError(f"Expected 'float;float' , got: {ical}") from e

    def __eq__(self, other: object) -> bool:
        return isinstance(other, vGeo) and self.to_ical() == other.to_ical()

    def __hash__(self) -> int:
        """Hash of the vGeo object."""
        return hash((self.latitude, self.longitude))

    def __repr__(self) -> str:
        """repr(self)"""
        return f"{self.__class__.__name__}(({self.latitude}, {self.longitude}))"

    def to_jcal(self, name: str) -> list:
        """Convert to jCal object."""
        return [
            name,
            self.params.to_jcal(),
            self.VALUE.lower(),
            [self.latitude, self.longitude],
        ]

    @classmethod
    def examples(cls) -> list[Self]:
        """Examples of vGeo."""
        return [cls((37.386013, -122.082932))]

    from icalendar.param import VALUE

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


__all__ = ["vGeo"]
