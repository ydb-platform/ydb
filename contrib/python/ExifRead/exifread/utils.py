"""
Misc utilities.
"""

from fractions import Fraction
from typing import Optional, Tuple


def _degrees_to_decimal(degrees: float, minutes: float, seconds: float) -> float:
    """
    Converts coordinates from a degrees minutes seconds format to a decimal degrees format.
    Reference: https://en.wikipedia.org/wiki/Geographic_coordinate_conversion
    """
    return degrees + minutes / 60 + seconds / 3600


def get_gps_coords(tags: dict) -> Optional[Tuple[float, float]]:
    """
    Extract tuple of latitude and longitude values in decimal degrees format from EXIF tags.
    Return None if no GPS coordinates are found.
    Handles regular and serialized Exif tags.
    """
    gps = {
        "lat_coord": "GPS GPSLatitude",
        "lat_ref": "GPS GPSLatitudeRef",
        "lng_coord": "GPS GPSLongitude",
        "lng_ref": "GPS GPSLongitudeRef",
    }

    # Verify if required keys are a subset of provided tags
    if not set(gps.values()) <= tags.keys():
        return None

    # If tags have not been converted to native Python types, do it
    if not isinstance(tags[gps["lat_coord"]], list):
        tags[gps["lat_coord"]] = [c.decimal() for c in tags[gps["lat_coord"]].values]
        tags[gps["lng_coord"]] = [c.decimal() for c in tags[gps["lng_coord"]].values]
        tags[gps["lat_ref"]] = tags[gps["lat_ref"]].values
        tags[gps["lng_ref"]] = tags[gps["lng_ref"]].values

    lat = _degrees_to_decimal(*tags[gps["lat_coord"]])
    if tags[gps["lat_ref"]] == "S":
        lat *= -1

    lng = _degrees_to_decimal(*tags[gps["lng_coord"]])
    if tags[gps["lng_ref"]] == "W":
        lng *= -1

    return lat, lng


class Ratio(Fraction):
    """
    Ratio object that eventually will be able to reduce itself to lowest
    common denominator for printing.
    """

    _numerator: Optional[int]
    _denominator: Optional[int]

    # We're immutable, so use __new__ not __init__
    def __new__(cls, numerator: int = 0, denominator: Optional[int] = None):
        try:
            self = super(Ratio, cls).__new__(cls, numerator, denominator)
        except ZeroDivisionError:
            self = super(Ratio, cls).__new__(cls)
            self._numerator = numerator
            self._denominator = denominator
        return self

    def __repr__(self) -> str:
        return str(self)

    @property
    def num(self) -> int:
        return self.numerator

    @property
    def den(self) -> int:
        return self.denominator

    def decimal(self) -> float:
        return float(self)
