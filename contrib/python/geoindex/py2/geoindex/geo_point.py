"""
Geo point. Our alternative for accurate, but not so fast geopy library.

"""

import math
from . import utils


class GeoPoint(object):
    """
    Simple geo point implementation.
    """
    __slots__ = (
        'latitude', 'longitude', '_rad_latitude', '_rad_longitude', 'ref')

    def __init__(self, latitude, longitude, ref=None):
        """
        Init geo point with latitude and longitude.
        ref can be used for reference to some associated data
        """
        self.latitude = latitude
        self.longitude = longitude
        self.ref = ref

        self._rad_latitude = None
        self._rad_longitude = None

    def __eq__(self, other):
        """
        Check if other point has same coordinates to current or not.
        """
        if not isinstance(other, GeoPoint):
            return False
        return (
            round(self.latitude, 6) == round(other.latitude, 6) and
            round(self.longitude, 6) == round(other.longitude, 6)
        )

    def __repr__(self):
        """
        Machine representation of Point instance.
        """
        return 'Point({0}, {1})'.format(self.latitude, self.longitude)

    __str__ = __repr__
    __str__.__doc__ = 'String representation of Point instance.'

    def distance_to(self, point, unit='km'):
        """
        Calculate distance in miles or kilometers between current and other
        passed point.
        """
        assert isinstance(point, GeoPoint), (
            'Other point should also be a Point instance.'
        )
        if self == point:
            return 0.0
        coefficient = 69.09
        theta = self.longitude - point.longitude
        unit = unit.lower() if unit else None

        distance = math.degrees(math.acos(
            math.sin(self.rad_latitude) * math.sin(point.rad_latitude) +
            math.cos(self.rad_latitude) * math.cos(point.rad_latitude) *
            math.cos(math.radians(theta))
        )) * coefficient

        if unit == 'km':
            return utils.mi_to_km(distance)

        return distance

    @property
    def rad_latitude(self):
        """
        Lazy conversion degrees latitude to radians.
        """
        if self._rad_latitude is None:
            self._rad_latitude = math.radians(self.latitude)
        return self._rad_latitude

    @property
    def rad_longitude(self):
        """
        Lazy conversion degrees longitude to radians.
        """
        if self._rad_longitude is None:
            self._rad_longitude = math.radians(self.longitude)
        return self._rad_longitude
