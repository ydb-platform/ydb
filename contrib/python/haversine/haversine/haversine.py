from enum import Enum
from math import pi
from typing import Union, Tuple
import math


# mean earth radius - https://en.wikipedia.org/wiki/Earth_radius#Mean_radius
_AVG_EARTH_RADIUS_KM = 6371.0088


class Unit(str, Enum):
    """
    Enumeration of supported units.
    The full list can be checked by iterating over the class; e.g.
    the expression `tuple(Unit)`.
    """

    KILOMETERS = 'km'
    METERS = 'm'
    MILES = 'mi'
    NAUTICAL_MILES = 'nmi'
    FEET = 'ft'
    INCHES = 'in'
    RADIANS = 'rad'
    DEGREES = 'deg'


class Direction(float, Enum):
    """
    Enumeration of supported directions.
    The full list can be checked by iterating over the class; e.g.
    the expression `tuple(Direction)`.
    Angles expressed in radians.
    """

    NORTH = 0.0
    NORTHEAST = pi * 0.25
    EAST = pi * 0.5
    SOUTHEAST = pi * 0.75
    SOUTH = pi
    SOUTHWEST = pi * 1.25
    WEST = pi * 1.5
    NORTHWEST = pi * 1.75


# Unit values taken from http://www.unitconversion.org/unit_converter/length.html
_CONVERSIONS = {
    Unit.KILOMETERS:       1.0,
    Unit.METERS:           1000.0,
    Unit.MILES:            0.621371192,
    Unit.NAUTICAL_MILES:   0.539956803,
    Unit.FEET:             3280.839895013,
    Unit.INCHES:           39370.078740158,
    Unit.RADIANS:          1/_AVG_EARTH_RADIUS_KM,
    Unit.DEGREES:          (1/_AVG_EARTH_RADIUS_KM)*(180.0/pi)
}


def get_avg_earth_radius(unit):
    return _AVG_EARTH_RADIUS_KM * _CONVERSIONS[unit]


def _normalize(lat: float, lon: float) -> Tuple[float, float]:
    """
    Normalize point to [-90, 90] latitude and [-180, 180] longitude.
    """
    lat = (lat + 90) % 360 - 90
    if lat > 90:
        lat = 180 - lat
        lon += 180
    lon = (lon + 180) % 360 - 180
    return lat, lon


def _normalize_vector(lat: "numpy.ndarray", lon: "numpy.ndarray") -> Tuple["numpy.ndarray", "numpy.ndarray"]:
    """
    Normalize points to [-90, 90] latitude and [-180, 180] longitude.
    """
    lat = (lat + 90) % 360 - 90
    lon = (lon + 180) % 360 - 180
    wrap = lat > 90
    if numpy.any(wrap):
        lat[wrap] = 180 - lat[wrap]
        lon[wrap] = lon[wrap] % 360 - 180
    return lat, lon


def _ensure_lat_lon(lat: float, lon: float):
    """
    Ensure that the given latitude and longitude have proper values. An exception is raised if they are not.
    """
    if lat < -90 or lat > 90:
        raise ValueError(f"Latitude {lat} is out of range [-90, 90]")
    if lon < -180 or lon > 180:
        raise ValueError(f"Longitude {lon} is out of range [-180, 180]")


def _ensure_lat_lon_vector(lat: "numpy.ndarray", lon: "numpy.ndarray"):
    """
    Ensure that the given latitude and longitude have proper values. An exception is raised if they are not.
    """
    if numpy.abs(lat).max() > 90:
        raise ValueError("Latitude(s) out of range [-90, 90]")
    if numpy.abs(lon).max() > 180:
        raise ValueError("Longitude(s) out of range [-180, 180]")


def _explode_args(f):
    return lambda ops: f(**ops.__dict__)


@_explode_args
def _create_haversine_kernel(*, asin=None, arcsin=None, cos, radians, sin, sqrt, **_):
    asin = asin or arcsin

    def _haversine_kernel(lat1, lng1, lat2, lng2):
        """
        Compute the haversine distance on unit sphere.  Inputs are in degrees,
        either scalars (with ops==math) or arrays (with ops==numpy).
        """
        lat1 = radians(lat1)
        lng1 = radians(lng1)
        lat2 = radians(lat2)
        lng2 = radians(lng2)
        lat = lat2 - lat1
        lng = lng2 - lng1
        d = (sin(lat * 0.5) ** 2
             + cos(lat1) * cos(lat2) * sin(lng * 0.5) ** 2)
        # Note: 2 * atan2(sqrt(d), sqrt(1-d)) is more accurate at
        # large distance (d is close to 1), but also slower.
        return 2 * asin(sqrt(d))
    return _haversine_kernel



@_explode_args
def _create_inverse_haversine_kernel(*, asin=None, arcsin=None, atan2=None, arctan2=None, cos, degrees, radians, sin, sqrt, **_):
    asin = asin or arcsin
    atan2 = atan2 or arctan2

    def _inverse_haversine_kernel(lat, lng, direction, d):
        """
        Compute the inverse haversine on unit sphere.  lat/lng are in degrees,
        direction in radians; all inputs are either scalars (with ops==math) or
        arrays (with ops==numpy).
        """
        lat = radians(lat)
        lng = radians(lng)
        cos_d, sin_d = cos(d), sin(d)
        cos_lat, sin_lat = cos(lat), sin(lat)
        sin_d_cos_lat = sin_d * cos_lat
        return_lat = asin(cos_d * sin_lat + sin_d_cos_lat * cos(direction))
        return_lng = lng + atan2(sin(direction) * sin_d_cos_lat,
                                 cos_d - sin_lat * sin(return_lat))
        return degrees(return_lat), degrees(return_lng)
    return _inverse_haversine_kernel


_haversine_kernel = _create_haversine_kernel(math)
_inverse_haversine_kernel = _create_inverse_haversine_kernel(math)

try:
    import numpy
    has_numpy = True
    _haversine_kernel_vector = _create_haversine_kernel(numpy)
    _inverse_haversine_kernel_vector = _create_inverse_haversine_kernel(numpy)
except ModuleNotFoundError:
    # Import error will be reported in haversine_vector() / inverse_haversine_vector()
    has_numpy = False

try:
    import numba # type: ignore
    if has_numpy:
        _haversine_kernel_vector = numba.vectorize(fastmath=True)(_haversine_kernel_vector)
        # Tuple output is not supported for numba.vectorize. Just jit the numpy version.
        _inverse_haversine_kernel_vector = numba.njit(fastmath=True)(_inverse_haversine_kernel_vector)
    _haversine_kernel = numba.njit(_haversine_kernel)
    _inverse_haversine_kernel = numba.njit(_inverse_haversine_kernel)
except ModuleNotFoundError:
    pass


def haversine(point1, point2, unit=Unit.KILOMETERS, normalize=False, check=True):
    """ Calculate the great-circle distance between two points on the Earth surface.

    Takes two 2-tuples, containing the latitude and longitude of each point in decimal degrees,
    and, optionally, a unit of length.

    :param point1: first point; tuple of (latitude, longitude) in decimal degrees
    :param point2: second point; tuple of (latitude, longitude) in decimal degrees
    :param unit: a member of haversine.Unit, or, equivalently, a string containing the
                 initials of its corresponding unit of measurement (i.e. miles = mi)
                 default 'km' (kilometers).
    :param normalize: if True, normalize the points to [-90, 90] latitude and [-180, 180] longitude.
    :param check: if True, check that points are normalized.

    Example: ``haversine((45.7597, 4.8422), (48.8567, 2.3508), unit=Unit.METERS)``

    Precondition: ``unit`` is a supported unit (supported units are listed in the `Unit` enum)

    :return: the distance between the two points in the requested unit, as a float.

    The default returned unit is kilometers. The default unit can be changed by
    setting the unit parameter to a member of ``haversine.Unit``
    (e.g. ``haversine.Unit.INCHES``), or, equivalently, to a string containing the
    corresponding abbreviation (e.g. 'in'). All available units can be found in the ``Unit`` enum.
    """

    # unpack latitude/longitude
    lat1, lng1 = point1
    lat2, lng2 = point2

    # normalize points or ensure they are proper lat/lon, i.e., in [-90, 90] and [-180, 180]
    if normalize:
        lat1, lng1 = _normalize(lat1, lng1)
        lat2, lng2 = _normalize(lat2, lng2)
    elif check:
        _ensure_lat_lon(lat1, lng1)
        _ensure_lat_lon(lat2, lng2)

    return get_avg_earth_radius(unit) * _haversine_kernel(lat1, lng1, lat2, lng2)


def haversine_vector(array1, array2, unit=Unit.KILOMETERS, comb=False, normalize=False, check=True):
    '''
    The exact same function as "haversine", except that this
    version replaces math functions with numpy functions.
    This may make it slightly slower for computing the haversine
    distance between two points, but is much faster for computing
    the distance between two vectors of points due to vectorization.
    '''
    if not has_numpy:
        raise RuntimeError('Error, unable to import Numpy, '
                           'consider using haversine instead of haversine_vector.')

    # ensure arrays are numpy ndarrays
    if not isinstance(array1, numpy.ndarray):
        array1 = numpy.array(array1)
    if not isinstance(array2, numpy.ndarray):
        array2 = numpy.array(array2)

    # ensure will be able to iterate over rows by adding dimension if needed
    if array1.ndim == 1:
        array1 = numpy.expand_dims(array1, 0)
    if array2.ndim == 1:
        array2 = numpy.expand_dims(array2, 0)

    # Asserts that both arrays have same dimensions if not in combination mode
    if not comb:
        if array1.shape != array2.shape:
            raise IndexError(
                "When not in combination mode, arrays must be of same size. If mode is required, use comb=True as argument.")

    # unpack latitude/longitude
    lat1, lng1 = array1[:, 0], array1[:, 1]
    lat2, lng2 = array2[:, 0], array2[:, 1]

    # normalize points or ensure they are proper lat/lon, i.e., in [-90, 90] and [-180, 180]
    if normalize:
        lat1, lng1 = _normalize_vector(lat1, lng1)
        lat2, lng2 = _normalize_vector(lat2, lng2)
    elif check:
        _ensure_lat_lon_vector(lat1, lng1)
        _ensure_lat_lon_vector(lat2, lng2)

    # If in combination mode, turn coordinates of array1 into column vectors for broadcasting
    if comb:
        lat1 = numpy.expand_dims(lat1, axis=0)
        lng1 = numpy.expand_dims(lng1, axis=0)
        lat2 = numpy.expand_dims(lat2, axis=1)
        lng2 = numpy.expand_dims(lng2, axis=1)

    return get_avg_earth_radius(unit) * _haversine_kernel_vector(lat1, lng1, lat2, lng2)


def inverse_haversine(point, distance, direction: Union[Direction, float], unit=Unit.KILOMETERS, normalize_output=False):
    lat, lng = point
    r = get_avg_earth_radius(unit)
    outLat, outLng = _inverse_haversine_kernel(lat, lng, direction, distance / r)

    if normalize_output:
        return _normalize(outLat, outLng)
    else:
        return (outLat, outLng)


def inverse_haversine_vector(array, distance, direction, unit=Unit.KILOMETERS, normalize_output=False): # -> Tuple["numpy.ndarray", "numpy.ndarray"]:
    if not has_numpy:
        raise RuntimeError('Error, unable to import Numpy, '
                           'consider using inverse_haversine instead of inverse_haversine_vector.')

    # ensure arrays are numpy ndarrays
    array, distance, direction = map(numpy.asarray, (array, distance, direction))

    # ensure will be able to iterate over rows by adding dimension if needed
    if array.ndim == 1:
        array = numpy.expand_dims(array, 0)

    # Asserts that arrays are correctly sized
    if array.ndim != 2 or array.shape[1] != 2 or array.shape[0] != len(distance) or array.shape[0] != len(direction):
        raise IndexError("Arrays must be of same size.")

    # unpack latitude/longitude
    lat, lng = array[:, 0], array[:, 1]

    r = get_avg_earth_radius(unit)
    outLatArray, outLngArray = _inverse_haversine_kernel_vector(lat, lng, direction, distance/r)

    if normalize_output:
        return _normalize_vector(outLatArray, outLngArray)

    return (outLatArray, outLngArray)