# cython: language_level=3
# Copyright (c) 2020-2024, Manfred Moitzi
# License: MIT License
from typing import Iterable, TYPE_CHECKING, Sequence, Optional
from libc.math cimport fabs, M_PI, M_PI_2, M_PI_4, M_E, sin, tan, pow, atan, log
from .vector cimport (
    isclose, 
    Vec2, 
    v2_isclose, 
    Vec3, 
    v3_sub, 
    v3_add, 
    v3_mul, 
    v3_normalize, 
    v3_cross, 
    v3_magnitude_sqr,
    v3_isclose,
)

import cython

if TYPE_CHECKING:
    from ezdxf.math import UVec

cdef extern from "constants.h":
    const double ABS_TOL
    const double REL_TOL
    const double M_TAU

cdef double RAD_ABS_TOL = 1e-15
cdef double DEG_ABS_TOL = 1e-13
cdef double TOLERANCE = 1e-10

def has_clockwise_orientation(vertices: Iterable[UVec]) -> bool:
    """ Returns True if 2D `vertices` have clockwise orientation. Ignores
    z-axis of all vertices.

    Args:
        vertices: iterable of :class:`Vec2` compatible objects

    Raises:
        ValueError: less than 3 vertices

    """
    cdef list _vertices = [Vec2(v) for v in vertices]
    if len(_vertices) < 3:
        raise ValueError('At least 3 vertices required.')

    cdef Vec2 p1 = <Vec2> _vertices[0]
    cdef Vec2 p2 = <Vec2> _vertices[-1]
    cdef double s = 0.0
    cdef Py_ssize_t index

    # Using the same tolerance as the Python implementation:
    if not v2_isclose(p1, p2, REL_TOL, ABS_TOL):
        _vertices.append(p1)

    for index in range(1, len(_vertices)):
        p2 = <Vec2> _vertices[index]
        s += (p2.x - p1.x) * (p2.y + p1.y)
        p1 = p2
    return s > 0.0

def intersection_line_line_2d(
        line1: Sequence[Vec2],
        line2: Sequence[Vec2],
        bint virtual=True,
        double abs_tol=TOLERANCE) -> Optional[Vec2]:
    """
    Compute the intersection of two lines in the xy-plane.

    Args:
        line1: start- and end point of first line to test
            e.g. ((x1, y1), (x2, y2)).
        line2: start- and end point of second line to test
            e.g. ((x3, y3), (x4, y4)).
        virtual: ``True`` returns any intersection point, ``False`` returns
            only real intersection points.
        abs_tol: tolerance for intersection test.

    Returns:
        ``None`` if there is no intersection point (parallel lines) or
        intersection point as :class:`Vec2`

    """
    # Algorithm based on: http://paulbourke.net/geometry/pointlineplane/
    # chapter: Intersection point of two line segments in 2 dimensions
    cdef Vec2 s1, s2, c1, c2, res
    cdef double s1x, s1y, s2x, s2y, c1x, c1y, c2x, c2y, den, us, uc
    cdef double lwr = 0.0, upr = 1.0

    s1 = line1[0]
    s2 = line1[1]
    c1 = line2[0]
    c2 = line2[1]

    s1x = s1.x
    s1y = s1.y
    s2x = s2.x
    s2y = s2.y
    c1x = c1.x
    c1y = c1.y
    c2x = c2.x
    c2y = c2.y

    den = (c2y - c1y) * (s2x - s1x) - (c2x - c1x) * (s2y - s1y)

    if fabs(den) <= abs_tol:
        return None


    # den near zero is checked by if-statement above:
    with cython.cdivision(True):
        us = ((c2x - c1x) * (s1y - c1y) - (c2y - c1y) * (s1x - c1x)) / den

    res = Vec2(s1x + us * (s2x - s1x), s1y + us * (s2y - s1y))
    if virtual:
        return res

    # 0 = intersection point is the start point of the line
    # 1 = intersection point is the end point of the line
    # otherwise: linear interpolation
    if lwr <= us <= upr:  # intersection point is on the subject line
        with cython.cdivision(True):
            uc = ((s2x - s1x) * (s1y - c1y) - (s2y - s1y) * (s1x - c1x)) / den
        if lwr <= uc <= upr:  # intersection point is on the clipping line
            return res
    return None

cdef double _determinant(Vec3 v1, Vec3 v2, Vec3 v3):
    return v1.x * v2.y * v3.z + v1.y * v2.z * v3.x + \
           v1.z * v2.x * v3.y - v1.z * v2.y * v3.x - \
           v1.x * v2.z * v3.y - v1.y * v2.x * v3.z

def intersection_ray_ray_3d(
        ray1: tuple[Vec3, Vec3],
        ray2: tuple[Vec3, Vec3],
        double abs_tol=TOLERANCE
    ) -> Sequence[Vec3]:
    """
    Calculate intersection of two 3D rays, returns a 0-tuple for parallel rays,
    a 1-tuple for intersecting rays and a 2-tuple for not intersecting and not
    parallel rays with points of closest approach on each ray.

    Args:
        ray1: first ray as tuple of two points as Vec3() objects
        ray2: second ray as tuple of two points as Vec3() objects
        abs_tol: absolute tolerance for comparisons

    """
    # source: http://www.realtimerendering.com/intersections.html#I304
    cdef:
        Vec3 o2_o1
        double det1, det2
        Vec3 o1 = Vec3(ray1[0])
        Vec3 p1 = Vec3(ray1[1])
        Vec3 o2 = Vec3(ray2[0])
        Vec3 p2 = Vec3(ray2[1])
        Vec3 d1 = v3_normalize(v3_sub(p1, o1), 1.0)
        Vec3 d2 = v3_normalize(v3_sub(p2, o2), 1.0)
        Vec3 d1xd2 = v3_cross(d1, d2)
        double denominator = v3_magnitude_sqr(d1xd2)

    if denominator <= abs_tol:
        # ray1 is parallel to ray2
        return tuple()
    else:
        o2_o1 = v3_sub(o2, o1)
        det1 = _determinant(o2_o1, d2, d1xd2)
        det2 = _determinant(o2_o1, d1, d1xd2)
        with cython.cdivision(True):  # denominator check is already done
            p1 = v3_add(o1, v3_mul(d1, (det1 / denominator)))
            p2 = v3_add(o2, v3_mul(d2, (det2 / denominator)))

        if v3_isclose(p1, p2, abs_tol, abs_tol):
            # ray1 and ray2 have an intersection point
            return p1,
        else:
            # ray1 and ray2 do not have an intersection point,
            # p1 and p2 are the points of closest approach on each ray
            return p1, p2

def arc_angle_span_deg(double start, double end) -> float:
    if isclose(start, end, REL_TOL, DEG_ABS_TOL):
        return 0.0

    start %= 360.0
    if isclose(start, end % 360.0, REL_TOL, DEG_ABS_TOL):
        return 360.0

    if not isclose(end, 360.0, REL_TOL, DEG_ABS_TOL):
        end %= 360.0

    if end < start:
        end += 360.0
    return end - start

def arc_angle_span_rad(double start, double end) -> float:
    if isclose(start, end, REL_TOL, RAD_ABS_TOL):
        return 0.0

    start %= M_TAU
    if isclose(start, end % M_TAU, REL_TOL, RAD_ABS_TOL):
        return M_TAU

    if not isclose(end, M_TAU, REL_TOL, RAD_ABS_TOL):
        end %= M_TAU

    if end < start:
        end += M_TAU
    return end - start


def is_point_in_polygon_2d(
    point: Vec2, polygon: list[Vec2], double abs_tol=TOLERANCE
) -> int:
    """
    Test if `point` is inside `polygon`.  Returns +1 for inside, 0 for on the 
    boundary and  -1 for outside.

    Supports convex and concave polygons with clockwise or counter-clockwise oriented
    polygon vertices.  Does not raise an exception for degenerated polygons.


    Args:
        point: 2D point to test as :class:`Vec2`
        polygon: list of 2D points as :class:`Vec2`
        abs_tol: tolerance for distance check

    Returns:
        +1 for inside, 0 for on the boundary, -1 for outside

    """
    # Source: http://www.faqs.org/faqs/graphics/algorithms-faq/
    # Subject 2.03: How do I find if a point lies within a polygon?
    # Numpy version was just 10x faster, this version is 23x faster than the Python 
    # version!
    cdef double  a, b, c, d, x, y, x1, y1, x2, y2
    cdef list vertices = polygon
    cdef Vec2 p1, p2
    cdef int size, last, i
    cdef bint inside = 0

    size = len(vertices)
    if size < 3:  # empty polygon
        return -1
    last = size - 1
    p1 = <Vec2> vertices[0]
    p2 = <Vec2> vertices[last]

    if v2_isclose(p1, p2, REL_TOL, ABS_TOL):  # open polygon
        size -= 1
        last -= 1
    if size < 3:
        return -1

    x = point.x
    y = point.y
    p1 = <Vec2> vertices[last]
    x1 = p1.x
    y1 = p1.y

    for i in range(size):
        p2 = <Vec2> vertices[i]
        x2 = p2.x
        y2 = p2.y

        # is point on polygon boundary line:
        # is point in x-range of line
        a, b = (x2, x1) if x2 < x1 else (x1, x2)
        if a <= x <= b:
            # is point in y-range of line
            c, d = (y2, y1) if y2 < y1 else (y1, y2)
            if (c <= y <= d) and fabs(
                (y2 - y1) * x - (x2 - x1) * y + (x2 * y1 - y2 * x1)
            ) <= abs_tol:
                return 0  # on boundary line
        if ((y1 <= y < y2) or (y2 <= y < y1)) and (
            x < (x2 - x1) * (y - y1) / (y2 - y1) + x1
        ):
            inside = not inside
        x1 = x2
        y1 = y2
    if inside:
        return 1  # inside polygon
    else:
        return -1  # outside polygon

cdef double WGS84_SEMI_MAJOR_AXIS = 6378137
cdef double WGS84_SEMI_MINOR_AXIS = 6356752.3142
cdef double WGS84_ELLIPSOID_ECCENTRIC = 0.08181919092890624
cdef double RADIANS = M_PI / 180.0
cdef double DEGREES = 180.0 / M_PI


def gps_to_world_mercator(double longitude, double latitude) -> tuple[float, float]:
    """Transform GPS (long/lat) to World Mercator.
    
    Transform WGS84 `EPSG:4326 <https://epsg.io/4326>`_ location given as
    latitude and longitude in decimal degrees as used by GPS into World Mercator
    cartesian 2D coordinates in meters `EPSG:3395 <https://epsg.io/3395>`_.

    Args:
        longitude: represents the longitude value (East-West) in decimal degrees 
        latitude: represents the latitude value (North-South) in decimal degrees.

    """
    # From: https://epsg.io/4326
    # EPSG:4326 WGS84 - World Geodetic System 1984, used in GPS
    # To: https://epsg.io/3395
    # EPSG:3395 - World Mercator
    # Source: https://gis.stackexchange.com/questions/259121/transformation-functions-for-epsg3395-projection-vs-epsg3857
    longitude = longitude * RADIANS  # east
    latitude = latitude * RADIANS  # north
    cdef double e_sin_lat = sin(latitude) * WGS84_ELLIPSOID_ECCENTRIC
    cdef double c = pow(
        (1.0 - e_sin_lat) / (1.0 + e_sin_lat), WGS84_ELLIPSOID_ECCENTRIC / 2.0
    )  # 7-7 p.44
    y = WGS84_SEMI_MAJOR_AXIS * log(tan(M_PI_4 + latitude / 2.0) * c)  # 7-7 p.44
    x = WGS84_SEMI_MAJOR_AXIS * longitude
    return x, y


def world_mercator_to_gps(double x, double y, double tol = 1e-6) -> tuple[float, float]:
    """Transform World Mercator to GPS.

    Transform WGS84 World Mercator `EPSG:3395 <https://epsg.io/3395>`_
    location given as cartesian 2D coordinates x, y in meters into WGS84 decimal
    degrees as longitude and latitude `EPSG:4326 <https://epsg.io/4326>`_ as
    used by GPS.

    Args:
        x: coordinate WGS84 World Mercator
        y: coordinate WGS84 World Mercator
        tol: accuracy for latitude calculation

    """
    # From: https://epsg.io/3395
    # EPSG:3395 - World Mercator
    # To: https://epsg.io/4326
    # EPSG:4326 WGS84 - World Geodetic System 1984, used in GPS
    # Source: Map Projections - A Working Manual
    # https://pubs.usgs.gov/pp/1395/report.pdf
    cdef double eccentric_2 = WGS84_ELLIPSOID_ECCENTRIC / 2.0
    cdef double t = pow(M_E, (-y / WGS84_SEMI_MAJOR_AXIS))  # 7-10 p.44
    cdef double e_sin_lat, latitude, latitude_prev

    latitude_prev = M_PI_2 - 2.0 * atan(t)  # 7-11 p.45
    while True:
        e_sin_lat = sin(latitude_prev) * WGS84_ELLIPSOID_ECCENTRIC
        latitude = M_PI_2 - 2.0 * atan(
            t * pow(((1.0 - e_sin_lat) / (1.0 + e_sin_lat)), eccentric_2)
        )  # 7-9 p.44
        if fabs(latitude - latitude_prev) < tol:
            break
        latitude_prev = latitude

    longitude = x / WGS84_SEMI_MAJOR_AXIS  # 7-12 p.45
    return longitude * DEGREES, latitude * DEGREES