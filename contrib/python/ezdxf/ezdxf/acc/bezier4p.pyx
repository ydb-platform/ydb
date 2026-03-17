# cython: language_level=3
# Copyright (c) 2020-2024 Manfred Moitzi
# License: MIT License
from typing import TYPE_CHECKING, Sequence, Iterable, Iterator
import cython
import warnings
from .vector cimport (
    Vec3,
    Vec2,
    isclose,
    v3_add,
    v3_mul,
    v3_dist,
    v3_lerp,
    v3_from_angle,
    normalize_rad_angle,
)
from .matrix44 cimport Matrix44
from libc.math cimport ceil, tan, M_PI
from .construct import arc_angle_span_deg

if TYPE_CHECKING:
    from ezdxf.math import UVec
    from ezdxf.math.ellipse import ConstructionEllipse

__all__ = [
    'Bezier4P', 'cubic_bezier_arc_parameters',
    'cubic_bezier_from_arc', 'cubic_bezier_from_ellipse',
]

cdef extern from "constants.h":
    const double ABS_TOL
    const double REL_TOL
    const double M_TAU

cdef double DEG2RAD = M_PI / 180.0
cdef double RECURSION_LIMIT = 1000


cdef class Bezier4P:
    cdef:
        FastCubicCurve curve    # pyright: ignore
        readonly Vec3 start_point
        Vec3 cp1
        Vec3 cp2
        readonly Vec3 end_point

    def __cinit__(self, defpoints: Sequence[UVec]):
        if not isinstance(defpoints[0], (Vec2, Vec3)):
            warnings.warn(
                DeprecationWarning, 
                "Bezier4P requires defpoints of type Vec2 or Vec3 in the future",
            )
        if len(defpoints) == 4:
            self.start_point = Vec3(defpoints[0])
            self.cp1 = Vec3(defpoints[1])
            self.cp2 = Vec3(defpoints[2])
            self.end_point = Vec3(defpoints[3])

            self.curve = FastCubicCurve(
                self.start_point,
                self.cp1,
                self.cp2,
                self.end_point
            )
        else:
            raise ValueError("Four control points required.")

    @property
    def control_points(self) -> tuple[Vec3, Vec3, Vec3, Vec3]:
        return self.start_point, self.cp1, self.cp2, self.end_point

    def __reduce__(self):
        return Bezier4P, (self.control_points,)

    def point(self, double t) -> Vec3:
        if 0.0 <= t <= 1.0:
            return self.curve.point(t)
        else:
            raise ValueError("t not in range [0 to 1]")

    def tangent(self, double t) -> Vec3:
        if 0.0 <= t <= 1.0:
            return self.curve.tangent(t)
        else:
            raise ValueError("t not in range [0 to 1]")

    def approximate(self, int segments) -> list[Vec3]:
        cdef double delta_t
        cdef int segment
        cdef list points = [self.start_point]

        if segments < 1:
            raise ValueError(segments)
        delta_t = 1.0 / segments
        for segment in range(1, segments):
            points.append(self.curve.point(delta_t * segment))
        points.append(self.end_point)
        return points

    def flattening(self, double distance, int segments = 4) -> list[Vec3]:
        cdef double dt = 1.0 / segments
        cdef double t0 = 0.0, t1
        cdef _Flattening f = _Flattening(self, distance)
        cdef Vec3 start_point = self.start_point
        cdef Vec3 end_point
        while t0 < 1.0:
            t1 = t0 + dt
            if isclose(t1, 1.0, REL_TOL, ABS_TOL):
                end_point = self.end_point
                t1 = 1.0
            else:
                end_point = self.curve.point(t1)
            f.reset_recursion_check()
            f.flatten(start_point, end_point, t0, t1)
            if f.has_recursion_error():
                raise RecursionError(
                    "Bezier4P flattening error, check for very large coordinates"
                )
            t0 = t1
            start_point = end_point
        
        return f.points

    def approximated_length(self, segments: int = 128) -> float:
        cdef double length = 0.0
        cdef bint start_flag = 0
        cdef Vec3 prev_point, point

        for point in self.approximate(segments):
            if start_flag:
                length += v3_dist(prev_point, point)
            else:
                start_flag = 1
            prev_point = point
        return length

    def reverse(self) -> Bezier4P:
        return Bezier4P((self.end_point, self.cp2, self.cp1, self.start_point))

    def transform(self, Matrix44 m) -> Bezier4P:
        return Bezier4P(tuple(m.transform_vertices(self.control_points)))


cdef class _Flattening:
    cdef FastCubicCurve curve  # pyright: ignore
    cdef double distance
    cdef list points
    cdef int _recursion_level
    cdef int _recursion_error

    def __cinit__(self, Bezier4P curve, double distance):
        self.curve = curve.curve
        self.distance = distance
        self.points = [curve.start_point]
        self._recursion_level = 0
        self._recursion_error = 0

    cdef has_recursion_error(self):
        return self._recursion_error

    cdef reset_recursion_check(self):
        self._recursion_level = 0
        self._recursion_error = 0

    cdef flatten(
        self,
        Vec3 start_point,
        Vec3 end_point,
        double start_t,
        double end_t
    ):
        # Keep in sync with CPython implementation: ezdxf/math/_bezier4p.py
        # Test suite: 630a
        if self._recursion_level > RECURSION_LIMIT:
            self._recursion_error = 1
            return
        self._recursion_level += 1
        cdef double mid_t = (start_t + end_t) * 0.5
        cdef Vec3 mid_point = self.curve.point(mid_t)
        cdef double d = v3_dist(mid_point, v3_lerp(start_point, end_point, 0.5))
        if d < self.distance:
            self.points.append(end_point)
        else:
            self.flatten(start_point, mid_point, start_t, mid_t)
            self.flatten(mid_point, end_point, mid_t, end_t)
        self._recursion_level -= 1

cdef double DEFAULT_TANGENT_FACTOR = 4.0 / 3.0  # 1.333333333333333333
cdef double OPTIMIZED_TANGENT_FACTOR = 1.3324407374108935
cdef double TANGENT_FACTOR = DEFAULT_TANGENT_FACTOR

@cython.cdivision(True)
def cubic_bezier_arc_parameters(
    double start_angle, 
    double end_angle,
    int segments = 1,
) -> Iterator[tuple[Vec3, Vec3, Vec3, Vec3]]:
    if segments < 1:
        raise ValueError('Invalid argument segments (>= 1).')
    cdef double delta_angle = end_angle - start_angle
    cdef int arc_count
    if delta_angle > 0:
        arc_count = <int> ceil(delta_angle / M_PI * 2.0)
        if segments > arc_count:
            arc_count = segments
    else:
        raise ValueError('Delta angle from start- to end angle has to be > 0.')

    cdef double segment_angle = delta_angle / arc_count
    cdef double tangent_length = TANGENT_FACTOR * tan(segment_angle / 4.0)
    cdef double angle = start_angle
    cdef Vec3 start_point, end_point, cp1, cp2
    end_point = v3_from_angle(angle, 1.0)

    for _ in range(arc_count):
        start_point = end_point
        angle += segment_angle
        end_point = v3_from_angle(angle, 1.0)
        cp1 = Vec3()
        cp1.x = start_point.x - start_point.y * tangent_length
        cp1.y = start_point.y + start_point.x * tangent_length
        cp2 = Vec3()
        cp2.x = end_point.x + end_point.y * tangent_length
        cp2.y = end_point.y - end_point.x * tangent_length
        yield start_point, cp1, cp2, end_point

def cubic_bezier_from_arc(
    center = (0, 0), 
    double radius = 1.0, 
    double start_angle = 0.0,
    double end_angle = 360.0, 
    int segments = 1
) -> Iterable[Bezier4P]:
    cdef Vec3 center_ = Vec3(center)
    cdef Vec3 tmp
    cdef list res
    cdef int i
    cdef double angle_span = arc_angle_span_deg(start_angle, end_angle)
    if abs(angle_span) < 1e-9:
        return

    cdef double s = start_angle
    start_angle = (s * DEG2RAD) % M_TAU
    end_angle = (s + angle_span) * DEG2RAD
    while start_angle > end_angle:
        end_angle += M_TAU

    for control_points in cubic_bezier_arc_parameters(start_angle, end_angle, segments):
        res = []
        for i in range(4):
            tmp = <Vec3> control_points[i]
            res.append(v3_add(center_, v3_mul(tmp, radius)))
        yield Bezier4P(res)

def cubic_bezier_from_ellipse(
    ellipse: ConstructionEllipse,
    int segments = 1
) -> Iterator[Bezier4P]:
    cdef double param_span = ellipse.param_span
    if abs(param_span) < 1e-9:
        return

    cdef double start_angle = normalize_rad_angle(ellipse.start_param)
    cdef double end_angle = start_angle + param_span

    while start_angle > end_angle:
        end_angle += M_TAU

    cdef Vec3 center = Vec3(ellipse.center)
    cdef Vec3 x_axis = Vec3(ellipse.major_axis)
    cdef Vec3 y_axis = Vec3(ellipse.minor_axis)
    cdef Vec3 cp
    cdef Vec3 c_res
    cdef list res
    for control_points in cubic_bezier_arc_parameters(start_angle, end_angle, segments):
        res = list()
        for i in range(4):
            cp = <Vec3> control_points[i]
            c_res = v3_add_3(center, v3_mul(x_axis, cp.x), v3_mul(y_axis, cp.y))
            res.append(c_res)
        yield Bezier4P(res)


cdef Vec3 v3_add_3(Vec3 a, Vec3 b, Vec3 c):
    cdef Vec3 result = Vec3()

    result.x = a.x + b.x + c.x
    result.y = a.y + b.y + c.y
    result.z = a.z + b.z + c.z

    return result


cdef class FastCubicCurve:
    cdef:
        double[3] offset
        double[3] p1
        double[3] p2
        double[3] p3

    def __cinit__(self, Vec3 p0, Vec3 p1, Vec3 p2, Vec3 p3):
        cdef:
            double x = p0.x
            double y = p0.y
            double z = p0.z

        self.offset[0] = x
        self.offset[1] = y
        self.offset[2] = z

        # 1st control point (p0) is always (0, 0, 0)
        self.p1[0] = p1.x - x
        self.p1[1] = p1.y - y
        self.p1[2] = p1.z - z

        self.p2[0] = p2.x - x
        self.p2[1] = p2.y - y
        self.p2[2] = p2.z - z

        self.p3[0] = p3.x - x
        self.p3[1] = p3.y - y
        self.p3[2] = p3.z - z
        

    cdef Vec3 point(self, double t):
        # 1st control point (p0) is always (0, 0, 0)
        # => p0 * a is always (0, 0, 0)
        cdef:
            Vec3 result = Vec3()
            double t2 = t * t
            double _1_minus_t = 1.0 - t
            # a = (1 - t) ** 3
            double b = 3.0 * _1_minus_t * _1_minus_t * t
            double c = 3.0 * _1_minus_t * t2
            double d = t2 * t
            
        iadd_mul(result, self.p1, b)
        iadd_mul(result, self.p2, c)
        iadd_mul(result, self.p3, d)

        # add offset at last - it is maybe very large
        result.x += self.offset[0]
        result.y += self.offset[1]
        result.z += self.offset[2]

        return result


    cdef Vec3 tangent(self, double t):
        # tangent vector is independent from offset location!
        cdef:
            Vec3 result = Vec3()
            double t2 = t * t
            # a = -3 * (1 - t) ** 2
            double b = 3.0 * (1.0 - 4.0 * t + 3.0 * t2)
            double c = 3.0 * t * (2.0 - 3.0 * t)
            double d = 3.0 * t2

        iadd_mul(result, self.p1, b)
        iadd_mul(result, self.p2, c)
        iadd_mul(result, self.p3, d)

        return result


cdef void iadd_mul(Vec3 a, double[3] b, double c):
    a.x += b[0] * c
    a.y += b[1] * c
    a.z += b[2] * c

