# cython: language_level=3
# Copyright (c) 2021-2024 Manfred Moitzi
# License: MIT License
from typing import TYPE_CHECKING, Sequence
from .vector cimport Vec3, isclose, v3_dist, v3_lerp, v3_add, Vec2
from .matrix44 cimport Matrix44
import warnings
if TYPE_CHECKING:
    from ezdxf.math import UVec

__all__ = ['Bezier3P']

cdef extern from "constants.h":
    const double ABS_TOL
    const double REL_TOL

cdef double RECURSION_LIMIT = 1000


cdef class Bezier3P:
    cdef:
        FastQuadCurve curve  # pyright: ignore
        readonly Vec3 start_point
        Vec3 cp1
        readonly Vec3 end_point

    def __cinit__(self, defpoints: Sequence[UVec]):
        if not isinstance(defpoints[0], (Vec2, Vec3)):
            warnings.warn(
                DeprecationWarning, 
                "Bezier3P requires defpoints of type Vec2 or Vec3 in the future",
            )
        if len(defpoints) == 3:
            self.start_point = Vec3(defpoints[0])
            self.cp1 = Vec3(defpoints[1])
            self.end_point = Vec3(defpoints[2])

            self.curve = FastQuadCurve(
                self.start_point,
                self.cp1,
                self.end_point
            )
        else:
            raise ValueError("Three control points required.")

    @property
    def control_points(self) -> tuple[Vec3, Vec3, Vec3]:
        return self.start_point, self.cp1, self.end_point

    def __reduce__(self):
        return Bezier3P, (self.control_points,)

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
            points.append(self.point(delta_t * segment))
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
                    "Bezier3P flattening error, check for very large coordinates"
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

    def reverse(self) -> Bezier3P:
        return Bezier3P((self.end_point, self.cp1, self.start_point))

    def transform(self, Matrix44 m) -> Bezier3P:
        return Bezier3P(tuple(m.transform_vertices(self.control_points)))


cdef class _Flattening:
    cdef FastQuadCurve curve  # pyright: ignore
    cdef double distance
    cdef list points
    cdef int _recursion_level
    cdef int _recursion_error

    def __cinit__(self, Bezier3P curve, double distance):
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


cdef class FastQuadCurve:
    cdef:
        double[3] offset
        double[3] p1
        double[3] p2

    def __cinit__(self, Vec3 p0, Vec3 p1, Vec3 p2):
        self.offset[0] = p0.x
        self.offset[1] = p0.y
        self.offset[2] = p0.z

        # 1st control point (p0) is always (0, 0, 0)
        self.p1[0] = p1.x - p0.x
        self.p1[1] = p1.y - p0.y
        self.p1[2] = p1.z - p0.z

        self.p2[0] = p2.x - p0.x
        self.p2[1] = p2.y - p0.y
        self.p2[2] = p2.z - p0.z


    cdef Vec3 point(self, double t):
        # 1st control point (p0) is always (0, 0, 0)
        # => p0 * a is always (0, 0, 0)
        cdef:
            Vec3 result = Vec3()
            # double a = (1 - t) ** 2
            double b = 2.0 * t * (1.0 - t)
            double c = t * t

        iadd_mul(result, self.p1, b)
        iadd_mul(result, self.p2, c)

        # add offset at last - it is maybe very large
        result.x += self.offset[0]
        result.y += self.offset[1]
        result.z += self.offset[2]

        return result


    cdef Vec3 tangent(self, double t):
        # tangent vector is independent from offset location!
        cdef:
            Vec3 result = Vec3()
            # double a = -2 * (1 - t)
            double b = 2.0 - 4.0 * t
            double c = 2.0 * t

        iadd_mul(result, self.p1, b)
        iadd_mul(result, self.p2, c)

        return result


cdef void iadd_mul(Vec3 a, double[3] b, double c):
    a.x += b[0] * c
    a.y += b[1] * c
    a.z += b[2] * c
