# Copyright (c) 2010-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    Sequence,
    TypeVar,
    Generic,
)
import math

# The pure Python implementation can't import from ._ctypes or ezdxf.math!
from ._vector import Vec3, Vec2
from ._matrix44 import Matrix44
from ._construct import arc_angle_span_deg

if TYPE_CHECKING:
    from ezdxf.math import UVec
    from ezdxf.math.ellipse import ConstructionEllipse

__all__ = [
    "Bezier4P",
    "cubic_bezier_arc_parameters",
    "cubic_bezier_from_arc",
    "cubic_bezier_from_ellipse",
]

T = TypeVar("T", Vec2, Vec3)


class Bezier4P(Generic[T]):
    """Implements an optimized cubic `Bézier curve`_ for exact 4 control points.

    A `Bézier curve`_ is a parametric curve, parameter `t` goes from 0 to 1,
    where 0 is the first control point and 1 is the fourth control point.

    The class supports points of type :class:`Vec2` and :class:`Vec3` as input, the
    class instances are immutable.

    Args:
        defpoints: sequence of definition points as :class:`Vec2` or
            :class:`Vec3` compatible objects.

    """

    __slots__ = ("_control_points", "_offset")

    def __init__(self, defpoints: Sequence[T]):
        if len(defpoints) != 4:
            raise ValueError("Four control points required.")
        point_type = defpoints[0].__class__
        if not point_type.__name__ in ("Vec2", "Vec3"):  # Cython types!!!
            raise TypeError(f"invalid point type: {point_type.__name__}")

        # The start point is the curve offset
        offset: T = defpoints[0]
        self._offset: T = offset
        # moving the curve to the origin reduces floating point errors:
        self._control_points: tuple[T, ...] = tuple(p - offset for p in defpoints)

    @property
    def control_points(self) -> Sequence[T]:
        """Control points as tuple of :class:`Vec3` or :class:`Vec2` objects."""
        # ezdxf optimization: p0 is always (0, 0, 0)
        p0, p1, p2, p3 = self._control_points
        offset = self._offset
        return offset, p1 + offset, p2 + offset, p3 + offset

    def tangent(self, t: float) -> T:
        """Returns direction vector of tangent for location `t` at the
        Bèzier-curve.

        Args:
            t: curve position in the range ``[0, 1]``

        """
        if not (0 <= t <= 1.0):
            raise ValueError("t not in range [0 to 1]")
        return self._get_curve_tangent(t)

    def point(self, t: float) -> T:
        """Returns point for location `t` at the Bèzier-curve.

        Args:
            t: curve position in the range ``[0, 1]``

        """
        if not (0 <= t <= 1.0):
            raise ValueError("t not in range [0 to 1]")
        return self._get_curve_point(t)

    def approximate(self, segments: int) -> Iterator[T]:
        """Approximate `Bézier curve`_ by vertices, yields `segments` + 1
        vertices as ``(x, y[, z])`` tuples.

        Args:
            segments: count of segments for approximation

        """
        if segments < 1:
            raise ValueError(segments)
        delta_t = 1.0 / segments
        cp = self.control_points
        yield cp[0]
        for segment in range(1, segments):
            yield self._get_curve_point(delta_t * segment)
        yield cp[3]

    def flattening(self, distance: float, segments: int = 4) -> Iterator[T]:
        """Adaptive recursive flattening. The argument `segments` is the
        minimum count of approximation segments, if the distance from the center
        of the approximation segment to the curve is bigger than `distance` the
        segment will be subdivided.

        Args:
            distance: maximum distance from the center of the cubic (C3)
                curve to the center of the linear (C1) curve between two
                approximation points to determine if a segment should be
                subdivided.
            segments: minimum segment count

        """
        stack: list[tuple[float, T]] = []
        dt: float = 1.0 / segments
        t0: float = 0.0
        t1: float
        cp = self.control_points
        start_point: T = cp[0]
        end_point: T

        yield start_point
        while t0 < 1.0:
            t1 = t0 + dt
            if math.isclose(t1, 1.0):
                end_point = cp[3]
                t1 = 1.0
            else:
                end_point = self._get_curve_point(t1)

            while True:
                mid_t: float = (t0 + t1) * 0.5
                mid_point: T = self._get_curve_point(mid_t)
                chk_point: T = start_point.lerp(end_point)

                d = chk_point.distance(mid_point)
                if d < distance:
                    yield end_point
                    t0 = t1
                    start_point = end_point
                    if stack:
                        t1, end_point = stack.pop()
                    else:
                        break
                else:
                    stack.append((t1, end_point))
                    t1 = mid_t
                    end_point = mid_point

    def _get_curve_point(self, t: float) -> T:
        # 1st control point (p0) is always (0, 0, 0)
        # => p0 * a is always (0, 0, 0)
        # add offset at last - it is maybe very large
        _, p1, p2, p3 = self._control_points
        t2 = t * t
        _1_minus_t = 1.0 - t
        # a = _1_minus_t_square * _1_minus_t
        b = 3.0 * _1_minus_t * _1_minus_t * t
        c = 3.0 * _1_minus_t * t2
        d = t2 * t
        return p1 * b + p2 * c + p3 * d + self._offset

    def _get_curve_tangent(self, t: float) -> T:
        # tangent vector is independent from offset location!
        # 1st control point (p0) is always (0, 0, 0)
        # => p0 * a is always (0, 0, 0)
        _, p1, p2, p3 = self._control_points
        t2 = t * t
        # a = -3.0 * (1.0 - t) ** 2
        b = 3.0 * (1.0 - 4.0 * t + 3.0 * t2)
        c = 3.0 * t * (2.0 - 3.0 * t)
        d = 3.0 * t2
        return p1 * b + p2 * c + p3 * d

    def approximated_length(self, segments: int = 128) -> float:
        """Returns estimated length of Bèzier-curve as approximation by line
        `segments`.
        """
        length = 0.0
        prev_point = None
        for point in self.approximate(segments):
            if prev_point is not None:
                length += prev_point.distance(point)
            prev_point = point
        return length

    def reverse(self) -> Bezier4P[T]:
        """Returns a new Bèzier-curve with reversed control point order."""
        return Bezier4P(list(reversed(self.control_points)))

    def transform(self, m: Matrix44) -> Bezier4P[Vec3]:
        """General transformation interface, returns a new :class:`Bezier4p`
        curve as a 3D curve.

        Args:
             m: 4x4 transformation :class:`Matrix44`

        """
        defpoints = Vec3.generate(self.control_points)
        return Bezier4P(tuple(m.transform_vertices(defpoints)))


def cubic_bezier_from_arc(
    center: UVec = (0, 0, 0),
    radius: float = 1,
    start_angle: float = 0,
    end_angle: float = 360,
    segments: int = 1,
) -> Iterator[Bezier4P[Vec3]]:
    """Returns an approximation for a circular 2D arc by multiple cubic
    Bézier-curves.

    Args:
        center: circle center as :class:`Vec3` compatible object
        radius: circle radius
        start_angle: start angle in degrees
        end_angle: end angle in degrees
        segments: count of Bèzier-curve segments, at least one segment for each
            quarter (90 deg), 1 for as few as possible.

    """
    center_: Vec3 = Vec3(center)
    radius = float(radius)
    angle_span: float = arc_angle_span_deg(start_angle, end_angle)
    if abs(angle_span) < 1e-9:
        return

    s: float = start_angle
    start_angle = math.radians(s) % math.tau
    end_angle = math.radians(s + angle_span)
    while start_angle > end_angle:
        end_angle += math.tau

    for control_points in cubic_bezier_arc_parameters(start_angle, end_angle, segments):
        defpoints = [center_ + (p * radius) for p in control_points]
        yield Bezier4P(defpoints)


PI_2: float = math.pi / 2.0


def cubic_bezier_from_ellipse(
    ellipse: "ConstructionEllipse", segments: int = 1
) -> Iterator[Bezier4P[Vec3]]:
    """Returns an approximation for an elliptic arc by multiple cubic
    Bézier-curves.

    Args:
        ellipse: ellipse parameters as :class:`~ezdxf.math.ConstructionEllipse`
            object
        segments: count of Bèzier-curve segments, at least one segment for each
            quarter (π/2), 1 for as few as possible.

    """
    param_span: float = ellipse.param_span
    if abs(param_span) < 1e-9:
        return
    start_angle: float = ellipse.start_param % math.tau
    end_angle: float = start_angle + param_span
    while start_angle > end_angle:
        end_angle += math.tau

    def transform(points: Iterable[Vec3]) -> Iterator[Vec3]:
        center = Vec3(ellipse.center)
        x_axis: Vec3 = ellipse.major_axis
        y_axis: Vec3 = ellipse.minor_axis
        for p in points:
            yield center + x_axis * p.x + y_axis * p.y

    for defpoints in cubic_bezier_arc_parameters(start_angle, end_angle, segments):
        yield Bezier4P(tuple(transform(defpoints)))


# Circular arc to Bezier curve:
# Source: https://stackoverflow.com/questions/1734745/how-to-create-circle-with-b%C3%A9zier-curves
# Optimization: https://spencermortensen.com/articles/bezier-circle/
# actual c = 0.5522847498307935  = 4.0/3.0*(sqrt(2)-1.0) and max. deviation of ~0.03%
DEFAULT_TANGENT_FACTOR = 4.0 / 3.0  # 1.333333333333333333
# optimal c = 0.551915024494 and max. deviation of ~0.02%
OPTIMIZED_TANGENT_FACTOR = 1.3324407374108935
# Not sure if this is the correct way to apply this optimization,
# so i stick to the original version for now:
TANGENT_FACTOR = DEFAULT_TANGENT_FACTOR


def cubic_bezier_arc_parameters(
    start_angle: float, end_angle: float, segments: int = 1
) -> Iterator[tuple[Vec3, Vec3, Vec3, Vec3]]:
    """Yields cubic Bézier-curve parameters for a circular 2D arc with center
    at (0, 0) and a radius of 1 in the form of [start point, 1. control point,
    2. control point, end point].

    Args:
        start_angle: start angle in radians
        end_angle: end angle in radians (end_angle > start_angle!)
        segments: count of Bèzier-curve segments, at least one segment for each
            quarter (π/2)

    """
    if segments < 1:
        raise ValueError("Invalid argument segments (>= 1).")
    delta_angle: float = end_angle - start_angle
    if delta_angle > 0:
        arc_count = max(math.ceil(delta_angle / math.pi * 2.0), segments)
    else:
        raise ValueError("Delta angle from start- to end angle has to be > 0.")

    segment_angle: float = delta_angle / arc_count
    tangent_length: float = TANGENT_FACTOR * math.tan(segment_angle / 4.0)

    angle: float = start_angle
    end_point: Vec3 = Vec3.from_angle(angle)
    for _ in range(arc_count):
        start_point = end_point
        angle += segment_angle
        end_point = Vec3.from_angle(angle)
        control_point_1 = start_point + (
            -start_point.y * tangent_length,
            start_point.x * tangent_length,
        )
        control_point_2 = end_point + (
            end_point.y * tangent_length,
            -end_point.x * tangent_length,
        )
        yield start_point, control_point_1, control_point_2, end_point
