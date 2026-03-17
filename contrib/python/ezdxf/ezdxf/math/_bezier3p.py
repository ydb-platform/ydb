# Copyright (c) 2021-2024 Manfred Moitzi
# License: MIT License
# pylint: disable=unused-variable
from __future__ import annotations
from typing import (
    Iterator,
    Sequence,
    Optional,
    Generic,
    TypeVar,
)
import math

# The pure Python implementation can't import from ._ctypes or ezdxf.math!
from ._vector import Vec3, Vec2
from ._matrix44 import Matrix44


__all__ = ["Bezier3P"]


def check_if_in_valid_range(t: float) -> None:
    if not 0.0 <= t <= 1.0:
        raise ValueError("t not in range [0 to 1]")


T = TypeVar("T", Vec2, Vec3)


class Bezier3P(Generic[T]):
    """Implements an optimized quadratic `Bézier curve`_ for exact 3 control
    points.

    The class supports points of type :class:`Vec2` and :class:`Vec3` as input, the
    class instances are immutable.

    Args:
        defpoints: sequence of definition points as :class:`Vec2` or
            :class:`Vec3` compatible objects.

    """

    __slots__ = ("_control_points", "_offset")

    def __init__(self, defpoints: Sequence[T]):
        if len(defpoints) != 3:
            raise ValueError("Three control points required.")
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
        _, p1, p2 = self._control_points
        offset = self._offset
        return offset, p1 + offset, p2 + offset

    def tangent(self, t: float) -> T:
        """Returns direction vector of tangent for location `t` at the
        Bèzier-curve.

        Args:
            t: curve position in the range ``[0, 1]``

        """
        check_if_in_valid_range(t)
        return self._get_curve_tangent(t)

    def point(self, t: float) -> T:
        """Returns point for location `t` at the Bèzier-curve.

        Args:
            t: curve position in the range ``[0, 1]``

        """
        check_if_in_valid_range(t)
        return self._get_curve_point(t)

    def approximate(self, segments: int) -> Iterator[T]:
        """Approximate `Bézier curve`_ by vertices, yields `segments` + 1
        vertices as ``(x, y[, z])`` tuples.

        Args:
            segments: count of segments for approximation

        """
        if segments < 1:
            raise ValueError(segments)
        delta_t: float = 1.0 / segments
        cp = self.control_points
        yield cp[0]
        for segment in range(1, segments):
            yield self._get_curve_point(delta_t * segment)
        yield cp[2]

    def approximated_length(self, segments: int = 128) -> float:
        """Returns estimated length of Bèzier-curve as approximation by line
        `segments`.
        """
        length: float = 0.0
        prev_point: Optional[T] = None
        for point in self.approximate(segments):
            if prev_point is not None:
                length += prev_point.distance(point)
            prev_point = point
        return length

    def flattening(self, distance: float, segments: int = 4) -> Iterator[T]:
        """Adaptive recursive flattening. The argument `segments` is the
        minimum count of approximation segments, if the distance from the center
        of the approximation segment to the curve is bigger than `distance` the
        segment will be subdivided.

        Args:
            distance: maximum distance from the center of the quadratic (C2)
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
                end_point = cp[2]
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
        _, p1, p2 = self._control_points
        _1_minus_t = 1.0 - t
        # a = (1 - t) ** 2
        b = 2.0 * t * _1_minus_t
        c = t * t
        # add offset at last - it is maybe very large
        return p1 * b + p2 * c + self._offset

    def _get_curve_tangent(self, t: float) -> T:
        # tangent vector is independent from offset location!
        # 1st control point (p0) is always (0, 0, 0)
        # => p0 * a is always (0, 0, 0)
        _, p1, p2 = self._control_points
        # a = -2 * (1 - t)
        b = 2.0 - 4.0 * t
        c = 2.0 * t
        return p1 * b + p2 * c

    def reverse(self) -> Bezier3P[T]:
        """Returns a new Bèzier-curve with reversed control point order."""
        return Bezier3P(list(reversed(self.control_points)))

    def transform(self, m: Matrix44) -> Bezier3P[Vec3]:
        """General transformation interface, returns a new :class:`Bezier3P`
        curve and it is always a 3D curve.

        Args:
             m: 4x4 transformation :class:`Matrix44`

        """
        defpoints = Vec3.generate(self.control_points)
        return Bezier3P(tuple(m.transform_vertices(defpoints)))
