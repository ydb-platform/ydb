# Copyright (c) 2010-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable
from ezdxf.math import Vec3
from ezdxf.math.bspline import global_bspline_interpolation, BSpline

__all__ = ["EulerSpiral"]


def powers(base: float, count: int) -> list[float]:
    assert count > 2, "requires count > 2"
    values = [1.0, base]
    next_value = base
    for _ in range(count - 2):
        next_value *= base
        values.append(next_value)
    return values


def _params(length: float, segments: int) -> Iterable[float]:
    delta_l = float(length) / float(segments)
    for index in range(0, segments + 1):
        yield delta_l * index


class EulerSpiral:
    """
    This class represents an euler spiral (clothoid) for `curvature` (Radius of
    curvature).

    This is a parametric curve, which always starts at the origin = ``(0, 0)``.

    Args:
        curvature: radius of curvature

    """

    def __init__(self, curvature: float = 1.0):
        curvature = float(curvature)
        self.curvature = curvature  # Radius of curvature
        self.curvature_powers: list[float] = powers(curvature, 19)
        self._cache: dict[float, Vec3] = {}  # coordinates cache

    def radius(self, t: float) -> float:
        """Get radius of circle at distance `t`."""
        if t > 0.0:
            return self.curvature_powers[2] / t
        else:
            return 0.0  # radius = infinite

    def tangent(self, t: float) -> Vec3:
        """Get tangent at distance `t` as :class:`Vec3` object."""
        angle = t ** 2 / (2.0 * self.curvature_powers[2])
        return Vec3.from_angle(angle)

    def distance(self, radius: float) -> float:
        """Get distance L from origin for `radius`."""
        return self.curvature_powers[2] / float(radius)

    def point(self, t: float) -> Vec3:
        """Get point at distance `t` as :class:`Vec3`."""

        def term(length_power, curvature_power, const):
            return t ** length_power / (
                const * self.curvature_powers[curvature_power]
            )

        if t not in self._cache:
            y = (
                term(3, 2, 6.0)
                - term(7, 6, 336.0)
                + term(11, 10, 42240.0)
                - term(15, 14, 9676800.0)
                + term(19, 18, 3530096640.0)
            )
            x = (
                t
                - term(5, 4, 40.0)
                + term(9, 8, 3456.0)
                - term(13, 12, 599040.0)
                + term(17, 16, 175472640.0)
            )
            self._cache[t] = Vec3(x, y)
        return self._cache[t]

    def approximate(self, length: float, segments: int) -> Iterable[Vec3]:
        """Approximate curve of length with line segments.
        Generates segments+1 vertices as :class:`Vec3` objects.

        """
        for t in _params(length, segments):
            yield self.point(t)

    def circle_center(self, t: float) -> Vec3:
        """Get circle center at distance `t`."""
        p = self.point(t)
        r = self.radius(t)
        return p + self.tangent(t).normalize(r).orthogonal()

    def bspline(
        self,
        length: float,
        segments: int = 10,
        degree: int = 3,
        method: str = "uniform",
    ) -> BSpline:
        """Approximate euler spiral as B-spline.

        Args:
            length: length of euler spiral
            segments: count of fit points for B-spline calculation
            degree: degree of BSpline
            method: calculation method for parameter vector t

        Returns:
            :class:`BSpline`

        """
        length = float(length)
        fit_points = list(self.approximate(length, segments=segments))
        derivatives = [
            # Scaling derivatives by chord length (< real length) is suggested
            # by Piegl & Tiller.
            self.tangent(t).normalize(length)
            for t in _params(length, segments)
        ]
        spline = global_bspline_interpolation(
            fit_points, degree, method=method, tangents=derivatives
        )
        return BSpline(
            spline.control_points,
            spline.order,
            # Scale knot values to length:
            [v * length for v in spline.knots()],
        )
