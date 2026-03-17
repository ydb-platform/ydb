# Copyright (c) 2010-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Sequence, Iterator, Iterable
import math
import numpy as np

from ezdxf.math import Vec2, UVec
from .line import ConstructionRay, ConstructionLine
from .bbox import BoundingBox2d


HALF_PI = math.pi / 2.0

__all__ = ["ConstructionCircle"]


class ConstructionCircle:
    """Construction tool for 2D circles.

    Args:
        center: center point as :class:`Vec2` compatible object
        radius: circle radius > `0`

    """

    def __init__(self, center: UVec, radius: float = 1.0):
        self.center = Vec2(center)
        self.radius = float(radius)
        if self.radius <= 0.0:
            raise ValueError("Radius has to be > 0.")

    def __str__(self) -> str:
        """Returns string representation of circle
        "ConstructionCircle(center, radius)".
        """
        return f"ConstructionCircle({self.center}, {self.radius})"

    @staticmethod
    def from_3p(p1: UVec, p2: UVec, p3: UVec) -> ConstructionCircle:
        """Creates a circle from three points, all points have to be compatible
        to :class:`Vec2` class.
        """
        _p1 = Vec2(p1)
        _p2 = Vec2(p2)
        _p3 = Vec2(p3)
        ray1 = ConstructionRay(_p1, _p2)
        ray2 = ConstructionRay(_p1, _p3)
        center_ray1 = ray1.orthogonal(_p1.lerp(_p2))
        center_ray2 = ray2.orthogonal(_p1.lerp(_p3))
        center = center_ray1.intersect(center_ray2)
        return ConstructionCircle(center, center.distance(_p1))

    @property
    def bounding_box(self) -> BoundingBox2d:
        """2D bounding box of circle as  :class:`BoundingBox2d` object."""
        rvec = Vec2((self.radius, self.radius))
        return BoundingBox2d((self.center - rvec, self.center + rvec))

    def translate(self, dx: float, dy: float) -> None:
        """Move circle about `dx` in x-axis and about `dy` in y-axis.

        Args:
            dx: translation in x-axis
            dy: translation in y-axis

        """
        self.center += Vec2((dx, dy))

    def point_at(self, angle: float) -> Vec2:
        """Returns point on circle at `angle` as :class:`Vec2` object.

        Args:
            angle: angle in radians, angle goes counter
                clockwise around the z-axis, x-axis = 0 deg.

        """
        return self.center + Vec2.from_angle(angle, self.radius)

    def vertices(self, angles: Iterable[float]) -> Iterable[Vec2]:
        """Yields vertices of the circle for iterable `angles`.

        Args:
            angles: iterable of angles as radians, angle goes counter-clockwise
                around the z-axis, x-axis = 0 deg.

        """
        center = self.center
        radius = self.radius
        for angle in angles:
            yield center + Vec2.from_angle(angle, radius)

    def flattening(self, sagitta: float) -> Iterator[Vec2]:
        """Approximate the circle by vertices, argument `sagitta` is the
        max. distance from the center of an arc segment to the center of its
        chord. Returns a closed polygon where the start vertex is coincident
        with the end vertex!
        """
        from .arc import arc_segment_count

        count = arc_segment_count(self.radius, math.tau, sagitta)
        yield from self.vertices(np.linspace(0.0, math.tau, count + 1))

    def inside(self, point: UVec) -> bool:
        """Returns ``True`` if `point` is inside circle."""
        return self.radius >= self.center.distance(Vec2(point))

    def tangent(self, angle: float) -> ConstructionRay:
        """Returns tangent to circle at `angle` as :class:`ConstructionRay`
        object.

        Args:
            angle: angle in radians

        """
        point_on_circle = self.point_at(angle)
        ray = ConstructionRay(self.center, point_on_circle)
        return ray.orthogonal(point_on_circle)

    def intersect_ray(
        self, ray: ConstructionRay, abs_tol: float = 1e-10
    ) -> Sequence[Vec2]:
        """Returns intersection points of circle and `ray` as sequence of
        :class:`Vec2` objects.

        Args:
            ray: intersection ray
            abs_tol: absolute tolerance for tests (e.g. test for tangents)

        Returns:
            tuple of :class:`Vec2` objects

            =========== ==================================
            tuple size  Description
            =========== ==================================
            0           no intersection
            1           ray is a tangent to circle
            2           ray intersects with the circle
            =========== ==================================

        """
        assert isinstance(ray, ConstructionRay)
        ortho_ray = ray.orthogonal(self.center)
        intersection_point = ray.intersect(ortho_ray)
        dist = self.center.distance(intersection_point)
        result = []
        # Intersect in two points:
        if dist < self.radius:
            # Ray goes through center point:
            if math.isclose(dist, 0.0, abs_tol=abs_tol):
                angle = ortho_ray.angle
                alpha = HALF_PI
            else:
                # The exact direction of angle (all 4 quadrants Q1-Q4) is
                # important: ortho_ray.angle is only correct at the center point
                angle = (intersection_point - self.center).angle
                alpha = math.acos(
                    intersection_point.distance(self.center) / self.radius
                )
            result.append(self.point_at(angle + alpha))
            result.append(self.point_at(angle - alpha))
        # Ray is a tangent of the circle:
        elif math.isclose(dist, self.radius, abs_tol=abs_tol):
            result.append(intersection_point)
        # else: No intersection
        return tuple(result)

    def intersect_line(
        self, line: ConstructionLine, abs_tol: float = 1e-10
    ) -> Sequence[Vec2]:
        """Returns intersection points of circle and `line` as sequence of
        :class:`Vec2` objects.

        Args:
            line: intersection line
            abs_tol: absolute tolerance for tests (e.g. test for tangents)

        Returns:
            tuple of :class:`Vec2` objects

            =========== ==================================
            tuple size  Description
            =========== ==================================
            0           no intersection
            1           line intersects or touches the circle at one point
            2           line intersects the circle at two points
            =========== ==================================

        """
        assert isinstance(line, ConstructionLine)
        return [
            point
            for point in self.intersect_ray(line.ray, abs_tol=abs_tol)
            if is_point_in_line_range(line.start, line.end, point)
        ]

    def intersect_circle(
        self, other: "ConstructionCircle", abs_tol: float = 1e-10
    ) -> Sequence[Vec2]:
        """Returns intersection points of two circles as sequence of
        :class:`Vec2` objects.

        Args:
            other: intersection circle
            abs_tol: absolute tolerance for tests

        Returns:
            tuple of :class:`Vec2` objects

            =========== ==================================
            tuple size  Description
            =========== ==================================
            0           no intersection
            1           circle touches the `other` circle at one point
            2           circle intersects with the `other` circle
            =========== ==================================

        """
        assert isinstance(other, ConstructionCircle)
        r1 = self.radius
        r2 = other.radius
        d = self.center.distance(other.center)
        if d < abs_tol:
            # concentric circles do not intersect by definition
            return tuple()

        d_max = r1 + r2
        d_min = math.fabs(r1 - r2)
        if d_min <= d <= d_max:
            angle = (other.center - self.center).angle
            # Circles touches at one point:
            if math.isclose(d, d_max, abs_tol=abs_tol):
                return (self.point_at(angle),)
            if math.isclose(d, d_min, abs_tol=abs_tol):
                if r1 >= r2:
                    return (self.point_at(angle),)
                return (self.point_at(angle + math.pi),)
            else:  # Circles intersect in two points:
                # Law of Cosines:
                alpha = math.acos((r2 * r2 - r1 * r1 - d * d) / (-2.0 * r1 * d))
                return tuple(self.vertices((angle + alpha, angle - alpha)))
        return tuple()


def is_point_in_line_range(start: Vec2, end: Vec2, point: Vec2) -> bool:
    length = (end - start).magnitude
    if (point - start).magnitude > length:
        return False
    return (point - end).magnitude <= length
