# Copyright (c) 2010-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Optional
import math
from ezdxf.math import Vec2, intersection_line_line_2d, UVec
from .construct2d import is_point_left_of_line, TOLERANCE
from .bbox import BoundingBox2d


__all__ = ["ConstructionRay", "ConstructionLine", "ParallelRaysError"]


class ParallelRaysError(ArithmeticError):
    pass


HALF_PI = math.pi / 2.0
THREE_PI_HALF = 1.5 * math.pi
DOUBLE_PI = math.pi * 2.0
ABS_TOL = 1e-12


class ConstructionRay:
    """Construction tool for infinite 2D rays.

    Args:
        p1: definition point 1
        p2: ray direction as 2nd point or ``None``
        angle: ray direction as angle in radians or ``None``

    """

    def __init__(
        self, p1: UVec, p2: Optional[UVec] = None, angle: Optional[float] = None
    ):
        self._location = Vec2(p1)
        self._angle: Optional[float]
        self._slope: Optional[float]
        self._yof0: Optional[float]
        self._direction: Vec2
        self._is_vertical: bool
        self._is_horizontal: bool

        if p2 is not None:
            p2_ = Vec2(p2)
            if self._location.x < p2_.x:
                self._direction = (p2_ - self._location).normalize()
            else:
                self._direction = (self._location - p2_).normalize()
            self._angle = self._direction.angle
        elif angle is not None:
            self._angle = angle
            self._direction = Vec2.from_angle(angle)
        else:
            raise ValueError("p2 or angle required.")

        if abs(self._direction.x) <= ABS_TOL:
            self._slope = None
            self._yof0 = None
        else:
            self._slope = self._direction.y / self._direction.x
            self._yof0 = self._location.y - self._slope * self._location.x
        self._is_vertical = self._slope is None
        self._is_horizontal = abs(self._direction.y) <= ABS_TOL

    @property
    def location(self) -> Vec2:
        """Location vector as :class:`Vec2`."""
        return self._location

    @property
    def direction(self) -> Vec2:
        """Direction vector as :class:`Vec2`."""
        return self._direction

    @property
    def slope(self) -> Optional[float]:
        """Slope of ray or ``None`` if vertical."""
        return self._slope

    @property
    def angle(self) -> float:
        """Angle between x-axis and ray in radians."""
        if self._angle is None:
            return self._direction.angle
        else:
            return self._angle

    @property
    def angle_deg(self) -> float:
        """Angle between x-axis and ray in degrees."""
        return math.degrees(self.angle)

    @property
    def is_vertical(self) -> bool:
        """``True`` if ray is vertical (parallel to y-axis)."""
        return self._is_vertical

    @property
    def is_horizontal(self) -> bool:
        """``True`` if ray is horizontal (parallel to x-axis)."""
        return self._is_horizontal

    def __repr__(self) -> str:
        return (
            "ConstructionRay(p1=({0.location.x:.3f}, {0.location.y:.3f}), "
            "angle={0.angle:.5f})".format(self)
        )

    def is_parallel(self, other: ConstructionRay) -> bool:
        """Returns ``True`` if rays are parallel."""
        if self._is_vertical:
            return other._is_vertical
        if other._is_vertical:
            return False
        if self._is_horizontal:
            return other._is_horizontal
        # guards above guarantee that no slope is None
        return math.isclose(self._slope, other._slope, abs_tol=ABS_TOL)  # type: ignore

    def intersect(self, other: ConstructionRay) -> Vec2:
        """Returns the intersection point as ``(x, y)`` tuple of `self` and
        `other`.

        Raises:
             ParallelRaysError: if rays are parallel

        """
        ray1 = self
        ray2 = other
        if ray1.is_parallel(ray2):
            raise ParallelRaysError("Rays are parallel")

        if ray1._is_vertical:
            x = ray1._location.x
            if ray2.is_horizontal:
                y = ray2._location.y
            else:
                y = ray2.yof(x)
        elif ray2._is_vertical:
            x = ray2._location.x
            if ray1.is_horizontal:
                y = ray1._location.y
            else:
                y = ray1.yof(x)
        elif ray1._is_horizontal:
            y = ray1._location.y
            x = ray2.xof(y)
        elif ray2._is_horizontal:
            y = ray2._location.y
            x = ray1.xof(y)
        else:
            # calc intersection with the 'straight-line-equation'
            # based on y(x) = y0 + x*slope
            # guards above guarantee that no slope is None
            x = (ray1._yof0 - ray2._yof0) / (ray2._slope - ray1._slope)  # type: ignore
            y = ray1.yof(x)
        return Vec2((x, y))

    def orthogonal(self, location: UVec) -> ConstructionRay:
        """Returns orthogonal ray at `location`."""
        return ConstructionRay(location, angle=self.angle + HALF_PI)

    def yof(self, x: float) -> float:
        """Returns y-value of ray for `x` location.

        Raises:
            ArithmeticError: for vertical rays

        """
        if self._is_vertical:
            raise ArithmeticError
        # guard above guarantee that slope is not None
        return self._yof0 + float(x) * self._slope  # type: ignore

    def xof(self, y: float) -> float:
        """Returns x-value of ray for `y` location.

        Raises:
            ArithmeticError: for horizontal rays

        """
        if self._is_vertical:  # slope == None
            return self._location.x
        elif not self._is_horizontal:  # slope != None & slope != 0
            return (float(y) - self._yof0) / self._slope  # type: ignore
        else:
            raise ArithmeticError

    def bisectrix(self, other: ConstructionRay) -> ConstructionRay:
        """Bisectrix between `self` and `other`."""
        intersection = self.intersect(other)
        alpha = (self.angle + other.angle) / 2.0
        return ConstructionRay(intersection, angle=alpha)


class ConstructionLine:
    """Construction tool for 2D lines.

    The :class:`ConstructionLine` class is similar to :class:`ConstructionRay`,
    but has a start- and endpoint. The direction of line goes from start- to
    endpoint, "left of line" is always in relation to this line direction.

    Args:
        start: start point of line as :class:`Vec2` compatible object
        end: end point of line as :class:`Vec2` compatible object

    """

    def __init__(self, start: UVec, end: UVec):
        self.start = Vec2(start)
        self.end = Vec2(end)

    def __repr__(self) -> str:
        return "ConstructionLine({0.start}, {0.end})".format(self)

    @property
    def bounding_box(self) -> BoundingBox2d:
        """bounding box of line as :class:`BoundingBox2d` object."""
        return BoundingBox2d((self.start, self.end))

    def translate(self, dx: float, dy: float) -> None:
        """
        Move line about `dx` in x-axis and about `dy` in y-axis.

        Args:
            dx: translation in x-axis
            dy: translation in y-axis

        """
        v = Vec2(dx, dy)
        self.start += v
        self.end += v

    @property
    def sorted_points(self):
        return (
            (self.end, self.start)
            if self.start > self.end
            else (self.start, self.end)
        )

    @property
    def ray(self):
        """collinear :class:`ConstructionRay`."""
        return ConstructionRay(self.start, self.end)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ConstructionLine):
            raise TypeError(type(other))
        return self.sorted_points == other.sorted_points

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ConstructionLine):
            raise TypeError(type(other))
        return self.sorted_points < other.sorted_points

    def length(self) -> float:
        """Returns length of line."""
        return (self.end - self.start).magnitude

    def midpoint(self) -> Vec2:
        """Returns mid point of line."""
        return self.start.lerp(self.end)

    @property
    def is_vertical(self) -> bool:
        """``True`` if line is vertical."""
        return math.isclose(self.start.x, self.end.x)

    @property
    def is_horizontal(self) -> bool:
        """``True`` if line is horizontal."""
        return math.isclose(self.start.y, self.end.y)

    def inside_bounding_box(self, point: UVec) -> bool:
        """Returns ``True`` if `point` is inside of line bounding box."""
        return self.bounding_box.inside(point)

    def intersect(
        self, other: ConstructionLine, abs_tol: float = TOLERANCE
    ) -> Optional[Vec2]:
        """Returns the intersection point of to lines or ``None`` if they have
        no intersection point.

        Args:
            other: other :class:`ConstructionLine`
            abs_tol: tolerance for distance check

        """
        return intersection_line_line_2d(
            (self.start, self.end),
            (other.start, other.end),
            virtual=False,
            abs_tol=abs_tol,
        )

    def has_intersection(
        self, other: ConstructionLine, abs_tol: float = TOLERANCE
    ) -> bool:
        """Returns ``True`` if has intersection with `other` line."""
        return self.intersect(other, abs_tol=abs_tol) is not None

    def is_point_left_of_line(self, point: UVec, colinear=False) -> bool:
        """Returns ``True`` if `point` is left of construction line in relation
        to the line direction from start to end.

        If `colinear` is ``True``, a colinear point is also left of the line.

        """
        return is_point_left_of_line(
            point, self.start, self.end, colinear=colinear
        )
