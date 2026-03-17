# Copyright (c) 2011-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Sequence

from functools import partial
import math
import numpy as np
import numpy.typing as npt

from ezdxf.math import (
    Vec3,
    Vec2,
    UVec,
    Matrix44,
    X_AXIS,
    Y_AXIS,
    arc_angle_span_rad,
)

TOLERANCE = 1e-10
RADIANS_90 = math.pi / 2.0
RADIANS_180 = math.pi
RADIANS_270 = RADIANS_90 * 3.0
RADIANS_360 = 2.0 * math.pi

__all__ = [
    "closest_point",
    "convex_hull_2d",
    "distance_point_line_2d",
    "is_convex_polygon_2d",
    "is_axes_aligned_rectangle_2d",
    "is_point_on_line_2d",
    "is_point_left_of_line",
    "point_to_line_relation",
    "enclosing_angles",
    "sign",
    "area",
    "np_area",
    "circle_radius_3p",
    "TOLERANCE",
    "has_matrix_2d_stretching",
    "decdeg2dms",
    "ellipse_param_span",
]


def sign(f: float) -> float:
    """Return sign of float `f` as -1 or +1, 0 returns +1"""
    return -1.0 if f < 0.0 else +1.0


def decdeg2dms(value: float) -> tuple[float, float, float]:
    """Return decimal degrees as tuple (Degrees, Minutes, Seconds)."""
    mnt, sec = divmod(value * 3600.0, 60.0)
    deg, mnt = divmod(mnt, 60.0)
    return deg, mnt, sec


def ellipse_param_span(start_param: float, end_param: float) -> float:
    """Returns the counter-clockwise params span of an elliptic arc from start-
    to end param.

    Returns the param span in the range [0, 2π], 2π is a full ellipse.
    Full ellipse handling is a special case, because normalization of params
    which describe a full ellipse would return 0 if treated as regular params.
    e.g. (0, 2π) → 2π, (0, -2π) → 2π, (π, -π) → 2π.
    Input params with the same value always return 0 by definition:
    (0, 0) → 0, (-π, -π) → 0, (2π, 2π) → 0.

    Alias to function: :func:`ezdxf.math.arc_angle_span_rad`

    """
    return arc_angle_span_rad(float(start_param), float(end_param))


def closest_point(base: UVec, points: Iterable[UVec]) -> Vec3 | None:
    """Returns the closest point to a give `base` point.

    Args:
        base: base point as :class:`Vec3` compatible object
        points: iterable of points as :class:`Vec3` compatible object

    """
    base = Vec3(base)
    min_dist: float | None = None
    found: Vec3 | None = None
    for point in points:
        p = Vec3(point)
        dist = base.distance(p)
        if (min_dist is None) or (dist < min_dist):
            min_dist = dist
            found = p
    return found


def convex_hull_2d(points: Iterable[UVec]) -> list[Vec2]:
    """Returns the 2D convex hull of given `points`.

    Returns a closed polyline, first vertex is equal to the last vertex.

    Args:
        points: iterable of points, z-axis is ignored

    """

    # Source: https://massivealgorithms.blogspot.com/2019/01/convex-hull-sweep-line.html?m=1
    def cross(o: Vec2, a: Vec2, b: Vec2) -> float:
        return (a - o).det(b - o)

    vertices = Vec2.list(set(points))
    vertices.sort()
    if len(vertices) < 3:
        raise ValueError("Convex hull calculation requires 3 or more unique points.")

    n: int = len(vertices)
    hull: list[Vec2] = [Vec2()] * (2 * n)
    k: int = 0
    i: int
    for i in range(n):
        while k >= 2 and cross(hull[k - 2], hull[k - 1], vertices[i]) <= 0.0:
            k -= 1
        hull[k] = vertices[i]
        k += 1
    t: int = k + 1
    for i in range(n - 2, -1, -1):
        while k >= t and cross(hull[k - 2], hull[k - 1], vertices[i]) <= 0.0:
            k -= 1
        hull[k] = vertices[i]
        k += 1
    return hull[:k]


def enclosing_angles(angle, start_angle, end_angle, ccw=True, abs_tol=TOLERANCE):
    isclose = partial(math.isclose, abs_tol=abs_tol)

    s = start_angle % math.tau
    e = end_angle % math.tau
    a = angle % math.tau
    if isclose(s, e):
        return isclose(s, a)

    if s < e:
        r = s < a < e
    else:
        r = not (e < a < s)
    return r if ccw else not r


def is_point_on_line_2d(
    point: Vec2, start: Vec2, end: Vec2, ray=True, abs_tol=TOLERANCE
) -> bool:
    """Returns ``True`` if `point` is on `line`.

    Args:
        point: 2D point to test as :class:`Vec2`
        start: line definition point as :class:`Vec2`
        end: line definition point as :class:`Vec2`
        ray: if ``True`` point has to be on the infinite ray, if ``False``
            point has to be on the line segment
        abs_tol: tolerance for on the line test

    """
    point_x, point_y = point
    start_x, start_y = start
    end_x, end_y = end
    on_line = (
        math.fabs(
            (end_y - start_y) * point_x
            - (end_x - start_x) * point_y
            + (end_x * start_y - end_y * start_x)
        )
        <= abs_tol
    )
    if not on_line or ray:
        return on_line
    else:
        if start_x > end_x:
            start_x, end_x = end_x, start_x
        if not (start_x - abs_tol <= point_x <= end_x + abs_tol):
            return False
        if start_y > end_y:
            start_y, end_y = end_y, start_y
        if not (start_y - abs_tol <= point_y <= end_y + abs_tol):
            return False
        return True


def point_to_line_relation(
    point: Vec2, start: Vec2, end: Vec2, abs_tol=TOLERANCE
) -> int:
    """Returns ``-1`` if `point` is left `line`, ``+1`` if `point` is right of
    `line` and ``0`` if `point` is on the `line`. The `line` is defined by two
    vertices given as arguments `start` and `end`.

    Args:
        point: 2D point to test as :class:`Vec2`
        start: line definition point as :class:`Vec2`
        end: line definition point as :class:`Vec2`
        abs_tol: tolerance for minimum distance to line

    """
    rel = (end.x - start.x) * (point.y - start.y) - (end.y - start.y) * (
        point.x - start.x
    )
    if abs(rel) <= abs_tol:
        return 0
    elif rel < 0:
        return +1
    else:
        return -1


def is_point_left_of_line(point: Vec2, start: Vec2, end: Vec2, colinear=False) -> bool:
    """Returns ``True`` if `point` is "left of line" defined by `start-` and
    `end` point, a colinear point is also "left of line" if argument `colinear`
    is ``True``.

    Args:
        point: 2D point to test as :class:`Vec2`
        start: line definition point as :class:`Vec2`
        end: line definition point as :class:`Vec2`
        colinear: a colinear point is also "left of line" if ``True``

    """
    rel = point_to_line_relation(point, start, end)
    if colinear:
        return rel < 1
    else:
        return rel < 0


def distance_point_line_2d(point: Vec2, start: Vec2, end: Vec2) -> float:
    """Returns the normal distance from `point` to 2D line defined by `start-`
    and `end` point.
    """
    # wikipedia: https://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line.
    if start.isclose(end):
        raise ZeroDivisionError("Not a line.")
    return math.fabs((start - point).det(end - point)) / (end - start).magnitude


def circle_radius_3p(a: Vec3, b: Vec3, c: Vec3) -> float:
    ba = b - a
    ca = c - a
    cb = c - b
    upper = ba.magnitude * ca.magnitude * cb.magnitude
    lower = ba.cross(ca).magnitude * 2.0
    return upper / lower


def area(vertices: Iterable[UVec]) -> float:
    """Returns the area of a polygon.

    Returns the projected area in the xy-plane for any vertices (z-axis will be ignored).

    """
    # TODO: how to do all this in numpy efficiently?

    vec2s = Vec2.list(vertices)
    if len(vec2s) < 3:
        return 0.0

    # close polygon:
    if not vec2s[0].isclose(vec2s[-1]):
        vec2s.append(vec2s[0])
    return np_area(np.array([(v.x, v.y) for v in vec2s], dtype=np.float64))


def np_area(vertices: npt.NDArray) -> float:
    """Returns the area of a polygon.

    Returns the projected area in the xy-plane, the z-axis will be ignored.
    The polygon has to be closed (first vertex == last vertex) and should have 3 or more
    corner vertices to return a valid result.

    Args:
        vertices: numpy array [:, n], n > 1

    """
    p1x = vertices[:-1, 0]
    p2x = vertices[1:, 0]
    p1y = vertices[:-1, 1]
    p2y = vertices[1:, 1]
    return np.abs(np.sum(p1x * p2y - p1y * p2x)) * 0.5


def has_matrix_2d_stretching(m: Matrix44) -> bool:
    """Returns ``True`` if matrix `m` performs a non-uniform xy-scaling.
    Uniform scaling is not stretching in this context.

    Does not check if the target system is a cartesian coordinate system, use the
    :class:`~ezdxf.math.Matrix44` property :attr:`~ezdxf.math.Matrix44.is_cartesian`
    for that.
    """
    ux = m.transform_direction(X_AXIS)
    uy = m.transform_direction(Y_AXIS)
    return not math.isclose(ux.magnitude_square, uy.magnitude_square)


def is_convex_polygon_2d(polygon: list[Vec2], *, strict=False, epsilon=1e-6) -> bool:
    """Returns ``True`` if the 2D `polygon` is convex.

    This function supports open and closed polygons with clockwise or counter-clockwise
    vertex orientation.

    Coincident vertices will always be skipped and if argument `strict` is ``True``,
    polygons with collinear vertices are not considered as convex.

    This solution works only for simple non-self-intersecting polygons!

    """
    # TODO: Cython implementation
    if len(polygon) < 3:
        return False

    global_sign: int = 0
    current_sign: int = 0
    prev = polygon[-1]
    prev_prev = polygon[-2]
    for vertex in polygon:
        if vertex.isclose(prev):  # skip coincident vertices
            continue

        det = (prev - vertex).det(prev_prev - prev)
        if abs(det) >= epsilon:
            current_sign = -1 if det < 0.0 else +1
            if not global_sign:
                global_sign = current_sign
            # do all determinants have the same sign?
            if global_sign != current_sign:
                return False
        elif strict:  # collinear vertices
            return False

        prev_prev = prev
        prev = vertex
    return bool(global_sign)


def is_axes_aligned_rectangle_2d(points: list[Vec2]) -> bool:
    """Returns ``True`` if the given points represent a rectangle aligned with the
    coordinate system axes.

    The sides of the rectangle must be parallel to the x- and y-axes of the coordinate
    system.  The rectangle can be open or closed (first point == last point) and
    oriented clockwise or counter-clockwise.  Only works with 4 or 5 vertices, rectangles
    that have sides with collinear edges are not considered rectangles.

    .. versionadded:: 1.2.0

    """

    def is_horizontal(a: Vec2, b: Vec2) -> bool:
        return math.isclose(a.y, b.y)

    def is_vertical(a: Vec2, b: Vec2):
        return math.isclose(a.x, b.x)

    count = len(points)
    if points[0].isclose(points[-1]):
        count -= 1
    if count != 4:
        return False
    p0, p1, p2, p3, *_ = points
    if (
        is_horizontal(p0, p1)
        and is_vertical(p1, p2)
        and is_horizontal(p2, p3)
        and is_vertical(p3, p0)
    ):
        return True
    if (
        is_horizontal(p1, p2)
        and is_vertical(p2, p3)
        and is_horizontal(p3, p0)
        and is_vertical(p0, p1)
    ):
        return True
    return False
