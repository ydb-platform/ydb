# Copyright (c) 2021-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Union, Sequence, TypeVar
import math
from ezdxf.math import (
    BSpline,
    Bezier4P,
    Bezier3P,
    UVec,
    Vec3,
    Vec2,
    AnyVec,
    BoundingBox,
)
from ezdxf.math.linalg import cubic_equation

__all__ = [
    "bezier_to_bspline",
    "quadratic_to_cubic_bezier",
    "have_bezier_curves_g1_continuity",
    "AnyBezier",
    "reverse_bezier_curves",
    "split_bezier",
    "quadratic_bezier_from_3p",
    "cubic_bezier_from_3p",
    "cubic_bezier_bbox",
    "quadratic_bezier_bbox",
    "intersection_ray_cubic_bezier_2d",
]


T = TypeVar("T", bound=AnyVec)

AnyBezier = Union[Bezier3P, Bezier4P]


def quadratic_to_cubic_bezier(curve: Bezier3P) -> Bezier4P:
    """Convert quadratic Bèzier curves (:class:`ezdxf.math.Bezier3P`) into
    cubic Bèzier curves (:class:`ezdxf.math.Bezier4P`).

    """
    start, control, end = curve.control_points
    control_1 = start + 2 * (control - start) / 3
    control_2 = end + 2 * (control - end) / 3
    return Bezier4P((start, control_1, control_2, end))


def bezier_to_bspline(curves: Iterable[AnyBezier]) -> BSpline:
    """Convert multiple quadratic or cubic Bèzier curves into a single cubic
    B-spline.

    For good results the curves must be lined up seamlessly, i.e. the starting
    point of the following curve must be the same as the end point of the
    previous curve. G1 continuity or better at the connection points of the
    Bézier curves is required to get best results.

    """

    # Source: https://math.stackexchange.com/questions/2960974/convert-continuous-bezier-curve-to-b-spline
    def get_points(bezier: AnyBezier):
        points = bezier.control_points
        if len(points) < 4:
            return quadratic_to_cubic_bezier(bezier).control_points
        else:
            return points

    bezier_curve_points = [get_points(c) for c in curves]
    if len(bezier_curve_points) == 0:
        raise ValueError("one or more Bézier curves required")
    # Control points of the B-spline are the same as of the Bézier curves.
    # Remove duplicate control points at start and end of the curves.
    control_points = list(bezier_curve_points[0])
    for c in bezier_curve_points[1:]:
        control_points.extend(c[1:])
    knots = [0, 0, 0, 0]  # multiplicity of the 1st and last control point is 4
    n = len(bezier_curve_points)
    for k in range(1, n):
        knots.extend((k, k, k))  # multiplicity of the inner control points is 3
    knots.extend((n, n, n, n))
    return BSpline(control_points, order=4, knots=knots)


def have_bezier_curves_g1_continuity(
    b1: AnyBezier, b2: AnyBezier, g1_tol: float = 1e-4
) -> bool:
    """Return ``True`` if the given adjacent Bézier curves have G1 continuity.
    """
    b1_pnts = tuple(b1.control_points)
    b2_pnts = tuple(b2.control_points)

    if not b1_pnts[-1].isclose(b2_pnts[0]):
        return False  # start- and end point are not close enough

    try:
        te = (b1_pnts[-1] - b1_pnts[-2]).normalize()
    except ZeroDivisionError:
        return False  # tangent calculation not possible

    try:
        ts = (b2_pnts[1] - b2_pnts[0]).normalize()
    except ZeroDivisionError:
        return False  # tangent calculation not possible

    # 0 = normal; 1 = same direction; -1 = opposite direction
    return math.isclose(te.dot(ts), 1.0, abs_tol=g1_tol)


def reverse_bezier_curves(curves: list[AnyBezier]) -> list[AnyBezier]:
    curves = list(c.reverse() for c in curves)
    curves.reverse()
    return curves


def split_bezier(
    control_points: Sequence[T], t: float
) -> tuple[list[T], list[T]]:
    """Split a Bèzier curve at parameter `t`.

    Returns the control points for two new Bèzier curves of the same degree
    and type as the input curve. (source: `pomax-1`_)

    Args:
         control_points: of the Bèzier curve as :class:`Vec2` or :class:`Vec3`
            objects. Requires 3 points for a quadratic curve, 4 points for a
            cubic curve , ...
         t: parameter where to split the curve in the range [0, 1]

    .. _pomax-1: https://pomax.github.io/bezierinfo/#splitting

    """
    if len(control_points) < 2:
        raise ValueError("2 or more control points required")
    if t < 0.0 or t > 1.0:
        raise ValueError("parameter `t` must be in range [0, 1]")
    left: list[T] = []
    right: list[T] = []

    def split(points: Sequence[T]):
        n: int = len(points) - 1
        left.append(points[0])
        right.append(points[n])
        if n == 0:
            return
        split(
            tuple(points[i] * (1.0 - t) + points[i + 1] * t for i in range(n))
        )

    split(control_points)
    return left, right


def quadratic_bezier_from_3p(p1: UVec, p2: UVec, p3: UVec) -> Bezier3P:
    """Returns a quadratic Bèzier curve :class:`Bezier3P` from three points.
    The curve starts at `p1`, goes through `p2` and ends at `p3`.
    (source: `pomax-2`_)

    .. _pomax-2: https://pomax.github.io/bezierinfo/#pointcurves

    """

    def u_func(t: float) -> float:
        mt = 1.0 - t
        mt2 = mt * mt
        return mt2 / (t * t + mt2)

    def ratio(t: float) -> float:
        t2 = t * t
        mt = 1.0 - t
        mt2 = mt * mt
        return abs((t2 + mt2 - 1.0) / (t2 + mt2))

    s = Vec3(p1)
    b = Vec3(p2)
    e = Vec3(p3)
    d1 = (s - b).magnitude
    d2 = (e - b).magnitude
    t = d1 / (d1 + d2)
    u = u_func(t)
    c = s * u + e * (1.0 - u)
    a = b + (b - c) / ratio(t)
    return Bezier3P([s, a, e])


def cubic_bezier_from_3p(p1: UVec, p2: UVec, p3: UVec) -> Bezier4P:
    """Returns a cubic Bèzier curve :class:`Bezier4P` from three points.
    The curve starts at `p1`, goes through `p2` and ends at `p3`.
    (source: `pomax-2`_)
    """
    qbez = quadratic_bezier_from_3p(p1, p2, p3)
    return quadratic_to_cubic_bezier(qbez)


def cubic_bezier_bbox(curve: Bezier4P, *, abs_tol=1e-12) -> BoundingBox:
    """Returns the :class:`~ezdxf.math.BoundingBox` of a cubic Bézier curve
    of type :class:`~ezdxf.math.Bezier4P`.
    """
    cp = curve.control_points
    points: list[Vec3] = [cp[0], cp[3]]
    for p1, p2, p3, p4 in zip(*cp):
        a = 3.0 * (-p1 + 3.0 * p2 - 3.0 * p3 + p4)
        b = 6.0 * (p1 - 2.0 * p2 + p3)
        c = 3.0 * (p2 - p1)
        if abs(a) < abs_tol:
            if abs(b) < abs_tol:
                t = -c  # or skip this case?
            else:
                t = -c / b
            if 0.0 < t < 1.0:
                points.append((curve.point(t)))
            continue

        try:
            sqrt_bb4ac = math.sqrt(b * b - 4.0 * a * c)
        except ValueError:  # domain error
            continue
        aa = 2.0 * a
        t = (-b + sqrt_bb4ac) / aa
        if 0.0 < t < 1.0:
            points.append(curve.point(t))
        t = (-b - sqrt_bb4ac) / aa
        if 0.0 < t < 1.0:
            points.append(curve.point(t))
    return BoundingBox(points)


def quadratic_bezier_bbox(curve: Bezier3P, *, abs_tol=1e-12) -> BoundingBox:
    """Returns the :class:`~ezdxf.math.BoundingBox` of a quadratic Bézier curve
    of type :class:`~ezdxf.math.Bezier3P`.
    """
    return cubic_bezier_bbox(quadratic_to_cubic_bezier(curve), abs_tol=abs_tol)


def _bezier4poly(a: float, b: float, c: float, d: float):
    a3 = a * 3.0
    b3 = b * 3.0
    c3 = c * 3.0
    return -a + b3 - c3 + d, a3 - b * 6.0 + c3, -a3 + b3, a


# noinspection PyPep8Naming
def intersection_params_ray_cubic_bezier(
    p0: AnyVec, p1: AnyVec, cp: Sequence[AnyVec]
) -> list[float]:
    """Returns the parameters of the intersection points between the ray defined
    by two points `p0` and `p1` and the cubic Bézier curve defined by four
    control points `cp`.
    """
    A = p1.y - p0.y
    B = p0.x - p1.x
    C = p0.x * (p0.y - p1.y) + p0.y * (p1.x - p0.x)

    c0, c1, c2, c3 = cp
    bx = _bezier4poly(c0.x, c1.x, c2.x, c3.x)
    by = _bezier4poly(c0.y, c1.y, c2.y, c3.y)
    return sorted(
        v
        for v in cubic_equation(
            A * bx[0] + B * by[0],
            A * bx[1] + B * by[1],
            A * bx[2] + B * by[2],
            A * bx[3] + B * by[3] + C,
        )
        if 0.0 <= v <= 1.0
    )


def intersection_ray_cubic_bezier_2d(
    p0: UVec,
    p1: UVec,
    curve: Bezier4P,
) -> Sequence[Vec2]:
    """Returns the intersection points between the `ray` defined by two points
    `p0` and `p1` and the given cubic Bézier `curve`. Ignores the z-axis of 3D
    curves.

    Returns 0-3 intersection points as :class:`Vec2` objects in the
    order start- to end point of the curve.

    """
    return Vec2.tuple(
        curve.point(t)
        for t in intersection_params_ray_cubic_bezier(
            Vec2(p0), Vec2(p1), curve.control_points
        )
    )
