# Copyright (c) 2020-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Sequence
import math
from ezdxf.math import Vec3
from .bezier_interpolation import (
    tangents_cubic_bezier_interpolation,
    cubic_bezier_interpolation,
)
from .construct2d import circle_radius_3p

__all__ = [
    "estimate_tangents",
    "estimate_end_tangent_magnitude",
    "create_t_vector",
    "chord_length",
]


def create_t_vector(fit_points: list[Vec3], method: str) -> list[float]:
    if method == "uniform":
        return uniform_t_vector(len(fit_points))
    elif method in ("distance", "chord"):
        return distance_t_vector(fit_points)
    elif method in ("centripetal", "sqrt_chord"):
        return centripetal_t_vector(fit_points)
    elif method == "arc":
        return arc_t_vector(fit_points)
    else:
        raise ValueError("Unknown method: {}".format(method))


def uniform_t_vector(length: int) -> list[float]:
    n = float(length - 1)
    return [t / n for t in range(length)]


def distance_t_vector(fit_points: list[Vec3]) -> list[float]:
    return _normalize_distances(list(linear_distances(fit_points)))


def centripetal_t_vector(fit_points: list[Vec3]) -> list[float]:
    distances = [
        math.sqrt(p1.distance(p2)) for p1, p2 in zip(fit_points, fit_points[1:])
    ]
    return _normalize_distances(distances)


def _normalize_distances(distances: Sequence[float]) -> list[float]:
    total_length = sum(distances)
    if abs(total_length) <= 1e-12:
        return []
    params: list[float] = [0.0]
    s = 0.0
    for d in distances[:-1]:
        s += d
        params.append(s / total_length)
    params.append(1.0)
    return params


def linear_distances(points: Iterable[Vec3]) -> Iterable[float]:
    prev = None
    for p in points:
        if prev is None:
            prev = p
            continue
        yield prev.distance(p)
        prev = p


def chord_length(points: Iterable[Vec3]) -> float:
    return sum(linear_distances(points))


def arc_t_vector(fit_points: list[Vec3]) -> list[float]:
    distances = list(arc_distances(fit_points))
    return _normalize_distances(distances)


def arc_distances(fit_points: list[Vec3]) -> Iterable[float]:
    p = fit_points

    def _radii() -> Iterable[float]:
        for i in range(len(p) - 2):
            try:
                radius = circle_radius_3p(p[i], p[i + 1], p[i + 2])
            except ZeroDivisionError:
                radius = 0.0
            yield radius

    r: list[float] = list(_radii())
    if len(r) == 0:
        return
    r.append(r[-1])  # 2x last radius
    for k in range(0, len(p) - 1):
        distance = (p[k + 1] - p[k]).magnitude
        rk = r[k]
        if math.isclose(rk, 0):
            yield distance
        else:
            yield math.asin(distance / 2.0 / rk) * 2.0 * rk


def estimate_tangents(
    points: list[Vec3], method: str = "5-points", normalize=True
) -> list[Vec3]:
    """Estimate tangents for curve defined by given fit points.
    Calculated tangents are normalized (unit-vectors).

    Available tangent estimation methods:

        - "3-points": 3 point interpolation
        - "5-points": 5 point interpolation
        - "bezier": tangents from an interpolated cubic bezier curve
        - "diff": finite difference

    Args:
        points: start-, end- and passing points of curve
        method: tangent estimation method
        normalize: normalize tangents if ``True``

    Returns:
        tangents as list of :class:`Vec3` objects

    """
    method = method.lower()
    if method.startswith("bez"):
        return tangents_cubic_bezier_interpolation(points, normalize=normalize)
    elif method.startswith("3-p"):
        return tangents_3_point_interpolation(points, normalize=normalize)
    elif method.startswith("5-p"):
        return tangents_5_point_interpolation(points, normalize=normalize)
    elif method.startswith("dif"):
        return finite_difference_interpolation(points, normalize=normalize)
    else:
        raise ValueError(f"Unknown method: {method}")


def estimate_end_tangent_magnitude(
    points: list[Vec3], method: str = "chord"
) -> tuple[float, float]:
    """Estimate tangent magnitude of start- and end tangents.

    Available estimation methods:

        - "chord": total chord length, curve approximation by straight segments
        - "arc": total arc length, curve approximation by arcs
        - "bezier-n": total length from cubic bezier curve approximation, n
          segments per section

    Args:
        points: start-, end- and passing points of curve
        method: tangent magnitude estimation method

    """
    if method == "chord":
        total_length = sum(p0.distance(p1) for p0, p1 in zip(points, points[1:]))
        return total_length, total_length
    elif method == "arc":
        total_length = sum(arc_distances(points))
        return total_length, total_length
    elif method.startswith("bezier-"):
        count = int(method[7:])
        s = 0.0
        for curve in cubic_bezier_interpolation(points):
            s += sum(linear_distances(curve.approximate(count)))
        return s, s
    else:
        raise ValueError(f"Unknown tangent magnitude calculation method: {method}")


def tangents_3_point_interpolation(
    fit_points: list[Vec3], method: str = "chord", normalize=True
) -> list[Vec3]:
    """Returns from 3 points interpolated and optional normalized tangent
    vectors.
    """
    q = [Q1 - Q0 for Q0, Q1 in zip(fit_points, fit_points[1:])]
    t = list(create_t_vector(fit_points, method))
    delta_t = [t1 - t0 for t0, t1 in zip(t, t[1:])]
    d = [qk / dtk for qk, dtk in zip(q, delta_t)]
    alpha = [dt0 / (dt0 + dt1) for dt0, dt1 in zip(delta_t, delta_t[1:])]
    tangents: list[Vec3] = [Vec3()]  # placeholder
    tangents.extend(
        [(1.0 - alpha[k]) * d[k] + alpha[k] * d[k + 1] for k in range(len(d) - 1)]
    )
    tangents[0] = 2.0 * d[0] - tangents[1]
    tangents.append(2.0 * d[-1] - tangents[-1])
    if normalize:
        tangents = [v.normalize() for v in tangents]
    return tangents


def tangents_5_point_interpolation(
    fit_points: list[Vec3], normalize=True
) -> list[Vec3]:
    """Returns from 5 points interpolated and optional normalized tangent
    vectors.
    """
    n = len(fit_points)
    q = _delta_q(fit_points)

    alpha = list()
    for k in range(n):
        v1 = (q[k - 1].cross(q[k])).magnitude
        v2 = (q[k + 1].cross(q[k + 2])).magnitude
        alpha.append(v1 / (v1 + v2))

    tangents = []
    for k in range(n):
        vk = (1.0 - alpha[k]) * q[k] + alpha[k] * q[k + 1]
        tangents.append(vk)
    if normalize:
        tangents = [v.normalize() for v in tangents]
    return tangents


def _delta_q(points: list[Vec3]) -> list[Vec3]:
    n = len(points)
    q = [Vec3()]  # placeholder
    q.extend([points[k + 1] - points[k] for k in range(n - 1)])
    q[0] = 2.0 * q[1] - q[2]
    q.append(2.0 * q[n - 1] - q[n - 2])  # q[n]
    q.append(2.0 * q[n] - q[n - 1])  # q[n+1]
    q.append(2.0 * q[0] - q[1])  # q[-1]
    return q


def finite_difference_interpolation(
    fit_points: list[Vec3], normalize=True
) -> list[Vec3]:
    f = 2.0
    p = fit_points

    t = [(p[1] - p[0]) / f]
    for k in range(1, len(fit_points) - 1):
        t.append((p[k] - p[k - 1]) / f + (p[k + 1] - p[k]) / f)
    t.append((p[-1] - p[-2]) / f)
    if normalize:
        t = [v.normalize() for v in t]
    return t


def cardinal_interpolation(fit_points: list[Vec3], tension: float) -> list[Vec3]:
    # https://en.wikipedia.org/wiki/Cubic_Hermite_spline
    def tangent(p0, p1):
        return (p0 - p1).normalize(1.0 - tension)

    t = [tangent(fit_points[0], fit_points[1])]
    for k in range(1, len(fit_points) - 1):
        t.append(tangent(fit_points[k + 1], fit_points[k - 1]))
    t.append(tangent(fit_points[-1], fit_points[-2]))
    return t
