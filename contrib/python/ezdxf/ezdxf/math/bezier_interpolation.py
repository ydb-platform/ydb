# Copyright (c) 2010-2023 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Sequence
from ezdxf.math import Vec3, Bezier4P, UVec

# These are low-level interpolation tools for B-splines, which can not be
# integrated into other curve related modules!
__all__ = ["cubic_bezier_interpolation", "tangents_cubic_bezier_interpolation"]


def cubic_bezier_interpolation(
    points: Iterable[UVec],
) -> Iterable[Bezier4P[Vec3]]:
    """Returns an interpolation curve for given data `points` as multiple cubic
    Bézier-curves. Returns n-1 cubic Bézier-curves for n given data points,
    curve i goes from point[i] to point[i+1].

    Args:
        points: data points

    """
    from ezdxf.math.linalg import tridiagonal_matrix_solver

    # Source: https://towardsdatascience.com/b%C3%A9zier-interpolation-8033e9a262c2
    pnts = Vec3.tuple(points)
    if len(pnts) < 3:
        return

    num = len(pnts) - 1

    # setup tri-diagonal matrix (a, b, c)
    b = [4.0] * num
    a = [1.0] * num
    c = [1.0] * num
    b[0] = 2.0
    b[num - 1] = 7.0
    a[num - 1] = 2.0

    # setup right-hand side quantities
    points_vector = [pnts[0] + 2.0 * pnts[1]]
    points_vector.extend(
        2.0 * (2.0 * pnts[i] + pnts[i + 1]) for i in range(1, num - 1)
    )
    points_vector.append(8.0 * pnts[num - 1] + pnts[num])

    # solve tri-diagonal linear equation system
    solution = tridiagonal_matrix_solver([a, b, c], points_vector)
    control_points_1 = Vec3.list(solution.rows())
    control_points_2 = [
        p * 2.0 - cp for p, cp in zip(pnts[1:], control_points_1[1:])
    ]
    control_points_2.append((control_points_1[num - 1] + pnts[num]) / 2.0)

    for defpoints in zip(
        pnts, control_points_1, control_points_2, pnts[1:]
    ):
        yield Bezier4P(defpoints)


def tangents_cubic_bezier_interpolation(
    fit_points: Sequence[Vec3], normalize=True
) -> list[Vec3]:
    if len(fit_points) < 3:
        raise ValueError("At least 3 points required")

    curves = list(cubic_bezier_interpolation(fit_points))
    tangents = [
        (curve.control_points[1] - curve.control_points[0]) for curve in curves
    ]

    last_points = curves[-1].control_points
    tangents.append(last_points[3] - last_points[2])
    if normalize:
        tangents = [t.normalize() for t in tangents]
    return tangents
