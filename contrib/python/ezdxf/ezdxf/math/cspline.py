# Copyright (c) 2020-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable
import numpy as np

from ezdxf.math import Vec3


class CSpline:
    """
    In numerical analysis, a cubic Hermite spline or cubic Hermite interpolator
    is a spline where each piece is a third-degree polynomial specified in
    Hermite form, that is, by its values and first derivatives at the end points
    of the corresponding domain interval.

    Source: https://en.wikipedia.org/wiki/Cubic_Hermite_spline
    """

    # https://de.wikipedia.org/wiki/Kubisch_Hermitescher_Spline
    def __init__(self, p0: Vec3, p1: Vec3, m0: Vec3, m1: Vec3):
        self.p0 = p0
        self.p1 = p1
        self.m0 = m0
        self.m1 = m1

    def point(self, t: float) -> Vec3:
        t2 = t * t
        t3 = t2 * t
        h00 = t3 * 2.0 - t2 * 3.0 + 1.0
        h10 = -t3 * 2.0 + t2 * 3.0
        h01 = t3 - t2 * 2.0 + t
        h11 = t3 - t2
        return self.p0 * h00 + self.p1 * h10 + self.m0 * h01 + self.m1 * h11


def approximate(csplines: Iterable[CSpline], count) -> Iterable[Vec3]:
    for cspline in csplines:
        for t in np.linspace(0.0, 1.0, count):
            yield cspline.point(t)
