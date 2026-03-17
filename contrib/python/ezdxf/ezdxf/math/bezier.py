# Copyright (c) 2010-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Iterable, Sequence
from functools import lru_cache
import math
import numpy as np

from ezdxf.math import Vec3, NULLVEC, Matrix44, UVec


__all__ = ["Bezier"]


"""

Bezier curves
=============

https://www.cl.cam.ac.uk/teaching/2000/AGraphHCI/SMEG/node3.html

A Bezier curve is a weighted sum of n+1 control points,  P0, P1, ..., Pn, where 
the weights are the Bernstein polynomials. 

The Bezier curve of order n+1 (degree n) has n+1 control points. These are the 
first three orders of Bezier curve definitions. 

(75) linear P(t) = (1-t)*P0 + t*P1
(76) quadratic P(t) = (1-t)^2*P0 + 2*(t-1)*t*P1 + t^2*P2
(77) cubic P(t) = (1-t)^3*P0 + 3*(1-t)^2*t*P1 + 3*(1-t)*t^2*P2 + t^3*P3

Ways of thinking about Bezier curves
------------------------------------

There are several useful ways in which you can think about Bezier curves. 
Here are the ones that I use.

Linear interpolation
~~~~~~~~~~~~~~~~~~~~

Equation (75) is obviously a linear interpolation between two points. Equation 
(76) can be rewritten as a linear interpolation between linear interpolations 
between points.

Weighted average
~~~~~~~~~~~~~~~~

A Bezier curve can be seen as a weighted average of all of its control points. 
Because all of the weights are positive, and because the weights sum to one, the 
Bezier curve is guaranteed to lie within the convex hull of its control points.
    
Refinement of the control polygon
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Bezier curve can be seen as some sort of refinement of the polygon made by 
connecting its control points in order. The Bezier curve starts and ends at the 
two end points and its shape is determined by the relative positions of the n-1 
other control points, although it will generally not pass through these other 
control points. The tangent vectors at the start and end of the curve pass 
through the end point and the immediately adjacent point.

Continuity
----------

You should note that each Bezier curve is independent of any other Bezier curve. 
If we wish two Bezier curves to join with any type of continuity, then we must 
explicitly position the control points of the second curve so that they bear 
the appropriate relationship with the control points in the first curve.

Any Bezier curve is infinitely differentiable within itself, and is therefore 
continuous to any degree.

"""


class Bezier:
    """Generic `Bézier curve`_ of any degree.

    A `Bézier curve`_ is a parametric curve used in computer graphics and
    related fields. Bézier curves are used to model smooth curves that can be
    scaled indefinitely. "Paths", as they are commonly referred to in image
    manipulation programs, are combinations of linked Bézier curves.
    Paths are not bound by the limits of rasterized images and are intuitive to
    modify. (Source: Wikipedia)

    This is a generic implementation which works with any count of definition
    points greater than 2, but it is a simple and slow implementation. For more
    performance look at the specialized :class:`Bezier4P` and :class:`Bezier3P`
    classes.

    Objects are immutable.

    Args:
        defpoints: iterable of definition points as :class:`Vec3` compatible objects.

    """

    def __init__(self, defpoints: Iterable[UVec]):
        self._defpoints: Sequence[Vec3] = Vec3.tuple(defpoints)

    @property
    def control_points(self) -> Sequence[Vec3]:
        """Control points as tuple of :class:`Vec3` objects."""
        return self._defpoints

    def approximate(self, segments: int = 20) -> Iterable[Vec3]:
        """Approximates curve by vertices as :class:`Vec3` objects, vertices
        count = segments + 1.
        """
        return self.points(self.params(segments))

    def flattening(self, distance: float, segments: int = 4) -> Iterable[Vec3]:
        """Adaptive recursive flattening. The argument `segments` is the
        minimum count of approximation segments, if the distance from the center
        of the approximation segment to the curve is bigger than `distance` the
        segment will be subdivided.

        Args:
            distance: maximum distance from the center of the curve (Cn)
                to the center of the linear (C1) curve between two
                approximation points to determine if a segment should be
                subdivided.
            segments: minimum segment count

        """

        def subdiv(start_point, end_point, start_t: float, end_t: float):
            mid_t = (start_t + end_t) * 0.5
            mid_point = self.point(mid_t)
            chk_point = start_point.lerp(end_point)
            # center point point is faster than projecting mid point onto
            # vector start -> end:
            if chk_point.distance(mid_point) < distance:
                yield end_point
            else:
                yield from subdiv(start_point, mid_point, start_t, mid_t)
                yield from subdiv(mid_point, end_point, mid_t, end_t)

        dt = 1.0 / segments
        t0 = 0.0
        start_point = self._defpoints[0]
        yield start_point
        while t0 < 1.0:
            t1 = t0 + dt
            if math.isclose(t1, 1.0):
                end_point = self._defpoints[-1]
                t1 = 1.0
            else:
                end_point = self.point(t1)
            yield from subdiv(start_point, end_point, t0, t1)
            t0 = t1
            start_point = end_point

    def params(self, segments: int) -> Iterable[float]:
        """Yield evenly spaced parameters from 0 to 1 for given segment count."""
        yield from np.linspace(0.0, 1.0, segments + 1)

    def point(self, t: float) -> Vec3:
        """Returns a point for parameter `t` in range [0, 1] as :class:`Vec3`
        object.
        """
        if t < 0.0 or t > 1.0:
            raise ValueError("Parameter t not in range [0, 1]")
        if (1.0 - t) < 5e-6:
            t = 1.0
        point = NULLVEC
        pts = self._defpoints
        n = len(pts)

        for i in range(n):
            point += bernstein_basis(n - 1, i, t) * pts[i]
        return point

    def points(self, t: Iterable[float]) -> Iterable[Vec3]:
        """Yields multiple points for parameters in vector `t` as :class:`Vec3`
        objects. Parameters have to be in range [0, 1].
        """
        for u in t:
            yield self.point(u)

    def derivative(self, t: float) -> tuple[Vec3, Vec3, Vec3]:
        """Returns (point, 1st derivative, 2nd derivative) tuple for parameter `t`
        in range [0, 1] as :class:`Vec3` objects.
        """
        if t < 0.0 or t > 1.0:
            raise ValueError("Parameter t not in range [0, 1]")

        if (1.0 - t) < 5e-6:
            t = 1.0
        pts = self._defpoints
        n = len(pts)
        n0 = n - 1
        point = NULLVEC
        d1 = NULLVEC
        d2 = NULLVEC
        t2 = t * t
        n0_1 = n0 - 1
        if t == 0.0:
            d1 = n0 * (pts[1] - pts[0])
            d2 = n0 * n0_1 * (pts[0] - 2.0 * pts[1] + pts[2])
        for i in range(n):
            tmp_bas = bernstein_basis(n0, i, t)
            point += tmp_bas * pts[i]
            if 0.0 < t < 1.0:
                _1_t = 1.0 - t
                i_n0_t = i - n0 * t
                d1 += i_n0_t / (t * _1_t) * tmp_bas * pts[i]
                d2 += (
                    (i_n0_t * i_n0_t - n0 * t2 - i * (1.0 - 2.0 * t))
                    / (t2 * _1_t * _1_t)
                    * tmp_bas
                    * pts[i]
                )
            if t == 1.0:
                d1 = n0 * (pts[n0] - pts[n0_1])
                d2 = n0 * n0_1 * (pts[n0] - 2 * pts[n0_1] + pts[n0 - 2])
        return point, d1, d2

    def derivatives(
        self, t: Iterable[float]
    ) -> Iterable[tuple[Vec3, Vec3, Vec3]]:
        """Returns multiple (point, 1st derivative, 2nd derivative) tuples for
        parameter vector  `t` as :class:`Vec3` objects.
        Parameters in range [0, 1]
        """
        for u in t:
            yield self.derivative(u)

    def reverse(self) -> Bezier:
        """Returns a new Bèzier-curve with reversed control point order."""
        return Bezier(list(reversed(self.control_points)))

    def transform(self, m: Matrix44) -> Bezier:
        """General transformation interface, returns a new :class:`Bezier` curve.

        Args:
             m: 4x4 transformation matrix (:class:`ezdxf.math.Matrix44`)

        """
        defpoints = tuple(m.transform_vertices(self.control_points))
        return Bezier(defpoints)


def bernstein_basis(n: int, i: int, t: float) -> float:
    # handle the special cases to avoid domain problem with pow
    if t == 0.0 and i == 0:
        ti = 1.0
    else:
        ti = pow(t, i)
    if n == i and t == 1.0:
        tni = 1.0
    else:
        tni = pow((1.0 - t), (n - i))
    Ni = factorial(n) / (factorial(i) * factorial(n - i))
    return Ni * ti * tni


@lru_cache(maxsize=None)
def factorial(n: int):
    return math.factorial(n)
