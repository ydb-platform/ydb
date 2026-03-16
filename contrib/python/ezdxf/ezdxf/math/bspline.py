# Copyright (c) 2012-2025, Manfred Moitzi
# License: MIT License
"""
B-Splines
=========

https://www.cl.cam.ac.uk/teaching/2000/AGraphHCI/SMEG/node4.html

Rational B-splines
==================

https://www.cl.cam.ac.uk/teaching/2000/AGraphHCI/SMEG/node5.html:

"The NURBS Book" by Les Piegl and Wayne Tiller

https://books.google.at/books/about/The_NURBS_Book.html?id=7dqY5dyAwWkC&redir_esc=y

"""
from __future__ import annotations
from typing import (
    Iterable,
    Iterator,
    Sequence,
    TYPE_CHECKING,
    Optional,
    Any,
    no_type_check,
)
import math
import numpy as np

from ezdxf.math import (
    Vec3,
    UVec,
    NULLVEC,
    Basis,
    Evaluator,
    create_t_vector,
    estimate_end_tangent_magnitude,
    distance_point_line_3d,
    arc_angle_span_deg,
)
from .bbox import BoundingBox
from ezdxf.math import linalg
from ezdxf.lldxf.const import DXFValueError

if TYPE_CHECKING:
    from ezdxf.math import (
        ConstructionArc,
        ConstructionEllipse,
        Matrix44,
        Bezier4P,
    )


__all__ = [
    # High level functions:
    "fit_points_to_cad_cv",
    "global_bspline_interpolation",
    "local_cubic_bspline_interpolation",
    "rational_bspline_from_arc",
    "rational_bspline_from_ellipse",
    "fit_points_to_cubic_bezier",
    "open_uniform_bspline",
    "closed_uniform_bspline",
    # B-spline representation with derivatives support:
    "BSpline",
    # Low level interpolation function:
    "unconstrained_global_bspline_interpolation",
    "global_bspline_interpolation_end_tangents",
    "cad_fit_point_interpolation",
    "global_bspline_interpolation_first_derivatives",
    "local_cubic_bspline_interpolation_from_tangents",
    # Low level knot parametrization functions:
    "knots_from_parametrization",
    "averaged_knots_unconstrained",
    "averaged_knots_constrained",
    "natural_knots_unconstrained",
    "natural_knots_constrained",
    "double_knots",
    # Low level knot function:
    "required_knot_values",
    "uniform_knot_vector",
    "open_uniform_knot_vector",
    "required_fit_points",
    "required_control_points",
    "round_knots",
]


def fit_points_to_cad_cv(
    fit_points: Iterable[UVec],
    tangents: Optional[Iterable[UVec]] = None,
) -> BSpline:
    """Returns a cubic :class:`BSpline` from fit points as close as possible
    to common CAD applications like BricsCAD.

    There exist infinite numerical correct solution for this setup, but some
    facts are known:

    - CAD applications use the global curve interpolation with start- and end
      derivatives if the end tangents are defined otherwise the equation system will
      be completed by setting the second derivatives of the start and end point to 0,
      for more information read this answer on stackoverflow: https://stackoverflow.com/a/74863330/6162864
    - The degree of the B-spline is always 3 regardless which degree is stored in the
      SPLINE entity, this is only valid for B-splines defined by fit points
    - Knot parametrization method is "chord"
    - Knot distribution is "natural"

    Args:
        fit_points: points the spline is passing through
        tangents: start- and end tangent, default is autodetect

    """
    # See also Spline class in ezdxf/entities/spline.py:
    points = Vec3.list(fit_points)
    if len(points) < 2:
        raise ValueError("two or more points required ")

    if tangents is None:
        control_points, knots = cad_fit_point_interpolation(points)
        return BSpline(control_points, order=4, knots=knots)
    t = Vec3.list(tangents)
    m1, m2 = estimate_end_tangent_magnitude(points, method="chord")
    start_tangent = t[0].normalize(m1)
    end_tangent = t[-1].normalize(m2)

    return global_bspline_interpolation(
        points,
        degree=3,
        tangents=(start_tangent, end_tangent),
        method="chord",
    )


def fit_points_to_cubic_bezier(fit_points: Iterable[UVec]) -> BSpline:
    """Returns a cubic :class:`BSpline` from fit points **without** end
    tangents.

    This function uses the cubic Bèzier interpolation to create multiple Bèzier
    curves and combine them into a single B-spline, this works for short simple
    splines better than the :func:`fit_points_to_cad_cv`, but is worse
    for longer and more complex splines.

    Args:
        fit_points: points the spline is passing through

    """
    points = Vec3.list(fit_points)
    if len(points) < 2:
        raise ValueError("two or more points required ")

    from ezdxf.math import cubic_bezier_interpolation, bezier_to_bspline

    bezier_curves = cubic_bezier_interpolation(points)
    return bezier_to_bspline(bezier_curves)


def global_bspline_interpolation(
    fit_points: Iterable[UVec],
    degree: int = 3,
    tangents: Optional[Iterable[UVec]] = None,
    method: str = "chord",
) -> BSpline:
    """`B-spline`_ interpolation by the `Global Curve Interpolation`_.
    Given are the fit points and the degree of the B-spline.
    The function provides 3 methods for generating the parameter vector t:

    - "uniform": creates a uniform t vector, from 0 to 1 evenly spaced, see
      `uniform`_ method
    - "chord", "distance": creates a t vector with values proportional to the
      fit point distances, see `chord length`_ method
    - "centripetal", "sqrt_chord": creates a t vector with values proportional
      to the fit point sqrt(distances), see `centripetal`_ method
    - "arc": creates a t vector with values proportional to the arc length
      between fit points.

    It is possible to constraint the curve by tangents, by start- and end
    tangent if only two tangents are given or by one tangent for each fit point.

    If tangents are given, they represent 1st derivatives and should be
    scaled if they are unit vectors, if only start- and end tangents given the
    function :func:`~ezdxf.math.estimate_end_tangent_magnitude` helps with an
    educated guess, if all tangents are given, scaling by chord length is a
    reasonable choice (Piegl & Tiller).

    Args:
        fit_points: fit points of B-spline, as list of :class:`Vec3` compatible
            objects
        tangents: if only two vectors are given, take the first and the last
            vector as start- and end tangent constraints or if for all fit
            points a tangent is given use all tangents as interpolation
            constraints (optional)
        degree: degree of B-spline
        method: calculation method for parameter vector t

    Returns:
        :class:`BSpline`

    """
    _fit_points = Vec3.list(fit_points)
    count = len(_fit_points)
    order: int = degree + 1

    if tangents:
        # two control points for tangents will be added
        count += 2
    if order > count and tangents is None:
        raise ValueError(f"More fit points required for degree {degree}")

    t_vector = list(create_t_vector(_fit_points, method))
    # natural knot generation for uneven degrees else averaged
    knot_generation_method = "natural" if degree % 2 else "average"
    if tangents is not None:
        _tangents = Vec3.list(tangents)
        if len(_tangents) == 2:
            control_points, knots = global_bspline_interpolation_end_tangents(
                _fit_points,
                _tangents[0],
                _tangents[1],
                degree,
                t_vector,
                knot_generation_method,
            )
        elif len(_tangents) == len(_fit_points):
            (
                control_points,
                knots,
            ) = global_bspline_interpolation_first_derivatives(
                _fit_points, _tangents, degree, t_vector
            )
        else:
            raise ValueError(
                "Invalid count of tangents, two tangents as start- and end "
                "tangent constrains or one tangent for each fit point."
            )
    else:
        control_points, knots = unconstrained_global_bspline_interpolation(
            _fit_points, degree, t_vector, knot_generation_method
        )
    bspline = BSpline(control_points, order=order, knots=knots)
    return bspline


def local_cubic_bspline_interpolation(
    fit_points: Iterable[UVec],
    method: str = "5-points",
    tangents: Optional[Iterable[UVec]] = None,
) -> BSpline:
    """`B-spline`_ interpolation by 'Local Cubic Curve Interpolation', which
    creates B-spline from fit points and estimated tangent direction at start-,
    end- and passing points.

    Source: Piegl & Tiller: "The NURBS Book" - chapter 9.3.4

    Available tangent estimation methods:

    - "3-points": 3 point interpolation
    - "5-points": 5 point interpolation
    - "bezier": cubic bezier curve interpolation
    - "diff": finite difference

    or pass pre-calculated tangents, which overrides tangent estimation.

    Args:
        fit_points: all B-spline fit points as :class:`Vec3` compatible objects
        method: tangent estimation method
        tangents: tangents as :class:`Vec3` compatible objects (optional)

    Returns:
        :class:`BSpline`

    """
    from .parametrize import estimate_tangents

    _fit_points = Vec3.list(fit_points)
    if tangents:
        _tangents = Vec3.list(tangents)
    else:
        _tangents = estimate_tangents(_fit_points, method)
    control_points, knots = local_cubic_bspline_interpolation_from_tangents(
        _fit_points, _tangents
    )
    return BSpline(control_points, order=4, knots=knots)


def required_knot_values(count: int, order: int) -> int:
    """Returns the count of required knot-values for a B-spline of `order` and
    `count` control points.

    Args:
        count: count of control points, in text-books referred as "n + 1"
        order: order of B-Spline, in text-books referred as "k"

    Relationship:

    "p" is the degree of the B-spline, text-book notation.

    - k = p + 1
    - 2 ≤ k ≤ n + 1

    """
    k = int(order)
    n = int(count) - 1
    p = k - 1
    if not (2 <= k <= (n + 1)):
        raise DXFValueError("Invalid count/order combination")
    # n + p + 2 = count + order
    return n + p + 2


def required_fit_points(order: int, tangents=True) -> int:
    """Returns the count of required fit points to calculate the spline
    control points.

    Args:
        order: spline order (degree + 1)
        tangents: start- and end tangent are given or estimated

    """
    if tangents:
        # If tangents are given or estimated two points for start- and end
        # tangent will be added automatically for the global bspline
        # interpolation. see function fit_points_to_cad_cv()
        order -= 2
    # required condition: order > count, see global_bspline_interpolation()
    return max(order, 2)


def required_control_points(order: int) -> int:
    """Returns the required count of control points for a valid B-spline.

    Args:
        order: spline order (degree + 1)

    Required condition: 2 <= order <= count, therefore:  count >= order

    """
    return max(order, 2)


def normalize_knots(knots: Sequence[float]) -> list[float]:
    """Normalize knot vector into range [0, 1]."""
    min_val = knots[0]
    max_val = knots[-1] - min_val
    return [(v - min_val) / max_val for v in knots]


def uniform_knot_vector(count: int, order: int, normalize=False) -> list[float]:
    """Returns an uniform knot vector for a B-spline of `order` and `count`
    control points.

    `order` = degree + 1

    Args:
        count: count of control points
        order: spline order
        normalize: normalize values in range [0, 1] if ``True``

    """
    if normalize:
        max_value = float(count + order - 1)
    else:
        max_value = 1.0
    return [knot_value / max_value for knot_value in range(count + order)]


def open_uniform_knot_vector(count: int, order: int, normalize=False) -> list[float]:
    """Returns an open (clamped) uniform knot vector for a B-spline of `order`
    and `count` control points.

    `order` = degree + 1

    Args:
        count: count of control points
        order: spline order
        normalize: normalize values in range [0, 1] if ``True``

    """
    k = count - order
    if normalize:
        max_value = float(count - order + 1)
        tail = [1.0] * order
    else:
        max_value = 1.0
        tail = [1.0 + k] * order

    knots = [0.0] * order
    knots.extend((1.0 + v) / max_value for v in range(k))
    knots.extend(tail)
    return knots


def knots_from_parametrization(
    n: int, p: int, t: Iterable[float], method="average", constrained=False
) -> list[float]:
    """Returns a 'clamped' knot vector for B-splines. All knot values are
    normalized in the range [0, 1].

    Args:
        n: count fit points - 1
        p: degree of spline
        t: parametrization vector, length(t_vector) == n, normalized [0, 1]
        method: "average", "natural"
        constrained: ``True`` for B-spline constrained by end derivatives

    Returns:
        List of n+p+2 knot values as floats

    """
    order = int(p + 1)
    if order > (n + 1):
        raise DXFValueError("Invalid n/p combination, more fit points required.")

    t = [float(v) for v in t]
    if t[0] != 0.0 or not math.isclose(t[-1], 1.0):
        raise ValueError("Parametrization vector t has to be normalized.")

    if method == "average":
        return (
            averaged_knots_constrained(n, p, t)
            if constrained
            else averaged_knots_unconstrained(n, p, t)
        )
    elif method == "natural":
        return (
            natural_knots_constrained(n, p, t)
            if constrained
            else natural_knots_unconstrained(n, p, t)
        )
    else:
        raise ValueError(f"Unknown knot generation method: {method}")


def averaged_knots_unconstrained(n: int, p: int, t: Sequence[float]) -> list[float]:
    """Returns an averaged knot vector from parametrization vector `t` for an
    unconstrained B-spline.

    Args:
        n: count of control points - 1
        p: degree
        t: parametrization vector, normalized [0, 1]

    """
    assert t[0] == 0.0
    assert math.isclose(t[-1], 1.0)

    knots = [0.0] * (p + 1)
    knots.extend(sum(t[j : j + p]) / p for j in range(1, n - p + 1))
    if knots[-1] > 1.0:
        raise ValueError("Normalized [0, 1] values required")
    knots.extend([1.0] * (p + 1))
    return knots


def averaged_knots_constrained(n: int, p: int, t: Sequence[float]) -> list[float]:
    """Returns an averaged knot vector from parametrization vector `t` for a
    constrained B-spline.

    Args:
        n: count of control points - 1
        p: degree
        t: parametrization vector, normalized [0, 1]

    """
    assert t[0] == 0.0
    assert math.isclose(t[-1], 1.0)

    knots = [0.0] * (p + 1)
    knots.extend(sum(t[j : j + p - 1]) / p for j in range(n - p))
    knots.extend([1.0] * (p + 1))
    return knots


def natural_knots_unconstrained(n: int, p: int, t: Sequence[float]) -> list[float]:
    """Returns a 'natural' knot vector from parametrization vector `t` for an
    unconstrained B-spline.

    Args:
        n: count of control points - 1
        p: degree
        t: parametrization vector, normalized [0, 1]

    """
    assert t[0] == 0.0
    assert math.isclose(t[-1], 1.0)

    knots = [0.0] * (p + 1)
    knots.extend(t[2 : n - p + 2])
    knots.extend([1.0] * (p + 1))
    return knots


def natural_knots_constrained(n: int, p: int, t: Sequence[float]) -> list[float]:
    """Returns a 'natural' knot vector from parametrization vector `t` for a
    constrained B-spline.

    Args:
        n: count of control points - 1
        p: degree
        t: parametrization vector, normalized [0, 1]

    """
    assert t[0] == 0.0
    assert math.isclose(t[-1], 1.0)

    knots = [0.0] * (p + 1)
    knots.extend(t[1 : n - p + 1])
    knots.extend([1.0] * (p + 1))
    return knots


def double_knots(n: int, p: int, t: Sequence[float]) -> list[float]:
    """Returns a knot vector from parametrization vector `t` for B-spline
    constrained by first derivatives at all fit points.

    Args:
        n: count of fit points - 1
        p: degree of spline
        t: parametrization vector, first value has to be 0.0 and last value has
            to be 1.0

    """
    assert t[0] == 0.0
    assert math.isclose(t[-1], 1.0)

    u = [0.0] * (p + 1)
    prev_t = 0.0

    u1 = []
    for t1 in t[1:-1]:
        if p == 2:
            # add one knot between prev_t and t
            u1.append((prev_t + t1) / 2.0)
            u1.append(t1)
        else:
            if prev_t == 0.0:  # first knot
                u1.append(t1 / 2)
            else:
                # add one knot at the 1st third and one knot
                # at the 2nd third between prev_t and t.
                u1.append((2 * prev_t + t1) / 3.0)
                u1.append((prev_t + 2 * t1) / 3.0)
        prev_t = t1
    u.extend(u1[: n * 2 - p])
    u.append((t[-2] + 1.0) / 2.0)  # last knot
    u.extend([1.0] * (p + 1))
    return u


def _get_best_solver(matrix: list | linalg.Matrix, degree: int) -> linalg.Solver:
    """Returns best suited linear equation solver depending on matrix configuration and
    python interpreter.
    """
    # v1.2: added NumpySolver
    #   Acceleration of banded diagonal matrix solver is still a thing but only for
    #   really big matrices N > 30 in pure Python and N > 20 for C-extension np_support
    # PyPy has no advantages when using the NumpySolver
    if not isinstance(matrix, linalg.Matrix):
        matrix = linalg.Matrix(matrix)

    if matrix.nrows < 20:  # use default equation solver
        return linalg.NumpySolver(matrix.matrix)
    else:
        # Theory: band parameters m1, m2 are at maximum degree-1, for
        # B-spline interpolation and approximation:
        # m1 = m2 = degree-1
        # But the speed gain is not that big and just to be sure:
        m1, m2 = linalg.detect_banded_matrix(matrix, check_all=False)
        A = linalg.compact_banded_matrix(matrix, m1, m2)
        return linalg.BandedMatrixLU(A, m1, m2)


def unconstrained_global_bspline_interpolation(
    fit_points: Sequence[UVec],
    degree: int,
    t_vector: Sequence[float],
    knot_generation_method: str = "average",
) -> tuple[list[Vec3], list[float]]:
    """Interpolates the control points for a B-spline by global interpolation
    from fit points without any constraints.

    Source: Piegl & Tiller: "The NURBS Book" - chapter 9.2.1

    Args:
        fit_points: points the B-spline has to pass
        degree: degree of spline >= 2
        t_vector: parametrization vector, first value has to be 0 and last
            value has to be 1
        knot_generation_method: knot generation method from parametrization
            vector, "average" or "natural"

    Returns:
        2-tuple of control points as list of Vec3 objects and the knot vector
        as list of floats

    """
    # Source: http://pages.mtu.edu/~shene/COURSES/cs3621/NOTES/INT-APP/CURVE-INT-global.html
    knots = knots_from_parametrization(
        len(fit_points) - 1,
        degree,
        t_vector,
        knot_generation_method,
        constrained=False,
    )
    N = Basis(knots=knots, order=degree + 1, count=len(fit_points))
    solver = _get_best_solver([N.basis_vector(t) for t in t_vector], degree)
    mat_B = np.array(fit_points, dtype=np.float64)
    control_points = solver.solve_matrix(mat_B)
    return Vec3.list(control_points.rows()), knots


def global_bspline_interpolation_end_tangents(
    fit_points: list[Vec3],
    start_tangent: Vec3,
    end_tangent: Vec3,
    degree: int,
    t_vector: Sequence[float],
    knot_generation_method: str = "average",
) -> tuple[list[Vec3], list[float]]:
    """Calculates the control points for a B-spline by global interpolation
    from fit points and the 1st derivative of the start- and end point as constraints.
    These 'tangents' are 1st derivatives and not unit vectors, if an estimation
    of the magnitudes is required use the :func:`estimate_end_tangent_magnitude`
    function.

    Source: Piegl & Tiller: "The NURBS Book" - chapter 9.2.2

    Args:
        fit_points: points the B-spline has to pass
        start_tangent: 1st derivative as start constraint
        end_tangent: 1st derivative as end constrain
        degree: degree of spline >= 2
        t_vector: parametrization vector, first value has to be 0 and last
            value has to be 1
        knot_generation_method: knot generation method from parametrization
            vector, "average" or "natural"

    Returns:
        2-tuple of control points as list of Vec3 objects and the knot vector
        as list of floats

    """
    n = len(fit_points) - 1
    p = degree
    if degree > 3:
        # todo: 'average' produces weird results for degree > 3, 'natural' is
        #  better but also not good
        knot_generation_method = "natural"
    knots = knots_from_parametrization(
        n + 2, p, t_vector, knot_generation_method, constrained=True
    )

    N = Basis(knots=knots, order=p + 1, count=n + 3)
    rows = [N.basis_vector(u) for u in t_vector]
    spacing = [0.0] * (n + 1)
    rows.insert(1, [-1.0, +1.0] + spacing)
    rows.insert(-1, spacing + [-1.0, +1.0])
    fit_points.insert(1, start_tangent * (knots[p + 1] / p))
    fit_points.insert(-1, end_tangent * ((1.0 - knots[-(p + 2)]) / p))

    solver = _get_best_solver(rows, degree)
    control_points = solver.solve_matrix(fit_points)
    return Vec3.list(control_points.rows()), knots


def cad_fit_point_interpolation(
    fit_points: list[Vec3],
) -> tuple[list[Vec3], list[float]]:
    """Calculates the control points for a B-spline by global interpolation
    from fit points without any constraints in the same way as AutoCAD and BricsCAD.

    Source: https://stackoverflow.com/a/74863330/6162864

    Args:
        fit_points: points the B-spline has to pass

    Returns:
        2-tuple of control points as list of Vec3 objects and the knot vector
        as list of floats

    """

    def coefficients1() -> list[float]:
        """Returns the coefficients for equation [1]."""
        # Piegl & Tiller: "The NURBS Book" formula (3.9)
        up1 = knots[p + 1]
        up2 = knots[p + 2]
        f = p * (p - 1) / up1
        return [
            f / up1,  # P0
            -f * (up1 + up2) / (up1 * up2),  # P1
            f / up2,  # P2
        ]

    def coefficients2() -> list[float]:
        """Returns the coefficients for equation [n-1]."""
        # Piegl & Tiller: "The NURBS Book" formula (3.10)
        m = len(knots) - 1
        ump1 = knots[m - p - 1]
        ump2 = knots[m - p - 2]
        f = p * (p - 1) / (1.0 - ump1)
        return [
            f / (1.0 - ump2),  # Pn-2
            -f * (2.0 - ump1 - ump2) / (1.0 - ump1) / (1.0 - ump2),  # Pn-1
            f / (1.0 - ump1),  # Pn
        ]

    t_vector = list(create_t_vector(fit_points, "chord"))
    n = len(fit_points) - 1
    p = 3
    knots = knots_from_parametrization(
        n + 2, p, t_vector, method="natural", constrained=True
    )

    N = Basis(knots=knots, order=p + 1, count=n + 3)
    rows = [N.basis_vector(u) for u in t_vector]
    spacing = [0.0] * n
    rows.insert(1, coefficients1() + spacing)
    rows.insert(-1, spacing + coefficients2())

    # C"(0) == 0
    fit_points.insert(1, Vec3(0, 0, 0))
    # C"(1) == 0
    fit_points.insert(-1, Vec3(0, 0, 0))

    solver = _get_best_solver(rows, p)
    control_points = solver.solve_matrix(fit_points)
    return Vec3.list(control_points.rows()), knots


def global_bspline_interpolation_first_derivatives(
    fit_points: list[Vec3],
    derivatives: list[Vec3],
    degree: int,
    t_vector: Sequence[float],
) -> tuple[list[Vec3], list[float]]:
    """Interpolates the control points for a B-spline by a global
    interpolation from fit points and 1st derivatives as constraints.

    Source: Piegl & Tiller: "The NURBS Book" - chapter 9.2.4

    Args:
        fit_points: points the B-spline has to pass
        derivatives: 1st derivatives as constrains, not unit vectors!
            Scaling by chord length is a reasonable choice (Piegl & Tiller).
        degree: degree of spline >= 2
        t_vector: parametrization vector, first value has to be 0 and last
            value has to be 1

    Returns:
        2-tuple of control points as list of Vec3 objects and the knot vector
        as list of floats

    """

    def nbasis(t: float):
        span = N.find_span(t)
        front = span - p
        back = count + p + 1 - span
        for basis in N.basis_funcs_derivatives(span, t, n=1):
            yield [0.0] * front + basis + [0.0] * back

    p = degree
    n = len(fit_points) - 1
    knots = double_knots(n, p, t_vector)
    count = len(fit_points) * 2
    N = Basis(knots=knots, order=p + 1, count=count)
    A = [
        [1.0] + [0.0] * (count - 1),  # Q0
        [-1.0, +1.0] + [0.0] * (count - 2),  # D0
    ]
    ncols = len(A[0])
    for f in (nbasis(t) for t in t_vector[1:-1]):
        A.extend([row[:ncols] for row in f])  # Qi, Di
    # swapped equations!
    A.append([0.0] * (count - 2) + [-1.0, +1.0])  # Dn
    A.append([0.0] * (count - 1) + [+1.0])  # Qn
    assert len(set(len(row) for row in A)) == 1, "inhomogeneous matrix detected"

    # Build right handed matrix B
    B: list[Vec3] = []
    for rows in zip(fit_points, derivatives):
        B.extend(rows)  # Qi, Di

    # also swap last rows!
    B[-1], B[-2] = B[-2], B[-1]  # Dn, Qn

    # modify equation for derivatives D0 and Dn
    B[1] *= knots[p + 1] / p
    B[-2] *= (1.0 - knots[-(p + 2)]) / p
    solver = _get_best_solver(A, degree)
    control_points = solver.solve_matrix(B)
    return Vec3.list(control_points.rows()), knots


def local_cubic_bspline_interpolation_from_tangents(
    fit_points: list[Vec3], tangents: list[Vec3]
) -> tuple[list[Vec3], list[float]]:
    """Interpolates the control points for a cubic B-spline by local
    interpolation from fit points and tangents as unit vectors for each fit
    point. Use the :func:`estimate_tangents` function to estimate end tangents.

    Source: Piegl & Tiller: "The NURBS Book" - chapter 9.3.4

    Args:
        fit_points: curve definition points - curve has to pass all given fit
            points
        tangents: one tangent vector for each fit point as unit vectors

    Returns:
        2-tuple of control points as list of Vec3 objects and the knot vector
        as list of floats

    """
    assert len(fit_points) == len(tangents)
    assert len(fit_points) > 2

    degree = 3
    order = degree + 1
    control_points = [fit_points[0]]
    u = 0.0
    params = []
    for i in range(len(fit_points) - 1):
        p0 = fit_points[i]
        p3 = fit_points[i + 1]
        t0 = tangents[i]
        t3 = tangents[i + 1]
        a = 16.0 - (t0 + t3).magnitude_square  # always > 0!
        b = 12.0 * (p3 - p0).dot(t0 + t3)
        c = -36.0 * (p3 - p0).magnitude_square
        try:
            alpha_plus, alpha_minus = linalg.quadratic_equation(a, b, c)
        except ValueError:  # complex solution
            continue
        p1 = p0 + alpha_plus * t0 / 3.0
        p2 = p3 - alpha_plus * t3 / 3.0
        control_points.extend((p1, p2))
        u += 3.0 * (p1 - p0).magnitude
        params.append(u)
    control_points.append(fit_points[-1])

    knots = [0.0] * order
    max_u = params[-1]
    for v in params[:-1]:
        knot = v / max_u
        knots.extend((knot, knot))
    knots.extend([1.0] * 4)

    assert len(knots) == required_knot_values(len(control_points), order)
    return control_points, knots


class BSpline:
    """B-spline construction tool.

    Internal representation of a `B-spline`_ curve. The default configuration
    of the knot vector is a uniform open `knot`_ vector ("clamped").

    Factory functions:

        - :func:`fit_points_to_cad_cv`
        - :func:`fit_points_to_cubic_bezier`
        - :func:`open_uniform_bspline`
        - :func:`closed_uniform_bspline`
        - :func:`rational_bspline_from_arc`
        - :func:`rational_bspline_from_ellipse`
        - :func:`global_bspline_interpolation`
        - :func:`local_cubic_bspline_interpolation`

    Args:
        control_points: iterable of control points as :class:`Vec3` compatible
            objects
        order: spline order (degree + 1)
        knots: iterable of knot values
        weights: iterable of weight values

    """

    __slots__ = ("_control_points", "_basis", "_clamped")

    def __init__(
        self,
        control_points: Iterable[UVec],
        order: int = 4,
        knots: Optional[Iterable[float]] = None,
        weights: Optional[Iterable[float]] = None,
    ):
        self._control_points = Vec3.tuple(control_points)
        count = len(self._control_points)
        order = int(order)
        if order > count:
            raise DXFValueError(
                f"got {count} control points, need {order} or more for order of {order}"
            )

        if knots is None:
            knots = open_uniform_knot_vector(count, order, normalize=True)
        else:
            knots = tuple(knots)
            required_knot_count = count + order
            if len(knots) != required_knot_count:
                raise ValueError(
                    f"{required_knot_count} knot values required, got {len(knots)}."
                )
            if knots[0] != 0.0:
                knots = normalize_knots(knots)
        self._basis = Basis(knots, order, count, weights=weights)
        self._clamped = len(set(knots[:order])) == 1 and len(set(knots[-order:])) == 1

    def __str__(self):
        return (
            f"BSpline degree={self.degree}, {self.count} "
            f"control points, {len(self.knots())} knot values, "
            f"{len(self.weights())} weights"
        )

    @property
    def control_points(self) -> Sequence[Vec3]:
        """Control points as tuple of :class:`~ezdxf.math.Vec3`"""
        return self._control_points

    @property
    def count(self) -> int:
        """Count of control points, (n + 1 in text book notation)."""
        return len(self._control_points)

    @property
    def max_t(self) -> float:
        """Biggest `knot`_ value."""
        return self._basis.max_t

    @property
    def order(self) -> int:
        """Order (k) of B-spline = p + 1"""
        return self._basis.order

    @property
    def degree(self) -> int:
        """Degree (p) of B-spline = order - 1"""
        return self._basis.degree

    @property
    def evaluator(self) -> Evaluator:
        return Evaluator(self._basis, self._control_points)

    @property
    def is_rational(self):
        """Returns ``True`` if curve is a rational B-spline. (has weights)"""
        return self._basis.is_rational

    @property
    def is_clamped(self):
        """Returns ``True`` if curve is a clamped (open) B-spline."""
        return self._clamped

    @staticmethod
    def from_fit_points(points: Iterable[UVec], degree=3, method="chord") -> BSpline:
        """Returns :class:`BSpline` defined by fit points."""
        return global_bspline_interpolation(points, degree, method=method)

    @staticmethod
    def ellipse_approximation(ellipse: ConstructionEllipse, num: int = 16) -> BSpline:
        """Returns an ellipse approximation as :class:`BSpline` with `num`
        control points.

        """
        return global_bspline_interpolation(
            ellipse.vertices(ellipse.params(num)), degree=2
        )

    @staticmethod
    def arc_approximation(arc: ConstructionArc, num: int = 16) -> BSpline:
        """Returns an arc approximation as :class:`BSpline` with `num`
        control points.

        """
        return global_bspline_interpolation(arc.vertices(arc.angles(num)), degree=2)

    @staticmethod
    def from_ellipse(ellipse: ConstructionEllipse) -> BSpline:
        """Returns the ellipse as :class:`BSpline` of 2nd degree with as few
        control points as possible.

        """
        return rational_bspline_from_ellipse(ellipse, segments=1)

    @staticmethod
    def from_arc(arc: ConstructionArc) -> BSpline:
        """Returns the arc as :class:`BSpline` of 2nd degree with as few control
        points as possible.

        """
        return rational_bspline_from_arc(
            arc.center, arc.radius, arc.start_angle, arc.end_angle, segments=1
        )

    @staticmethod
    def from_nurbs_python_curve(curve) -> BSpline:
        """Interface to the `NURBS-Python <https://pypi.org/project/geomdl/>`_
        package.

        Returns a :class:`BSpline` object from a :class:`geomdl.BSpline.Curve`
        object.

        """
        return BSpline(
            control_points=curve.ctrlpts,
            order=curve.order,
            knots=curve.knotvector,
            weights=curve.weights,
        )

    def reverse(self) -> BSpline:
        """Returns a new :class:`BSpline` object with reversed control point
        order.

        """

        def reverse_knots():
            for k in reversed(normalize_knots(self.knots())):
                yield 1.0 - k

        return self.__class__(
            control_points=reversed(self.control_points),
            order=self.order,
            knots=reverse_knots(),
            weights=reversed(self.weights()) if self.is_rational else None,
        )

    def knots(self) -> Sequence[float]:
        """Returns a tuple of `knot`_ values as floats, the knot vector
        **always** has order + count values (n + p + 2 in text book notation).

        """
        return self._basis.knots

    def weights(self) -> Sequence[float]:
        """Returns a tuple of weights values as floats, one for each control
        point or an empty tuple.

        """
        return self._basis.weights

    def approximate(self, segments: int = 20) -> Iterable[Vec3]:
        """Approximates curve by vertices as :class:`Vec3` objects, vertices
        count = segments + 1.

        """
        return self.evaluator.points(self.params(segments))

    def params(self, segments: int) -> Iterable[float]:
        """Yield evenly spaced parameters for given segment count."""
        # works for clamped and unclamped curves
        knots = self.knots()
        lower_bound = knots[self.order - 1]
        upper_bound = knots[self.count]
        return np.linspace(lower_bound, upper_bound, segments + 1)

    def flattening(self, distance: float, segments: int = 4) -> Iterator[Vec3]:
        """Adaptive recursive flattening. The argument `segments` is the
        minimum count of approximation segments between two knots, if the
        distance from the center of the approximation segment to the curve is
        bigger than `distance` the segment will be subdivided.

        Args:
            distance: maximum distance from the projected curve point onto the
                segment chord.
            segments: minimum segment count between two knots

        """

        def subdiv(s: Vec3, e: Vec3, start_t: float, end_t: float):
            mid_t = (start_t + end_t) * 0.5
            m = evaluator.point(mid_t)
            try:
                _dist = distance_point_line_3d(m, s, e)
            except ZeroDivisionError:  # s == e
                _dist = 0
            if _dist < distance:
                yield e
            else:
                yield from subdiv(s, m, start_t, mid_t)
                yield from subdiv(m, e, mid_t, end_t)

        evaluator = self.evaluator
        knots = np.unique(np.array(self.knots()))
        seg_f64 = np.float64(segments)
        t = knots[0]
        start_point = evaluator.point(t)
        yield start_point
        for t1 in knots[1:]:
            delta = (t1 - t) / seg_f64
            while t < t1:
                next_t = t + delta
                if np.isclose(next_t, t1):
                    next_t = t1
                end_point = evaluator.point(next_t)
                yield from subdiv(start_point, end_point, t, next_t)
                t = next_t
                start_point = end_point

    def point(self, t: float) -> Vec3:
        """Returns point  for parameter `t`.

        Args:
            t: parameter in range [0, max_t]

        """
        return self.evaluator.point(t)

    def points(self, t: Iterable[float]) -> Iterable[Vec3]:
        """Yields points for parameter vector `t`.

        Args:
            t: parameters in range [0, max_t]

        """
        return self.evaluator.points(t)

    def derivative(self, t: float, n: int = 2) -> list[Vec3]:
        """Return point and derivatives up to `n` <= degree for parameter `t`.

        e.g. n=1 returns point and 1st derivative.

        Args:
            t: parameter in range [0, max_t]
            n: compute all derivatives up to n <= degree

        Returns:
            n+1 values as :class:`Vec3` objects

        """
        return self.evaluator.derivative(t, n)

    def derivatives(self, t: Iterable[float], n: int = 2) -> Iterable[list[Vec3]]:
        """Yields points and derivatives up to `n` <= degree for parameter
        vector `t`.

        e.g. n=1 returns point and 1st derivative.

        Args:
            t: parameters in range [0, max_t]
            n: compute all derivatives up to n <= degree

        Returns:
            List of n+1 values as :class:`Vec3` objects

        """
        return self.evaluator.derivatives(t, n)

    def insert_knot(self, t: float) -> BSpline:
        """Insert an additional knot, without altering the shape of the curve.
        Returns a new :class:`BSpline` object.

        Args:
            t: position of new knot 0 < t < max_t

        """
        if self._basis.is_rational:
            return self._insert_knot_rational(t)

        knots = list(self._basis.knots)
        cpoints = list(self._control_points)
        p = self.degree

        def new_point(index: int) -> Vec3:
            a = (t - knots[index]) / (knots[index + p] - knots[index])
            return cpoints[index - 1] * (1 - a) + cpoints[index] * a

        if t <= 0.0 or t >= self.max_t:
            raise DXFValueError("Invalid position t")

        k = self._basis.find_span(t)
        if k < p:
            raise DXFValueError("Invalid position t")

        cpoints[k - p + 1 : k] = [new_point(i) for i in range(k - p + 1, k + 1)]
        knots.insert(k + 1, t)  # knot[k] <= t < knot[k+1]
        return BSpline(cpoints, self.order, knots)

    def _insert_knot_rational(self, t: float) -> BSpline:
        """Knot insertion for rational B-splines."""
        if not self._basis.is_rational:
            raise TypeError("Requires a rational B-splines.")

        knots: list[float] = list(self._basis.knots)
        # homogeneous point representation: (x*w, y*w, z*w, w)
        hg_points: np.typing.NDArray = to_homogeneous_points(self)
        p: int = self.degree

        def new_point(index: int) -> np.typing.NDArray:
            a: float = (t - knots[index]) / (knots[index + p] - knots[index])
            return hg_points[index - 1] * (1 - a) + hg_points[index] * a

        if t <= 0.0 or t >= self.max_t:
            raise DXFValueError("Invalid position t")

        k: int = self._basis.find_span(t)
        if k < p:
            raise DXFValueError("Invalid position t")
        new_points: Any = hg_points.tolist()
        new_points[k - p + 1 : k] = [new_point(i) for i in range(k - p + 1, k + 1)]

        points, weights = from_homogeneous_points(new_points)
        knots.insert(k + 1, t)  # knot[k] <= t < knot[k+1]
        return BSpline(points, self.order, knots=knots, weights=weights)

    def knot_refinement(self, u: Iterable[float]) -> BSpline:
        """Insert multiple knots, without altering the shape of the curve.
        Returns a new :class:`BSpline` object.

        Args:
            u: vector of new knots t and for each t: 0 < t  < max_t

        """
        spline = self
        for t in u:
            spline = spline.insert_knot(t)
        return spline

    def transform(self, m: Matrix44) -> BSpline:
        """Returns a new :class:`BSpline` object transformed by a
        :class:`Matrix44` transformation matrix.

        """
        cpoints = m.transform_vertices(self.control_points)
        return BSpline(cpoints, self.order, self.knots(), self.weights())

    def bezier_decomposition(self) -> Iterable[list[Vec3]]:
        """Decompose a non-rational B-spline into multiple Bézier curves.

        This is the preferred method to represent the most common non-rational
        B-splines of 3rd degree by cubic Bézier curves, which are often supported
        by render backends.

        Returns:
            Yields control points of Bézier curves, each Bézier segment
            has degree+1 control points e.g. B-spline of 3rd degree yields
            cubic Bézier curves of 4 control points.

        """
        # Source: "The NURBS Book": Algorithm A5.6
        if self._basis.is_rational:
            raise TypeError("Rational B-splines not supported.")
        if not self.is_clamped:
            raise TypeError("Clamped B-Spline required.")

        n = self.count - 1
        p = self.degree
        knots = self._basis.knots  # U
        control_points = self._control_points  # Pw
        alphas = [0.0] * len(knots)

        m = n + p + 1
        a = p
        b = p + 1
        bezier_points = list(control_points[0 : p + 1])  # Qw

        while b < m:
            next_bezier_points = [NULLVEC] * (p + 1)
            i = b
            while b < m and math.isclose(knots[b + 1], knots[b]):
                b += 1
            mult = b - i + 1
            if mult < p:
                numer = knots[b] - knots[a]
                for j in range(p, mult, -1):
                    alphas[j - mult - 1] = numer / (knots[a + j] - knots[a])
                r = p - mult
                for j in range(1, r + 1):
                    save = r - j
                    s = mult + j
                    for k in range(p, s - 1, -1):
                        alpha = alphas[k - s]
                        bezier_points[k] = bezier_points[k] * alpha + bezier_points[
                            k - 1
                        ] * (1.0 - alpha)
                    if b < m:
                        next_bezier_points[save] = bezier_points[p]
            yield bezier_points

            if b < m:
                for i in range(p - mult, p + 1):
                    next_bezier_points[i] = control_points[b - p + i]
                a = b
                b += 1
                bezier_points = next_bezier_points

    def cubic_bezier_approximation(
        self, level: int = 3, segments: Optional[int] = None
    ) -> Iterable[Bezier4P]:
        """Approximate arbitrary B-splines (degree != 3 and/or rational) by
        multiple segments of cubic Bézier curves. The choice of cubic Bézier
        curves is based on the widely support of this curves by many render
        backends. For cubic non-rational B-splines, which is maybe the most
        common used B-spline, is :meth:`bezier_decomposition` the better choice.

        1. approximation by `level`: an educated guess, the first level of
           approximation segments is based on the count of control points
           and their distribution along the B-spline, every additional level
           is a subdivision of the previous level.

        E.g. a B-Spline of 8 control points has 7 segments at the first level,
        14 at the 2nd level and 28 at the 3rd level, a level >= 3 is recommended.

        2. approximation by a given count of evenly distributed approximation
           segments.

        Args:
            level: subdivision level of approximation segments (ignored if
                argument `segments` is not ``None``)
            segments: absolute count of approximation segments

        Returns:
            Yields control points of cubic Bézier curves as :class:`Bezier4P`
            objects

        """
        if segments is None:
            points = list(self.points(self.approximation_params(level)))
        else:
            points = list(self.approximate(segments))
        from .bezier_interpolation import cubic_bezier_interpolation

        return cubic_bezier_interpolation(points)

    def approximation_params(self, level: int = 3) -> Sequence[float]:
        """Returns an educated guess, the first level of approximation
        segments is based on the count of control points and their distribution
        along the B-spline, every additional level is a subdivision of the
        previous level.

        E.g. a B-Spline of 8 control points has 7 segments at the first level,
        14 at the 2nd level and 28 at the 3rd level.

        """
        params = list(create_t_vector(self._control_points, "chord"))
        if len(params) == 0:
            return params
        if self.max_t != 1.0:
            max_t = self.max_t
            params = [p * max_t for p in params]
        for _ in range(level - 1):
            params = list(subdivide_params(params))
        return params

    def degree_elevation(self, t: int) -> BSpline:
        """Returns a new :class:`BSpline` with a t-times elevated degree.

        Degree elevation increases the degree of a curve without changing the shape of the
        curve. This method implements the algorithm A5.9 of the "The NURBS Book" by
        Piegl & Tiller.

        .. versionadded:: 1.4

        """
        return degree_elevation(self, t)

    def point_inversion(
        self, point: UVec, *, epsilon=1e-8, max_iterations=100, init=8
    ) -> float:
        """Returns the parameter t for a point on the curve that is closest to the input
        point.

        This is an iterative search using Newton's method, so there is no guarantee
        of success, especially for splines with many turns.

        Args:
            point(UVec): point on the curve or near the curve
            epsilon(float): desired precision (distance input point to point on curve)
            max_iterations(int): max iterations for Newton's method
            init(int): number of points to calculate in the initialization phase

        .. versionadded:: 1.4

        """
        return point_inversion(
            self, Vec3(point), epsilon=epsilon, max_iterations=max_iterations, init=init
        )

    def measure(self, segments: int = 100) -> Measurement:
        """Returns a B-spline measurement tool.

        All measurements are based on the approximated curve.

        Args:
            segments: count of segments for B-spline approximation.

        .. versionadded:: 1.4

        """
        return Measurement(self, segments)

    def split(self, t: float) -> tuple[BSpline, BSpline]:
        """Splits the B-spline at parameter `t` and returns two new B-splines.

        Raises:
            ValuerError: t out of range 0 < t < max_t

        .. versionadded:: 1.4

        """

        return split_bspline(self, t)


def subdivide_params(p: list[float]) -> Iterable[float]:
    for i in range(len(p) - 1):
        yield p[i]
        yield (p[i] + p[i + 1]) / 2.0
    yield p[-1]


def open_uniform_bspline(
    control_points: Iterable[UVec],
    order: int = 4,
    weights: Optional[Iterable[float]] = None,
) -> BSpline:
    """Creates an open uniform (periodic) `B-spline`_ curve (`open curve`_).

    This is an unclamped curve, which means the curve passes none of the
    control points.

    Args:
        control_points: iterable of control points as :class:`Vec3` compatible
            objects
        order: spline order (degree + 1)
        weights: iterable of weight values

    """
    _control_points = Vec3.tuple(control_points)
    knots = uniform_knot_vector(len(_control_points), order, normalize=False)
    return BSpline(control_points, order=order, knots=knots, weights=weights)


def closed_uniform_bspline(
    control_points: Iterable[UVec],
    order: int = 4,
    weights: Optional[Iterable[float]] = None,
) -> BSpline:
    """Creates a closed uniform (periodic) `B-spline`_ curve (`open curve`_).

    This B-spline does not pass any of the control points.

    Args:
        control_points: iterable of control points as :class:`Vec3` compatible
            objects
        order: spline order (degree + 1)
        weights: iterable of weight values

    """
    _control_points = Vec3.list(control_points)
    _control_points.extend(_control_points[: order - 1])
    if weights is not None:
        weights = list(weights)
        weights.extend(weights[: order - 1])
    return open_uniform_bspline(_control_points, order, weights)


def rational_bspline_from_arc(
    center: Vec3 = (0, 0),
    radius: float = 1,
    start_angle: float = 0,
    end_angle: float = 360,
    segments: int = 1,
) -> BSpline:
    """Returns a rational B-splines for a circular 2D arc.

    Args:
        center: circle center as :class:`Vec3` compatible object
        radius: circle radius
        start_angle: start angle in degrees
        end_angle: end angle in degrees
        segments: count of spline segments, at least one segment for each
            quarter (90 deg), default is 1, for as few as needed.

    """
    center = Vec3(center)
    radius = float(radius)

    start_rad = math.radians(start_angle % 360)
    end_rad = start_rad + math.radians(arc_angle_span_deg(start_angle, end_angle))
    control_points, weights, knots = nurbs_arc_parameters(start_rad, end_rad, segments)
    return BSpline(
        control_points=(center + (p * radius) for p in control_points),
        weights=weights,
        knots=knots,
        order=3,
    )


PI_2 = math.pi / 2.0


def rational_bspline_from_ellipse(
    ellipse: ConstructionEllipse, segments: int = 1
) -> BSpline:
    """Returns a rational B-splines for an elliptic arc.

    Args:
        ellipse: ellipse parameters as :class:`~ezdxf.math.ConstructionEllipse`
            object
        segments: count of spline segments, at least one segment for each
            quarter (π/2), default is 1, for as few as needed.

    """
    start_angle = ellipse.start_param % math.tau
    end_angle = start_angle + ellipse.param_span

    def transform_control_points() -> Iterable[Vec3]:
        center = Vec3(ellipse.center)
        x_axis = ellipse.major_axis
        y_axis = ellipse.minor_axis
        for p in control_points:
            yield center + x_axis * p.x + y_axis * p.y

    control_points, weights, knots = nurbs_arc_parameters(
        start_angle, end_angle, segments
    )
    return BSpline(
        control_points=transform_control_points(),
        weights=weights,
        knots=knots,
        order=3,
    )


def nurbs_arc_parameters(start_angle: float, end_angle: float, segments: int = 1):
    """Returns a rational B-spline parameters for a circular 2D arc with center
    at (0, 0) and a radius of 1.

    Args:
        start_angle: start angle in radians
        end_angle: end angle in radians
        segments: count of segments, at least one segment for each quarter (π/2)

    Returns:
        control_points, weights, knots

    """
    # Source: https://www.researchgate.net/publication/283497458_ONE_METHOD_FOR_REPRESENTING_AN_ARC_OF_ELLIPSE_BY_A_NURBS_CURVE/citation/download
    if segments < 1:
        raise ValueError("Invalid argument segments (>= 1).")
    delta_angle = end_angle - start_angle
    arc_count = max(math.ceil(delta_angle / PI_2), segments)

    segment_angle = delta_angle / arc_count
    segment_angle_2 = segment_angle / 2
    arc_weight = math.cos(segment_angle_2)

    # First control point
    control_points = [Vec3(math.cos(start_angle), math.sin(start_angle))]
    weights = [1.0]

    angle = start_angle
    d = 1.0 / math.cos(segment_angle / 2.0)
    for _ in range(arc_count):
        # next control point between points on arc
        angle += segment_angle_2
        control_points.append(Vec3(math.cos(angle) * d, math.sin(angle) * d))
        weights.append(arc_weight)

        # next control point on arc
        angle += segment_angle_2
        control_points.append(Vec3(math.cos(angle), math.sin(angle)))
        weights.append(1.0)

    # Knot vector calculation for B-spline of order=3
    # Clamped B-Spline starts with `order` 0.0 knots and
    # ends with `order` 1.0 knots
    knots = [0.0, 0.0, 0.0]
    step = 1.0 / ((max(len(control_points) + 1, 4) - 4) / 2.0 + 1.0)
    g = step
    while g < 1.0:
        knots.extend((g, g))
        g += step
    knots.extend([1.0] * (required_knot_values(len(control_points), 3) - len(knots)))

    return control_points, weights, knots


def bspline_basis(u: float, index: int, degree: int, knots: Sequence[float]) -> float:
    """B-spline basis_vector function.

    Simple recursive implementation for testing and comparison.

    Args:
        u: curve parameter in range [0, max(knots)]
        index: index of control point
        degree: degree of B-spline
        knots: knots vector

    Returns:
        float: basis_vector value N_i,p(u)

    """
    cache: dict[tuple[int, int], float] = {}
    u = float(u)

    def N(i: int, p: int) -> float:
        try:
            return cache[(i, p)]
        except KeyError:
            if p == 0:
                retval = 1 if knots[i] <= u < knots[i + 1] else 0.0
            else:
                dominator = knots[i + p] - knots[i]
                f1 = (u - knots[i]) / dominator * N(i, p - 1) if dominator else 0.0

                dominator = knots[i + p + 1] - knots[i + 1]
                f2 = (
                    (knots[i + p + 1] - u) / dominator * N(i + 1, p - 1)
                    if dominator
                    else 0.0
                )

                retval = f1 + f2
            cache[(i, p)] = retval
            return retval

    return N(int(index), int(degree))


def bspline_basis_vector(
    u: float, count: int, degree: int, knots: Sequence[float]
) -> list[float]:
    """Create basis_vector vector at parameter u.

    Used with the bspline_basis() for testing and comparison.

    Args:
        u: curve parameter in range [0, max(knots)]
        count: control point count (n + 1)
        degree: degree of B-spline (order = degree + 1)
        knots: knot vector

    Returns:
        list[float]: basis_vector vector, len(basis_vector) == count

    """
    assert len(knots) == (count + degree + 1)
    basis: list[float] = [
        bspline_basis(u, index, degree, knots) for index in range(count)
    ]
    # pick up last point ??? why is this necessary ???
    if math.isclose(u, knots[-1]):
        basis[-1] = 1.0
    return basis


@no_type_check
def degree_elevation(spline: BSpline, t: int) -> BSpline:
    """Returns a new :class:`BSpline` with a t-times elevated degree.

    Degree elevation increases the degree of a curve without changing the shape of the
    curve. This function implements the algorithm A5.9 of the "The NURBS Book" by
    Piegl & Tiller.

    .. versionadded:: 1.4

    """
    # Naming and structure have been retained to facilitate comparison with the original
    # algorithm during debugging.
    t = int(t)
    if t < 1:
        return spline
    p = spline.degree  # degree of spline
    # Pw: control points
    if spline.is_rational:
        # homogeneous point representation (x*w, y*w, z*w, w)
        dim = 4
        Pw = to_homogeneous_points(spline)
    else:
        # non-rational splines: (x, y, z)
        dim = 3
        Pw = np.array(spline.control_points)

    # knot vector:
    U = np.array(spline.knots())

    # n + 1: count of control points (text book definition)
    n = len(Pw) - 1
    m = n + p + 1
    ph = p + t
    ph2 = ph // 2

    # control points of the elevated B-spline
    Qw = np.zeros(shape=(len(Pw) * (2 + t), dim))  # size not known yet???

    # knot vector of the elevated B-spline
    Uh = np.zeros(m * (2 + t))  # size not known yet???

    # coefficients for degree elevating the Bezier segments
    bezalfs = np.zeros(shape=(p + t + 1, p + 1))

    # Bezier control points of the current segment
    bpts = np.zeros(shape=(p + 1, dim))

    # (p+t)th-degree Bezier control points of the current segment
    ebpts = np.zeros(shape=(p + t + 1, dim))

    # leftmost control points of the next Bezier segment
    Nextbpts = np.zeros(shape=(p - 1, dim))

    # knot insertion alphas
    alfs = np.zeros(p - 1)
    bezalfs[0, 0] = 1.0
    bezalfs[ph, p] = 1.0
    binom = linalg.binomial_coefficient
    for i in range(1, ph2 + 1):
        inv = 1.0 / binom(ph, i)
        mpi = min(p, i)
        for j in range(max(0, i - t), mpi + 1):
            bezalfs[i, j] = inv * binom(p, j) * binom(t, i - j)
    for i in range(ph2 + 1, ph):
        mpi = min(p, i)
        for j in range(max(0, i - t), mpi + 1):
            bezalfs[i, j] = bezalfs[ph - i, p - j]
    mh = ph
    kind = ph + 1
    r = -1
    a = p
    b = p + 1
    cind = 1
    ua = U[0]
    Qw[0] = Pw[0]

    # for i in range(0, ph + 1):
    #     Uh[i] = ua
    Uh[: ph + 1] = ua

    # for i in range(0, p + 1):
    #     bpts[i] = Pw[i]
    # initialize first Bezier segment
    bpts[: p + 1] = Pw[: p + 1]

    while b < m:  # big loop thru knot vector
        i = b
        while (b < m) and (math.isclose(U[b], U[b + 1])):
            b += 1
        mul = b - i + 1
        mh = mh + mul + t
        ub = U[b]
        oldr = r
        r = p - mul
        # insert knot u(b) r-times
        if oldr > 0:
            lbz = (oldr + 2) // 2
        else:
            lbz = 1
        if r > 0:
            rbz = ph - (r + 1) // 2
        else:
            rbz = ph
        if r > 0:
            # insert knot to get Bezier segment
            numer = ub - ua
            for k in range(p, mul, -1):
                alfs[k - mul - 1] = numer / (U[a + k] - ua)
            for j in range(1, r + 1):
                save = r - j
                s = mul + j
                for k in range(p, s - 1, -1):
                    bpts[k] = alfs[k - s] * bpts[k] + (1.0 - alfs[k - s]) * bpts[k - 1]
                Nextbpts[save] = bpts[p]
            # end of insert knot
        for i in range(lbz, ph + 1):
            # degree elevate bezier
            # only points lbz, .. ,ph are used below
            ebpts[i] = 0.0
            mpi = min(p, i)
            for j in range(max(0, i - t), mpi + 1):
                ebpts[i] = ebpts[i] + bezalfs[i, j] * bpts[j]
            # end degree elevate bezier
        if oldr > 1:
            # must remove knot u=U[a] oldr times
            first = kind - 2
            last = kind
            den = ub - ua
            bet = (ub - Uh[kind - 1]) / den
            for tr in range(1, oldr):
                # knot removal loop
                i = first
                j = last
                kj = j - kind + 1
                while j - i > tr:
                    # loop and compute new control points for one removal step
                    if i < cind:
                        alf = (ub - Uh[i]) / (ua - Uh[i])
                        Qw[i] = alf * Qw[i] + (1.0 - alf) * Qw[i - 1]
                    if j >= lbz:
                        if j - tr <= kind - ph + oldr:
                            gam = (ub - Uh[j - tr]) / den
                            ebpts[kj] = gam * ebpts[kj] + (1.0 - gam) * ebpts[kj + 1]
                        else:
                            ebpts[kj] = bet * ebpts[kj] + (1.0 - bet) * ebpts[kj + 1]
                    i += 1
                    j -= 1
                    kj -= 1
                first -= 1
                last += 1
            # end of removing knot, u=U[a]
        if a != p:
            # load the knot ua
            # for i in range(0, ph - oldr):
            #     Uh[kind] = ua
            i = ph - oldr
            Uh[kind : kind + i] = ua
            kind += i
        for j in range(lbz, rbz + 1):
            # load control points into Qw
            Qw[cind] = ebpts[j]
            cind += 1
        if b < m:
            # set up for next pass thru loop
            # for j in range(0, r):
            #     bpts[j] = Nextbpts[j]
            bpts[:r] = Nextbpts[:r]
            # for j in range(r, p + 1):
            #     bpts[j] = Pw[b - p + j]
            bpts[r : p + 1] = Pw[b - p + r : b + 1]
            a = b
            b += 1
            ua = ub
        else:  # end knot
            # for i in range(0, ph + 1):
            #     Uh[kind + i] = ub
            Uh[kind : kind + ph + 1] = ub

    nh = mh - ph - 1
    count_cpts = nh + 1  # text book n+1 == count of control points
    order = ph + 1

    weights = None
    cpoints = Qw[:count_cpts, :3]
    if dim == 4:
        # homogeneous point representation (x*w, y*w, z*w, w)
        weights = Qw[:count_cpts, 3]
        cpoints = [p / w for p, w in zip(cpoints, weights)]
        # if weights: ... not supported for numpy arrays
        weights = weights.tolist()
    return BSpline(
        cpoints, order=order, weights=weights, knots=Uh[: count_cpts + order]
    )


def to_homogeneous_points(spline: BSpline) -> np.typing.NDArray:
    weights = np.array(spline.weights(), dtype=np.float64)
    if not len(weights):
        weights = np.ones(spline.count)

    return np.array(
        [(v.x * w, v.y * w, v.z * w, w) for v, w in zip(spline.control_points, weights)]
    )


def from_homogeneous_points(
    hg_points: Iterable[Sequence[float]],
) -> tuple[list[Vec3], list[float]]:
    points: list[Vec3] = []
    weights: list[float] = []
    for point in hg_points:
        w = float(point[3])
        points.append(Vec3(point[:3]) / w)
        weights.append(w)
    return points, weights


def point_inversion(
    spline: BSpline, point: Vec3, *, epsilon=1e-8, max_iterations=100, init=8
) -> float:
    """Returns the parameter t for a point on the curve that is closest to the input
    point.

    This is an iterative search using Newton's method, so there is no guarantee
    of success, especially for splines with many turns.

    Args:
        spline(BSpline): curve
        point(Vec3): point on the curve or near the curve
        epsilon(float): desired precision (distance input point to point on curve)
        max_iterations(int): max iterations for Newton's method
        init(int): number of points to calculate in the initialization phase

    .. versionadded:: 1.4

    """
    max_t = spline.max_t
    prev_distance = float("inf")
    u = max_t / 2

    # Initialization phase
    t = np.linspace(0, max_t, init, endpoint=True)
    chk_points = list(spline.points(t))
    for u1, p in zip(t, chk_points):
        distance = point.distance(p)
        if distance < prev_distance:
            u = float(u1)
            prev_distance = distance

    no_progress_counter: int = 0
    for iteration in range(max_iterations):
        # Evaluate the B-spline curve at the current parameter value
        p, dpdu = spline.derivative(u, n=1)

        # Calculate the difference between the current point and the target point
        diff = p - point

        # Check if the difference is within the desired epsilon
        distance = diff.magnitude
        if distance < epsilon:
            break  # goal reached
        if math.isclose(prev_distance, distance):
            no_progress_counter += 1
            if no_progress_counter > 2:
                break
        else:
            no_progress_counter = 0
        prev_distance = distance

        # Update the parameter value using Newton's method
        u -= diff.dot(dpdu) / dpdu.dot(dpdu)

        # Clamp the parameter value within the valid range
        if u < 0.0:
            u = 0.0
        elif u > max_t:
            u = max_t
    return u


class Measurement:
    """B-spline measurement tool.

    All measurements are based on the approximated curve.

    .. versionadded:: 1.4

    """

    def __init__(self, spline: BSpline, segments: int) -> None:
        if not isinstance(spline, BSpline):
            raise TypeError(f"BSpline instance expected, got {type(spline)}")
        segments = int(segments)
        if segments < 1:
            raise ValueError(f"invalid segment count: {segments}")
        self._spline = spline
        self._parameters = np.linspace(0.0, spline.max_t, segments + 1, endpoint=True)
        points = list(self._spline.points(self._parameters))
        bbox = BoundingBox(points)
        self.extmin = bbox.extmin
        self.extmax = bbox.extmax
        self._distances = self._measure(points)  # distance between points
        self._offsets = np.cumsum(self._distances)  # distance along curve from start

    @staticmethod
    def _measure(points: list[Vec3]) -> np.typing.NDArray:
        prev = points[0]
        distances = np.zeros(len(points), dtype=np.float64)
        for index, point in enumerate(points):
            distances[index] = prev.distance(point)
            prev = point
        return distances

    @property
    def length(self) -> float:
        """Returns the approximated length of the B-spline."""
        return float(self._offsets[-1])

    def distance(self, t: float) -> float:
        """Returns the distance along the curve from the start point for then given
        parameter t.
        """
        if t <= 0.0:
            return 0.0
        if t >= self._spline.max_t:
            return self.length
        params = self._parameters
        index = np.searchsorted(params, t, side="right")
        t1 = params[index]
        t2 = params[index + 1]
        distance = self._distances[index + 1] * (t - t1) / (t2 - t1)
        return float(self._offsets[index] + distance)

    def param_at(self, distance: float) -> float:
        """Returns the parameter t for a given distance along the curve from the
        start point.
        """
        index = np.searchsorted(self._offsets, distance, side="right")
        if index <= 0:
            return 0.0
        params = self._parameters
        if index >= len(params):
            return self._spline.max_t
        prev = index - 1
        prev_distance = distance - self._offsets[prev]
        t1 = params[prev]
        t2 = params[index]
        return float(t1 + (t2 - t1) * (prev_distance / self._distances[index]))

    def divide(self, count: int) -> list[float]:
        """Returns the interpolated B-spline parameters for dividing the curve into
        `count` segments of equal length.

        The method yields only the dividing parameters, the first (0.0) and last parameter
        (max_t) are not included e.g. dividing a B-spline by 3 yields two parameters
        [t1, t2] that divides the B-spline into 3 parts of equal length.

        """
        if count < 1:
            raise ValueError(f"invalid count: {count}")
        total_length: float = self.length
        seg_length = total_length / count
        distance = seg_length
        max_t = self._spline.max_t
        result: list[float] = []
        while distance < total_length:
            t = self.param_at(distance)
            if not math.isclose(max_t, t):
                result.append(t)
            distance += seg_length
        return result


def split_bspline(spline: BSpline, t: float) -> tuple[BSpline, BSpline]:
    """Splits a B-spline at a parameter t.

    Raises:
        ValuerError: t out of range 0 < t < max_t

    .. versionadded:: 1.4

    """
    tol = 1e-12
    if t < tol:
        raise ValueError("t must be greater than 0")
    if t > spline.max_t - tol:
        raise ValueError("t must be smaller than max_t")
    order = spline.order

    # Clamp spline at parameter t
    u = np.full(order, t)
    spline = spline.knot_refinement(u)

    knots = np.array(spline.knots(), dtype=np.float64)
    # Determine the knot span
    span = np.searchsorted(knots, t, side="right")

    # Calculate the new knot vector
    knots1 = knots[:span]
    knots2 = knots[span:]

    # Append the desired parameter value to the knot vector
    knots2 = np.concatenate((u, knots2))

    # Split control points
    points = spline.control_points
    split_index = len(knots1) - order
    points1 = points[:split_index]
    points2 = points[split_index:]
    weights1: Sequence[float] = []
    weights2: Sequence[float] = []
    weights = spline.weights()
    if len(weights):
        weights1 = weights[:split_index]
        weights2 = weights[split_index:]
    return (
        BSpline(points1, order, knots=knots1, weights=weights1),
        BSpline(points2, order, knots=knots2, weights=weights2),
    )


def round_knots(knots: list[float], tolerance: float) -> list[float]:
    """Returns rounded knot-values.

    The `tolerance` defines the minimal difference between two knot values like 1e-9.

    """
    try:
        ndigits = -int(math.log10(tolerance))  # e.g. log10(0.0001) = -4.0
    except ValueError:
        return knots
    if ndigits <= 0:
        return knots
    return [round(k, ndigits=ndigits) for k in knots]
