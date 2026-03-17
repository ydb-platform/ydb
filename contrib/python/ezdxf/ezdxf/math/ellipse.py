# Copyright (c) 2020-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Any
import numpy as np

import math
from ezdxf.math import (
    Vec3,
    UVec,
    NULLVEC,
    X_AXIS,
    Z_AXIS,
    OCS,
    Matrix44,
    arc_angle_span_rad,
    distance_point_line_3d,
    enclosing_angles,
)

if TYPE_CHECKING:
    from ezdxf.layouts import BaseLayout
    from ezdxf.entities import Ellipse

__all__ = [
    "ConstructionEllipse",
    "angle_to_param",
    "param_to_angle",
    "rytz_axis_construction",
]
QUARTER_PARAMS = [0, math.pi * 0.5, math.pi, math.pi * 1.5]
HALF_PI = math.pi / 2.0


class ConstructionEllipse:
    """Construction tool for 3D ellipsis.

    Args:
        center: 3D center point
        major_axis: major axis as 3D vector
        extrusion: normal vector of ellipse plane
        ratio: ratio of minor axis to major axis
        start_param: start param in radians
        end_param: end param in radians
        ccw: is counter-clockwise flag - swaps start- and end param if ``False``

    """

    def __init__(
        self,
        center: UVec = NULLVEC,
        major_axis: UVec = X_AXIS,
        extrusion: UVec = Z_AXIS,
        ratio: float = 1,
        start_param: float = 0,
        end_param: float = math.tau,
        ccw: bool = True,
    ):
        self.center = Vec3(center)
        self.major_axis = Vec3(major_axis)
        if self.major_axis.isclose(NULLVEC):
            raise ValueError(f"Invalid major axis (null vector).")
        self.extrusion = Vec3(extrusion)
        if self.major_axis.isclose(NULLVEC):
            raise ValueError(f"Invalid extrusion vector (null vector).")
        self.ratio = float(ratio)
        self.start_param = float(start_param)
        self.end_param = float(end_param)
        if not ccw:
            self.start_param, self.end_param = self.end_param, self.start_param
        self.minor_axis = minor_axis(self.major_axis, self.extrusion, self.ratio)

    @classmethod
    def from_arc(
        cls,
        center: UVec = NULLVEC,
        radius: float = 1,
        extrusion: UVec = Z_AXIS,
        start_angle: float = 0,
        end_angle: float = 360,
        ccw: bool = True,
    ) -> ConstructionEllipse:
        """Returns :class:`ConstructionEllipse` from arc or circle.

        Arc and Circle parameters defined in OCS.

        Args:
             center: center in OCS
             radius: arc or circle radius
             extrusion: OCS extrusion vector
             start_angle: start angle in degrees
             end_angle: end angle in degrees
             ccw: arc curve goes counter clockwise from start to end if ``True``
        """
        radius = abs(radius)
        if NULLVEC.isclose(extrusion):
            raise ValueError(f"Invalid extrusion: {str(extrusion)}")
        ratio = 1.0
        ocs = OCS(extrusion)
        center = ocs.to_wcs(center)
        # Major axis along the OCS x-axis.
        major_axis = ocs.to_wcs(Vec3(radius, 0, 0))
        # No further adjustment of start- and end angle required.
        start_param = math.radians(start_angle)
        end_param = math.radians(end_angle)
        return cls(
            center,
            major_axis,
            extrusion,
            ratio,
            start_param,
            end_param,
            bool(ccw),
        )

    def __copy__(self):
        return self.__class__(
            self.center,
            self.major_axis,
            self.extrusion,
            self.ratio,
            self.start_param,
            self.end_param,
        )

    @property
    def start_point(self) -> Vec3:
        """Returns start point of ellipse as Vec3."""
        return vertex(
            self.start_param,
            self.major_axis,
            self.minor_axis,
            self.center,
            self.ratio,
        )

    @property
    def end_point(self) -> Vec3:
        """Returns end point of ellipse as Vec3."""
        return vertex(
            self.end_param,
            self.major_axis,
            self.minor_axis,
            self.center,
            self.ratio,
        )

    def dxfattribs(self) -> dict[str, Any]:
        """Returns required DXF attributes to build an ELLIPSE entity.

        Entity ELLIPSE has always a ratio in range from 1e-6 to 1.

        """
        if self.ratio > 1:
            e = self.__copy__()
            e.swap_axis()
        else:
            e = self
        return {
            "center": e.center,
            "major_axis": e.major_axis,
            "extrusion": e.extrusion,
            "ratio": max(e.ratio, 1e-6),
            "start_param": e.start_param,
            "end_param": e.end_param,
        }

    def main_axis_points(self) -> Iterable[Vec3]:
        """Yields main axis points of ellipse in the range from start- to end
        param.
        """
        start = self.start_param
        end = self.end_param
        for param in QUARTER_PARAMS:
            if enclosing_angles(param, start, end):
                yield vertex(
                    param,
                    self.major_axis,
                    self.minor_axis,
                    self.center,
                    self.ratio,
                )

    def transform(self, m: Matrix44) -> None:
        """Transform ellipse in place by transformation matrix `m`."""
        new_center = m.transform(self.center)
        # 2021-01-28 removed % math.tau
        old_start_param = start_param = self.start_param
        old_end_param = end_param = self.end_param
        old_minor_axis = minor_axis(self.major_axis, self.extrusion, self.ratio)
        new_major_axis, new_minor_axis = m.transform_directions(
            (self.major_axis, old_minor_axis)
        )
        # Original ellipse parameters stay untouched until end of transformation
        dot_product = new_major_axis.normalize().dot(new_minor_axis.normalize())
        if abs(dot_product) > 1e-6:
            new_major_axis, new_minor_axis, new_ratio = rytz_axis_construction(
                new_major_axis, new_minor_axis
            )
            new_extrusion = new_major_axis.cross(new_minor_axis).normalize()
            adjust_params = True
        else:
            # New axis are nearly orthogonal:
            new_ratio = new_minor_axis.magnitude / new_major_axis.magnitude
            # New normal vector:
            new_extrusion = new_major_axis.cross(new_minor_axis).normalize()
            # Calculate exact minor axis:
            new_minor_axis = minor_axis(new_major_axis, new_extrusion, new_ratio)
            adjust_params = False

        if adjust_params and not math.isclose(start_param, end_param, abs_tol=1e-9):
            # open ellipse, adjusting start- and end parameter
            x_axis = new_major_axis.normalize()
            y_axis = new_minor_axis.normalize()
            # TODO: use ellipse_param_span()?
            #  2021-01-28 this is possibly the source of errors!
            old_param_span = (end_param - start_param) % math.tau

            def param(vec: "Vec3") -> float:
                dy = y_axis.dot(vec) / new_ratio  # adjust to circle
                dx = x_axis.dot(vec)
                return math.atan2(dy, dx) % math.tau

            # transformed start- and end point of old ellipse
            start_point = m.transform(
                vertex(
                    start_param,
                    self.major_axis,
                    old_minor_axis,
                    self.center,
                    self.ratio,
                )
            )
            end_point = m.transform(
                vertex(
                    end_param,
                    self.major_axis,
                    old_minor_axis,
                    self.center,
                    self.ratio,
                )
            )

            start_param = param(start_point - new_center)
            end_param = param(end_point - new_center)

            # Test if drawing the correct side of the curve
            if not math.isclose(old_param_span, math.pi, abs_tol=1e-9):
                # Equal param span check works well, except for a span of exact
                # pi (180 deg).
                # TODO: use ellipse_param_span()?
                #  2021-01-28 this is possibly the source of errors!
                new_param_span = (end_param - start_param) % math.tau
                if not math.isclose(old_param_span, new_param_span, abs_tol=1e-9):
                    start_param, end_param = end_param, start_param
            else:  # param span is exact pi (180 deg)
                # expensive but it seem to work:
                old_chk_point = m.transform(
                    vertex(
                        mid_param(old_start_param, old_end_param),
                        self.major_axis,
                        old_minor_axis,
                        self.center,
                        self.ratio,
                    )
                )
                new_chk_point = vertex(
                    mid_param(start_param, end_param),
                    new_major_axis,
                    new_minor_axis,
                    new_center,
                    new_ratio,
                )
                if not old_chk_point.isclose(new_chk_point, abs_tol=1e-9):
                    start_param, end_param = end_param, start_param

        if new_ratio > 1:
            new_major_axis = minor_axis(new_major_axis, new_extrusion, new_ratio)
            new_ratio = 1.0 / new_ratio
            new_minor_axis = minor_axis(new_major_axis, new_extrusion, new_ratio)
            if not (math.isclose(start_param, 0) and math.isclose(end_param, math.tau)):
                start_param -= HALF_PI
                end_param -= HALF_PI

        # TODO: remove normalize start- and end params?
        #  2021-01-28 this is possibly the source of errors!
        start_param = start_param % math.tau
        end_param = end_param % math.tau
        if math.isclose(start_param, end_param):
            start_param = 0.0
            end_param = math.tau

        self.center = new_center
        self.major_axis = new_major_axis
        self.minor_axis = new_minor_axis
        self.extrusion = new_extrusion
        self.ratio = new_ratio
        self.start_param = start_param
        self.end_param = end_param

    @property
    def param_span(self) -> float:
        """Returns the counter-clockwise params span from start- to end param,
        see also :func:`ezdxf.math.ellipse_param_span` for more information.

        """
        return arc_angle_span_rad(self.start_param, self.end_param)

    def params(self, num: int) -> Iterable[float]:
        """Returns `num` params from start- to end param in counter-clockwise
        order.

        All params are normalized in the range from [0, 2π).

        """
        yield from get_params(self.start_param, self.end_param, num)

    def vertices(self, params: Iterable[float]) -> Iterable[Vec3]:
        """Yields vertices on ellipse for iterable `params` in WCS.

        Args:
            params: param values in the range from [0, 2π) in radians,
                param goes counter-clockwise around the extrusion vector,
                major_axis = local x-axis = 0 rad.

        """
        center = self.center
        ratio = self.ratio
        x_axis = self.major_axis.normalize()
        y_axis = self.minor_axis.normalize()
        radius_x = self.major_axis.magnitude
        radius_y = radius_x * ratio

        for param in params:
            x = math.cos(param) * radius_x * x_axis
            y = math.sin(param) * radius_y * y_axis
            yield center + x + y

    def flattening(self, distance: float, segments: int = 4) -> Iterable[Vec3]:
        """Adaptive recursive flattening. The argument `segments` is the
        minimum count of approximation segments, if the distance from the center
        of the approximation segment to the curve is bigger than `distance` the
        segment will be subdivided. Returns a closed polygon for a full ellipse:
        start vertex == end vertex.

        Args:
            distance: maximum distance from the projected curve point onto the
                segment chord.
            segments: minimum segment count

        """

        def vertex_(p: float) -> Vec3:
            x = math.cos(p) * radius_x * x_axis
            y = math.sin(p) * radius_y * y_axis
            return self.center + x + y

        def subdiv(s: Vec3, e: Vec3, s_param: float, e_param: float):
            m_param = (s_param + e_param) * 0.5
            m = vertex_(m_param)
            d = distance_point_line_3d(m, s, e)
            if d < distance:
                yield e
            else:
                yield from subdiv(s, m, s_param, m_param)
                yield from subdiv(m, e, m_param, e_param)

        x_axis = self.major_axis.normalize()
        y_axis = self.minor_axis.normalize()
        radius_x = self.major_axis.magnitude
        radius_y = radius_x * self.ratio

        delta = self.param_span / segments
        if delta == 0.0:
            return

        param = self.start_param % math.tau
        if math.isclose(self.end_param, math.tau):
            end_param = math.tau
        else:
            end_param = self.end_param % math.tau

        if math.isclose(param, end_param):
            return
        elif param > end_param:
            end_param += math.tau

        start_point = vertex_(param)
        yield start_point
        while param < end_param:
            next_end_param = param + delta
            if math.isclose(next_end_param, end_param):
                next_end_param = end_param
            end_point = vertex_(next_end_param)
            yield from subdiv(start_point, end_point, param, next_end_param)
            param = next_end_param
            start_point = end_point

    def params_from_vertices(self, vertices: Iterable[UVec]) -> Iterable[float]:
        """Yields ellipse params for all given `vertices`.

        The vertex don't have to be exact on the ellipse curve or in the range
        from start- to end param or even in the ellipse plane. Param is
        calculated from the intersection point of the ray projected on the
        ellipse plane from the center of the ellipse through the vertex.

        .. warning::

            An input for start- and end vertex at param 0 and 2π return
            unpredictable results because of floating point inaccuracy,
            sometimes 0 and sometimes 2π.

        """
        x_axis = self.major_axis.normalize()
        y_axis = self.minor_axis.normalize()
        ratio = self.ratio
        center = self.center
        for v in Vec3.generate(vertices):
            v -= center
            yield math.atan2(y_axis.dot(v) / ratio, x_axis.dot(v)) % math.tau

    def tangents(self, params: Iterable[float]) -> Iterable[Vec3]:
        """Yields tangents on ellipse for iterable `params` in WCS as direction
        vectors.

        Args:
            params: param values in the range from [0, 2π] in radians, param
                goes counter-clockwise around the extrusion vector,
                major_axis = local x-axis = 0 rad.

        """
        ratio = self.ratio
        x_axis = self.major_axis.normalize()
        y_axis = self.minor_axis.normalize()

        for param in params:
            x = -math.sin(param) * x_axis
            y = math.cos(param) * ratio * y_axis
            yield (x + y).normalize()

    def swap_axis(self) -> None:
        """Swap axis and adjust start- and end parameter."""
        self.major_axis = self.minor_axis
        ratio = 1.0 / self.ratio
        self.ratio = max(ratio, 1e-6)
        self.minor_axis = minor_axis(self.major_axis, self.extrusion, self.ratio)

        start_param = self.start_param
        end_param = self.end_param
        if math.isclose(start_param, 0) and math.isclose(end_param, math.tau):
            return
        self.start_param = (start_param - HALF_PI) % math.tau
        self.end_param = (end_param - HALF_PI) % math.tau

    def add_to_layout(self, layout: BaseLayout, dxfattribs=None) -> Ellipse:
        """Add ellipse as DXF :class:`~ezdxf.entities.Ellipse` entity to a
        layout.

        Args:
            layout: destination layout as :class:`~ezdxf.layouts.BaseLayout`
                object
            dxfattribs: additional DXF attributes for the ELLIPSE entity

        """
        from ezdxf.entities import Ellipse

        dxfattribs = dict(dxfattribs or {})
        dxfattribs.update(self.dxfattribs())
        e = Ellipse.new(dxfattribs=dxfattribs, doc=layout.doc)
        layout.add_entity(e)
        return e

    def to_ocs(self) -> ConstructionEllipse:
        """Returns ellipse parameters as OCS representation.

        OCS elevation is stored in :attr:`center.z`.

        """
        ocs = OCS(self.extrusion)
        return self.__class__(
            center=ocs.from_wcs(self.center),
            major_axis=ocs.from_wcs(self.major_axis).replace(z=0.0),
            ratio=self.ratio,
            start_param=self.start_param,
            end_param=self.end_param,
        )


def mid_param(start: float, end: float) -> float:
    if end < start:
        end += math.tau
    return (start + end) / 2.0


def minor_axis(major_axis: Vec3, extrusion: Vec3, ratio: float) -> Vec3:
    return extrusion.cross(major_axis).normalize(major_axis.magnitude * ratio)


def vertex(
    param: float, major_axis: Vec3, minor_axis: Vec3, center: Vec3, ratio: float
) -> Vec3:
    x_axis = major_axis.normalize()
    y_axis = minor_axis.normalize()
    radius_x = major_axis.magnitude
    radius_y = radius_x * ratio
    x = math.cos(param) * radius_x * x_axis
    y = math.sin(param) * radius_y * y_axis
    return center + x + y


def get_params(start: float, end: float, num: int) -> Iterable[float]:
    """Returns `num` params from start- to end param in counter-clockwise order.

    All params are normalized in the range from [0, 2π).

    """
    if num < 2:
        raise ValueError("num >= 2")
    if end <= start:
        end += math.tau

    for param in np.linspace(start, end, num):
        yield param % math.tau


def angle_to_param(ratio: float, angle: float) -> float:
    """Returns ellipse parameter for argument `angle`.

    Args:
        ratio: minor axis to major axis ratio as stored in the ELLIPSE entity
            (always <= 1).
        angle: angle between major axis and line from center to point on the
            ellipse

    Returns:
        the ellipse parameter in the range [0, 2π)
    """
    return math.atan2(math.sin(angle) / ratio, math.cos(angle)) % math.tau


def param_to_angle(ratio: float, param: float) -> float:
    """Returns circle angle from ellipse parameter for argument `angle`.

    Args:
        ratio: minor axis to major axis ratio as stored in the ELLIPSE entity
            (always <= 1).
        param: ellipse parameter between major axis and point on the ellipse
            curve

    Returns:
        the circle angle in the range [0, 2π)
    """
    return math.atan2(math.sin(param) * ratio, math.cos(param))


def rytz_axis_construction(d1: Vec3, d2: Vec3) -> tuple[Vec3, Vec3, float]:
    """The Rytz’s axis construction is a basic method of descriptive Geometry
    to find the axes, the semi-major axis and semi-minor axis, starting from two
    conjugated half-diameters.

    Source: `Wikipedia <https://en.m.wikipedia.org/wiki/Rytz%27s_construction>`_

    Given conjugated diameter `d1` is the vector from center C to point P and
    the given conjugated diameter `d2` is the vector from center C to point Q.
    Center of ellipse is always ``(0, 0, 0)``. This algorithm works for
    2D/3D vectors.

    Args:
        d1: conjugated semi-major axis as :class:`Vec3`
        d2: conjugated semi-minor axis as :class:`Vec3`

    Returns:
         Tuple of (major axis, minor axis, ratio)

    """
    Q = Vec3(d1)  # vector CQ
    # calculate vector CP', location P'
    if math.isclose(d1.z, 0, abs_tol=1e-9) and math.isclose(d2.z, 0, abs_tol=1e-9):
        # Vec3.orthogonal() works only for vectors in the xy-plane!
        P1 = Vec3(d2).orthogonal(ccw=False)
    else:
        extrusion = d1.cross(d2)
        P1 = extrusion.cross(d2).normalize(d2.magnitude)

    D = P1.lerp(Q)  # vector CD, location D, midpoint of P'Q
    radius = D.magnitude
    radius_vector = (Q - P1).normalize(radius)  # direction vector P'Q
    A = D - radius_vector  # vector CA, location A
    B = D + radius_vector  # vector CB, location B
    if A.isclose(NULLVEC) or B.isclose(NULLVEC):
        raise ArithmeticError("Conjugated axis required, invalid source data.")
    major_axis_length = (A - Q).magnitude
    minor_axis_length = (B - Q).magnitude
    if math.isclose(major_axis_length, 0.0) or math.isclose(minor_axis_length, 0.0):
        raise ArithmeticError("Conjugated axis required, invalid source data.")
    ratio = minor_axis_length / major_axis_length
    major_axis = B.normalize(major_axis_length)
    minor_axis = A.normalize(minor_axis_length)
    return major_axis, minor_axis, ratio
