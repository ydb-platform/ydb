# Copyright (c) 2018-2022 Manfred Moitzi
# License: MIT License
"""
DXF R12 Splines
===============

DXF R12 supports 2d B-splines, but Autodesk do not document the usage in the
DXF Reference. The base entity for splines in DXF R12 is the POLYLINE entity.

Transformed Into 3D Space
-------------------------

The spline itself is always in a plane, but as any 2D entity, the spline can be
transformed into the 3D object by elevation, extrusion and thickness/width.

Open Quadratic Spline with Fit Vertices
-------------------------------------

Example: 2D_SPLINE_QUADRATIC.dxf
expected knot vector: open uniform
degree: 2
order: 3

POLYLINE:
flags (70): 4 = SPLINE_FIT_VERTICES_ADDED
smooth type (75): 5 = QUADRATIC_BSPLINE

Sequence of VERTEX
flags (70): SPLINE_VERTEX_CREATED = 8  # Spline vertex created by spline-fitting

This vertices are the curve vertices of the spline (fitted).

Frame control vertices appear after the curve vertices.

Sequence of VERTEX
flags (70): SPLINE_FRAME_CONTROL_POINT = 16

No control point at the starting point, but a control point at the end point,
last control point == last fit vertex

Closed Quadratic Spline with Fit Vertices
-----------------------------------------

Example: 2D_SPLINE_QUADRATIC_CLOSED.dxf
expected knot vector: closed uniform
degree: 2
order: 3

POLYLINE:
flags (70): 5 = CLOSED | SPLINE_FIT_VERTICES_ADDED
smooth type (75): 5 = QUADRATIC_BSPLINE

Sequence of VERTEX
flags (70): SPLINE_VERTEX_CREATED = 8  # Spline vertex created by spline-fitting

Frame control vertices appear after the curve vertices.

Sequence of VERTEX
flags (70): SPLINE_FRAME_CONTROL_POINT = 16


Open Cubic Spline with Fit Vertices
-----------------------------------

Example: 2D_SPLINE_CUBIC.dxf
expected knot vector: open uniform
degree: 3
order: 4

POLYLINE:
flags (70): 4 = SPLINE_FIT_VERTICES_ADDED
smooth type (75): 6 = CUBIC_BSPLINE

Sequence of VERTEX
flags (70): SPLINE_VERTEX_CREATED = 8  # Spline vertex created by spline-fitting

This vertices are the curve vertices of the spline (fitted).

Frame control vertices appear after the curve vertices.

Sequence of VERTEX
flags (70): SPLINE_FRAME_CONTROL_POINT = 16

No control point at the starting point, but a control point at the end point,
last control point == last fit vertex

Closed Curve With Extra Vertices Created
----------------------------------------

Example: 2D_FIT_CURVE_CLOSED.dxf

POLYLINE:
flags (70): 3 = CLOSED | CURVE_FIT_VERTICES_ADDED

Vertices with bulge values:

flags (70): 1 = EXTRA_VERTEX_CREATED
Vertex 70=0, Vertex 70=1, Vertex 70=0, Vertex 70=1

"""
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Optional
from ezdxf.lldxf import const
from ezdxf.math import BSpline, closed_uniform_bspline, Vec3, UCS, UVec

if TYPE_CHECKING:
    from ezdxf.layouts import BaseLayout
    from ezdxf.entities import Polyline


class R12Spline:
    """DXF R12 supports 2D B-splines, but Autodesk do not document the usage
    in the DXF Reference. The base entity for splines in DXF R12 is the POLYLINE
    entity. The spline itself is always in a plane, but as any 2D entity, the
    spline can be transformed into the 3D object by elevation and extrusion
    (:ref:`OCS`, :ref:`UCS`).

    This way it was possible to store the spline parameters in the DXF R12 file,
    to allow CAD applications to modify the spline parameters and rerender the
    B-spline afterward again as polyline approximation. Therefore, the result is
    not better than an approximation by the :class:`Spline` class, it is also
    just a POLYLINE entity, but maybe someone need exact this tool in the
    future.

    """

    def __init__(
        self,
        control_points: Iterable[UVec],
        degree: int = 2,
        closed: bool = True,
    ):
        """
        Args:
            control_points: B-spline control frame vertices
            degree: degree of B-spline, only 2 and 3 is supported
            closed: ``True`` for closed curve

        """
        self.control_points = Vec3.list(control_points)
        self.degree = degree
        self.closed = closed

    def approximate(
        self, segments: int = 40, ucs: Optional[UCS] = None
    ) -> list[UVec]:
        """Approximate the B-spline by a polyline with `segments` line segments.
        If `ucs` is not ``None``, ucs defines an :class:`~ezdxf.math.UCS`, to
        transform the curve into :ref:`OCS`. The control points are placed
        xy-plane of the UCS, don't use z-axis coordinates, if so make sure all
        control points are in a plane parallel to the OCS base plane
        (UCS xy-plane), else the result is unpredictable and depends on the CAD
        application used to open the DXF file - it may crash.

        Args:
            segments: count of line segments for approximation, vertex count is
                `segments` + 1
            ucs: :class:`~ezdxf.math.UCS` definition, control points in ucs
                coordinates

        Returns:
            list of vertices in :class:`~ezdxf.math.OCS` as
            :class:`~ezdxf.math.Vec3` objects

        """
        if self.closed:
            spline = closed_uniform_bspline(
                self.control_points, order=self.degree + 1
            )
        else:
            spline = BSpline(self.control_points, order=self.degree + 1)
        vertices = spline.approximate(segments)
        if ucs is not None:
            vertices = (ucs.to_ocs(vertex) for vertex in vertices)
        return list(vertices)

    def render(
        self,
        layout: BaseLayout,
        segments: int = 40,
        ucs: Optional[UCS] = None,
        dxfattribs=None,
    ) -> Polyline:
        """Renders the B-spline into `layout` as 2D :class:`~ezdxf.entities.Polyline`
        entity. Use an :class:`~ezdxf.math.UCS` to place the 2D spline in the
        3D space, see :meth:`approximate` for more information.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            segments: count of line segments for approximation, vertex count is
                `segments` + 1
            ucs: :class:`~ezdxf.math.UCS` definition, control points in ucs
                coordinates.
            dxfattribs: DXF attributes for :class:`~ezdxf.entities.Polyline`

        """
        polyline = layout.add_polyline2d(points=[], dxfattribs=dxfattribs)
        flags = polyline.SPLINE_FIT_VERTICES_ADDED
        if self.closed:
            flags |= polyline.CLOSED
        polyline.dxf.flags = flags

        if self.degree == 2:
            smooth_type = polyline.QUADRATIC_BSPLINE
        elif self.degree == 3:
            smooth_type = polyline.CUBIC_BSPLINE
        else:
            raise ValueError("invalid degree of spline")
        polyline.dxf.smooth_type = smooth_type

        # set OCS extrusion vector
        if ucs is not None:
            polyline.dxf.extrusion = ucs.uz

        # add fit points in OCS
        polyline.append_vertices(
            self.approximate(segments, ucs),
            dxfattribs={
                "layer": polyline.dxf.layer,
                "flags": const.VTX_SPLINE_VERTEX_CREATED,
            },
        )

        # add control frame points in OCS
        control_points = self.control_points
        if ucs is not None:
            control_points = list(ucs.points_to_ocs(control_points))
            polyline.dxf.elevation = (0, 0, control_points[0].z)
        polyline.append_vertices(
            control_points,
            dxfattribs={
                "layer": polyline.dxf.layer,
                "flags": const.VTX_SPLINE_FRAME_CONTROL_POINT,
            },
        )
        return polyline
