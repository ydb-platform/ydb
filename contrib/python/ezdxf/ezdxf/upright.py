# Copyright (c) 2021, Manfred Moitzi
# License: MIT License
#
# Purpose:
# Flips an inverted OCS defined by extrusion vector (0, 0, -1) into a WCS
# aligned OCS defined by extrusion vector (0, 0, 1).
#
# This simplifies 2D entity processing for ezdxf users and creates DXF
# output for 3rd party DXF libraries which ignore the existence of the OCS.
#
# Warning:
# The WCS representation of OCS entities with flipped extrusion vector
# is not 100% identical to the source entity, curve orientation and vertex order
# may change. A mirrored text represented by an extrusion vector (0, 0, -1)
# cannot represented by an extrusion vector (0, 0, 1), therefore this CANNOT
# work for text entities or entities including text:
# TEXT, ATTRIB, ATTDEF, MTEXT, DIMENSION, LEADER, MLEADER
from __future__ import annotations
from typing import Iterable, Sequence
from functools import singledispatch
import math
from ezdxf.math import Z_AXIS, Vec3, Vec2
from ezdxf.entities import (
    DXFGraphic,
    DXFNamespace,
    Circle,
    Arc,
    Ellipse,
    Solid,
    Insert,
    LWPolyline,
    Polyline,
)
from ezdxf.entities.polygon import DXFPolygon
from ezdxf.entities.boundary_paths import (
    PolylinePath,
    EdgePath,
    LineEdge,
    ArcEdge,
    EllipseEdge,
    SplineEdge,
)
from ezdxf.lldxf import const

__all__ = ["upright", "upright_all"]


def upright(entity: DXFGraphic) -> None:
    """Flips an inverted :ref:`OCS` defined by extrusion vector (0, 0, -1) into
    a :ref:`WCS` aligned :ref:`OCS` defined by extrusion vector (0, 0, 1).
    DXF entities with other extrusion vectors and unsupported DXF entities will
    be silently ignored. For more information about the limitations read the
    documentation of the :mod:`ezdxf.upright` module.

    """
    if not (
        isinstance(entity, DXFGraphic)
        and entity.is_alive
        and entity.dxf.hasattr("extrusion")
    ):
        return
    extrusion = Vec3(entity.dxf.extrusion).normalize()
    if extrusion.isclose(FLIPPED_Z_AXIS):
        _flip_dxf_graphic(entity)


def upright_all(entities: Iterable[DXFGraphic]) -> None:
    """Call function :func:`upright` for all DXF entities in iterable
    `entities`::

        upright_all(doc.modelspace())

    """
    for e in entities:
        upright(e)


FLIPPED_Z_AXIS = -Z_AXIS


def _flip_deg_angle(angle: float) -> float:
    return (180.0 if angle >= 0.0 else -180.0) - angle


def _flip_rad_angle(angle: float) -> float:
    return (math.pi if angle >= 0.0 else -math.pi) - angle


def _flip_vertex(vertex: Vec3) -> Vec3:
    return Vec3(-vertex.x, vertex.y, -vertex.z)


def _flip_2d_vertex(vertex: Vec2) -> Vec2:
    return Vec2(-vertex.x, vertex.y)


def _flip_existing_vertex(dxf: DXFNamespace, name: str) -> None:
    if dxf.hasattr(name):
        vertex = _flip_vertex(dxf.get(name))
        dxf.set(name, vertex)


def _flip_thickness(dxf: DXFNamespace) -> None:
    if dxf.hasattr("thickness"):
        dxf.thickness = -dxf.thickness


def _flip_elevation(dxf: DXFNamespace) -> None:
    if dxf.hasattr("elevation"):
        # works with float (LWPOLYLINE) and Vec3 (POLYLINE)
        dxf.elevation = -dxf.elevation


@singledispatch
def _flip_dxf_graphic(entity: DXFGraphic) -> None:
    pass  # ignore unsupported types on purpose


@_flip_dxf_graphic.register(Circle)
def _flip_circle(circle: Circle) -> None:
    dxf = circle.dxf
    _flip_existing_vertex(dxf, "center")
    _flip_thickness(dxf)
    dxf.discard("extrusion")


@_flip_dxf_graphic.register(Arc)
def _flip_arc(arc: Arc) -> None:
    _flip_circle(arc)
    dxf = arc.dxf
    end_angle = dxf.end_angle
    dxf.end_angle = _flip_deg_angle(dxf.start_angle)
    dxf.start_angle = _flip_deg_angle(end_angle)


# SOLID and TRACE - but not 3DFACE!
@_flip_dxf_graphic.register(Solid)
def _flip_solid(solid: Solid) -> None:
    dxf = solid.dxf
    for name in const.VERTEXNAMES:
        _flip_existing_vertex(dxf, name)
    _flip_thickness(dxf)
    dxf.discard("extrusion")


@_flip_dxf_graphic.register(Insert)
def _flip_insert(insert: Insert) -> None:
    # see exploration/upright_insert.py
    dxf = insert.dxf
    _flip_existing_vertex(dxf, "insert")
    dxf.rotation = -dxf.rotation
    dxf.xscale = -dxf.xscale
    dxf.zscale = -dxf.zscale
    # Attached attributes cannot be flipped!
    dxf.discard("extrusion")


@_flip_dxf_graphic.register(Ellipse)
def _flip_ellipse(ellipse: Ellipse) -> None:
    # ELLIPSE is a WCS entity!
    # just process start- and end params
    dxf = ellipse.dxf
    end_param = -dxf.end_param
    dxf.end_param = -dxf.start_param
    dxf.start_param = end_param
    dxf.discard("extrusion")


@_flip_dxf_graphic.register(LWPolyline)
def _flip_lwpolyline(polyline: LWPolyline) -> None:
    flipped_points: list[Sequence[float]] = []
    for x, y, start_width, end_width, bulge in polyline.lwpoints:
        bulge = -bulge
        v = _flip_2d_vertex(Vec2(x, y))
        flipped_points.append((v.x, v.y, start_width, end_width, bulge))
    polyline.set_points(flipped_points, format="xyseb")
    _finalize_polyline(polyline.dxf)


def _finalize_polyline(dxf: DXFNamespace):
    _flip_thickness(dxf)
    _flip_elevation(dxf)
    dxf.discard("extrusion")


@_flip_dxf_graphic.register(Polyline)
def _flip_polyline2d(polyline: Polyline) -> None:
    if not polyline.is_2d_polyline:
        return  # ignore silently
    for vertex in polyline.vertices:
        dxf = vertex.dxf
        _flip_existing_vertex(dxf, "location")
        if dxf.hasattr("bulge"):
            dxf.bulge = -dxf.bulge
    _finalize_polyline(polyline.dxf)


# HATCH and MPOLYGON
@_flip_dxf_graphic.register(DXFPolygon)
def _flip_polygon(polygon: DXFPolygon) -> None:
    for p in polygon.paths:
        _flip_boundary_path(p)
    _flip_elevation(polygon.dxf)
    polygon.dxf.discard("extrusion")


@singledispatch
def _flip_boundary_path(path) -> None:
    raise TypeError(f"unsupported path type: {path!r}")


@_flip_boundary_path.register(PolylinePath)
def _flip_polyline_path(polyline: PolylinePath) -> None:
    flipped_vertices: list[tuple[float, float, float]] = []
    for x, y, bulge in polyline.vertices:
        bulge = -bulge
        v = _flip_2d_vertex(Vec2(x, y))
        flipped_vertices.append((v.x, v.y, bulge))
    polyline.vertices = flipped_vertices


@_flip_boundary_path.register(EdgePath)
def _flip_edge_path(edges: EdgePath) -> None:
    # see exploration/upright_hatch.py
    for edge in edges:
        _flip_edge(edge)


@singledispatch
def _flip_edge(edge) -> None:
    raise TypeError(f"unsupported edge type: {edge!r}")


@_flip_edge.register(LineEdge)
def _flip_line_edge(edge: LineEdge) -> None:
    edge.start = _flip_2d_vertex(edge.start)
    edge.end = _flip_2d_vertex(edge.end)


@_flip_edge.register(ArcEdge)
def _flip_arc_edge(edge: ArcEdge) -> None:
    edge.center = _flip_2d_vertex(edge.center)
    # Start- and end angles are always stored in counter-clockwise orientation!
    end_angle = edge.end_angle
    edge.end_angle = _flip_deg_angle(edge.start_angle)
    edge.start_angle = _flip_deg_angle(end_angle)
    edge.ccw = not edge.ccw


@_flip_edge.register(EllipseEdge)
def _flip_ellipse_edge(edge: EllipseEdge) -> None:
    edge.center = _flip_2d_vertex(edge.center)
    edge.major_axis = _flip_2d_vertex(edge.major_axis)
    # Ellipse params as angles in degrees - not radians!
    # Do not exchange start- and end angles!
    # see exploration/upright_hatch.py
    edge.start_angle = _flip_deg_angle(edge.start_angle)
    edge.end_angle = _flip_deg_angle(edge.end_angle)
    edge.ccw = not edge.ccw


@_flip_edge.register(SplineEdge)
def _flip_spline_edge(edge: SplineEdge) -> None:
    flip_2d_vertex = _flip_2d_vertex
    if edge.start_tangent is not None:
        edge.start_tangent = flip_2d_vertex(edge.start_tangent)
    if edge.end_tangent is not None:
        edge.end_tangent = flip_2d_vertex(edge.end_tangent)
    edge.control_points = [flip_2d_vertex(v) for v in edge.control_points]
    edge.fit_points = [flip_2d_vertex(v) for v in edge.fit_points]
