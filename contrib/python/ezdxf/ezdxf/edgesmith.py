# Copyright (c) 2024, Manfred Moitzi
# License: MIT License
"""
EdgeSmith
=========

A module for creating entities like polylines and hatch boundary paths from linked edges.

The complementary module to ezdxf.edgeminer.

"""
from __future__ import annotations

from typing import Iterator, Iterable, Sequence, Any, NamedTuple
from typing_extensions import TypeAlias
import math
import functools
from operator import itemgetter

from ezdxf import edgeminer as em
from ezdxf import entities as et
from ezdxf import path
from ezdxf.entities import boundary_paths as bp
from ezdxf.upright import upright_all

from ezdxf.math import (
    UVec,
    Vec2,
    Vec3,
    arc_angle_span_deg,
    ellipse_param_span,
    bulge_from_arc_angle,
    Z_AXIS,
    Matrix44,
    intersection_line_line_2d,
    is_point_in_polygon_2d,
    BoundingBox2d,
    area,
)

__all__ = [
    "bounding_box_2d",
    "chain_vertices",
    "edge_path_from_chain",
    "edges_from_entities_2d",
    "filter_2d_entities",
    "filter_edge_entities",
    "filter_open_edges",
    "intersecting_edges_2d",
    "is_closed_entity",
    "is_inside_polygon_2d",
    "is_pure_2d_entity",
    "loop_area",
    "lwpolyline_from_chain",
    "make_edge_2d",
    "path2d_from_chain",
    "polyline2d_from_chain",
    "polyline_path_from_chain",
]

# Tolerances
LEN_TOL = 1e-9  # length and distance
DEG_TOL = 1e-9  # angles in degree
RAD_TOL = 1e-7  # angles in radians
GAP_TOL = em.GAP_TOL


# noinspection PyUnusedLocal
@functools.singledispatch
def is_closed_entity(entity: et.DXFEntity) -> bool:
    """Returns ``True`` if the given entity represents a closed loop.

    Tests the following DXF entities:

        - CIRCLE (radius > 0)
        - ARC
        - ELLIPSE
        - SPLINE
        - LWPOLYLINE
        - POLYLINE
        - HATCH
        - SOLID
        - TRACE

    Returns ``False`` for all other DXF entities.
    """
    return False


@is_closed_entity.register(et.Arc)
def _is_closed_arc(entity: et.Arc) -> bool:
    radius = abs(entity.dxf.radius)
    start_angle = entity.dxf.start_angle
    end_angle = entity.dxf.end_angle
    angle_span = arc_angle_span_deg(start_angle, end_angle)
    return abs(radius) > LEN_TOL and math.isclose(angle_span, 360.0, abs_tol=LEN_TOL)


@is_closed_entity.register(et.Circle)
def _is_closed_circle(entity: et.Circle) -> bool:
    return abs(entity.dxf.radius) > LEN_TOL


@is_closed_entity.register(et.Ellipse)
def _is_closed_ellipse(entity: et.Ellipse) -> bool:
    start_param = entity.dxf.start_param
    end_param = entity.dxf.end_param
    span = ellipse_param_span(start_param, end_param)
    if not math.isclose(span, math.tau, abs_tol=RAD_TOL):
        return False
    return True


@is_closed_entity.register(et.Spline)
def _is_closed_spline(entity: et.Spline) -> bool:
    try:
        bspline = entity.construction_tool()
    except ValueError:
        return False
    control_points = bspline.control_points
    if len(control_points) < 3:
        return False
    start = control_points[0]
    end = control_points[-1]
    return start.isclose(end, abs_tol=LEN_TOL)


@is_closed_entity.register(et.LWPolyline)
def _is_closed_lwpolyline(entity: et.LWPolyline) -> bool:
    if len(entity) < 1:
        return False
    if entity.closed is True:
        return True
    start = Vec2(entity.lwpoints[0][:2])
    end = Vec2(entity.lwpoints[-1][:2])
    return start.isclose(end, abs_tol=LEN_TOL)


@is_closed_entity.register(et.Polyline)
def _is_closed_polyline2d(entity: et.Polyline) -> bool:
    if entity.is_2d_polyline or entity.is_3d_polyline:
        # Note: does not check if all vertices of a 3D polyline are placed on a
        # common plane.
        vertices = entity.vertices
        if len(vertices) < 2:
            return False
        if entity.is_closed:
            return True
        p0: Vec3 = vertices[0].dxf.location  # type: ignore
        p1: Vec3 = vertices[-1].dxf.location  # type: ignore
        if p0.isclose(p1, abs_tol=LEN_TOL):
            return True
    return False


@is_closed_entity.register(et.Hatch)
def _is_closed_hatch(entity: et.Hatch) -> bool:
    return bool(len(entity.paths))


@is_closed_entity.register(et.Trace)
@is_closed_entity.register(et.Solid)
def _is_closed_solid(entity: et.Solid | et.Trace) -> bool:
    return True


# noinspection PyUnusedLocal
@functools.singledispatch
def is_pure_2d_entity(entity: et.DXFEntity) -> bool:
    """Returns ``True`` if the given entity represents a pure 2D entity in the
    xy-plane of the WCS.

    - All vertices must be in the xy-plane of the WCS.
    - Thickness must be 0.
    - The extrusion vector must be (0, 0, 1).
    - Entities with inverted extrusions vectors (0, 0, -1) are **not** pure 2D entities.
      The ezdxf.upright module can be used to revert inverted extrusion vectors
      back to (0, 0, 1).

    Tests the following DXF entities:

        - LINE
        - CIRCLE
        - ARC
        - ELLIPSE
        - SPLINE
        - LWPOLYLINE
        - POLYLINE
        - HATCH
        - SOLID
        - TRACE

    Returns ``False`` for all other DXF entities.

    """
    return False


@is_pure_2d_entity.register(et.Line)
def _is_pure_2d_entity(entity: et.Line) -> bool:
    if not Z_AXIS.isclose(entity.dxf.extrusion):
        return False
    if abs(entity.dxf.thickness) > LEN_TOL:
        return False
    if abs(Vec3(entity.dxf.start).z) > LEN_TOL:
        return False
    if abs(Vec3(entity.dxf.end).z) > LEN_TOL:
        return False
    return True


@is_pure_2d_entity.register(et.Circle)
@is_pure_2d_entity.register(et.Arc)
def _is_pure_2d_arc(entity: et.Circle | et.Arc) -> bool:
    if not Z_AXIS.isclose(entity.dxf.extrusion):
        return False
    if abs(Vec3(entity.dxf.center).z) > LEN_TOL:
        return False
    if abs(entity.dxf.elevation) > LEN_TOL:
        return False
    if abs(entity.dxf.thickness) > LEN_TOL:
        return False
    return True


@is_pure_2d_entity.register(et.Ellipse)
def _is_pure_2d_ellipse(entity: et.Ellipse) -> bool:
    if not Z_AXIS.isclose(entity.dxf.extrusion):
        return False
    if abs(Vec3(entity.dxf.center).z) > LEN_TOL:
        return False
    return True


@is_pure_2d_entity.register(et.Spline)
def _is_pure_2d_spline(entity: et.Spline) -> bool:
    if not Z_AXIS.isclose(entity.dxf.extrusion):
        return False
    try:
        ct = entity.construction_tool()
    except ValueError:
        return False
    if any(abs(v.z) > LEN_TOL for v in ct.control_points):
        return False
    return True


@is_pure_2d_entity.register(et.LWPolyline)
def _is_pure_2d_lwpolyline(entity: et.LWPolyline) -> bool:
    if not Z_AXIS.isclose(entity.dxf.extrusion):
        return False
    if abs(entity.dxf.elevation) > LEN_TOL:
        return False
    if abs(entity.dxf.thickness) > LEN_TOL:
        return False
    return True


@is_pure_2d_entity.register(et.Polyline)
def _is_pure_2d_polyline(entity: et.Polyline) -> bool:
    if entity.is_polygon_mesh or entity.is_poly_face_mesh:
        return False
    if any(abs(v.z) > LEN_TOL for v in entity.points_in_wcs()):
        return False
    if abs(entity.dxf.thickness) > LEN_TOL:
        return False
    return True


@is_pure_2d_entity.register(et.Hatch)
def _is_pure_2d_hatch(entity: et.Hatch) -> bool:
    if not Z_AXIS.isclose(entity.dxf.extrusion):
        return False
    if abs(Vec3(entity.dxf.elevation).z) > LEN_TOL:
        return False
    return True


@is_pure_2d_entity.register(et.Trace)
@is_pure_2d_entity.register(et.Solid)
def _is_pure_2d_solid(entity: et.Solid | et.Trace) -> bool:
    if not Z_AXIS.isclose(entity.dxf.extrusion):
        return False
    if abs(entity.dxf.elevation) > LEN_TOL:
        return False
    if abs(entity.dxf.thickness) > LEN_TOL:
        return False
    if any(abs(v.z) > LEN_TOL for v in entity.wcs_vertices()):
        return False
    return True


def filter_edge_entities(entities: Iterable[et.DXFEntity]) -> Iterator[et.DXFEntity]:
    """Returns all entities that can be used to build edges in the context of
    :mod:`ezdxf.edgeminer`.

    Returns the following DXF entities:

        - LINE
        - ARC
        - ELLIPSE
        - SPLINE
        - LWPOLYLINE
        - POLYLINE

    .. note::

        - CIRCLE, TRACE and SOLID are closed shapes by definition and cannot be used as
          edge in the context of :mod:`ezdxf.edgeminer` or :mod:`ezdxf.edgesmith`.
        - This filter is not limited to pure 2D entities!
        - Does not test if the entity is a closed loop!

    """
    return (
        entity
        for entity in entities
        if isinstance(
            entity,
            (
                et.Line,
                et.Arc,
                et.Ellipse,
                et.Spline,
                et.LWPolyline,
                et.Polyline,
            ),
        )
    )


def filter_2d_entities(entities: Iterable[et.DXFEntity]) -> Iterator[et.DXFEntity]:
    """Returns all pure 2D entities, ignores all entities placed outside or extending
    beyond the xy-plane of the :ref:`WCS`. See :func:`is_pure_2d_entity` for more
    information.

    """
    return (e for e in entities if not is_pure_2d_entity(e))


def filter_open_edges(entities: Iterable[et.DXFEntity]) -> Iterator[et.DXFEntity]:
    """Returns all open linear entities usable as edges in the context of
    :mod:`ezdxf.edgeminer` or :mod:`ezdxf.edgesmith`.

    Ignores all entities that represent closed loops like circles, closed arcs, closed
    ellipses, closed splines and closed polylines.

    .. note::

        This filter is not limited to pure 2D entities!

    """
    edges = filter_edge_entities(entities)
    return (e for e in edges if not is_closed_entity(e))


def _validate_edge(edge: em.Edge, gap_tol: float) -> em.Edge | None:
    if edge.start.distance(edge.end) < gap_tol:
        return None
    if edge.length < gap_tol:
        return None
    return edge


# noinspection PyUnusedLocal
@functools.singledispatch
def make_edge_2d(entity: et.DXFEntity, *, gap_tol=GAP_TOL) -> em.Edge | None:
    """Makes an Edge instance from the following DXF entity types:

        - LINE (length accurate)
        - ARC (length accurate)
        - ELLIPSE (length approximated)
        - SPLINE (length approximated as straight lines between control points)
        - LWPOLYLINE (length of bulges as straight line from start- to end point)
        - POLYLINE (length of bulges as straight line from start- to end point)

    The start- and end points of the edge is projected onto the xy-plane. Returns
    ``None`` if the entity has a closed shape or cannot be represented as an edge.
    """
    return None


@make_edge_2d.register(et.Line)
def _edge_from_line(entity: et.Line, *, gap_tol=GAP_TOL) -> em.Edge | None:
    # line projected onto the xy-plane
    start = Vec2(entity.dxf.start)
    end = Vec2(entity.dxf.end)
    length = start.distance(end)
    return _validate_edge(em.make_edge(start, end, length, payload=entity), gap_tol)


@make_edge_2d.register(et.Arc)
def _edge_from_arc(entity: et.Arc, *, gap_tol=GAP_TOL) -> em.Edge | None:
    radius = abs(entity.dxf.radius)
    if radius < LEN_TOL:
        return None
    start_angle = entity.dxf.start_angle
    end_angle = entity.dxf.end_angle
    span_deg = arc_angle_span_deg(start_angle, end_angle)
    length = radius * span_deg / 180.0 * math.pi
    sp, ep = entity.vertices((start_angle, end_angle))
    return _validate_edge(
        # arc projected onto the xy-plane
        em.make_edge(Vec2(sp), Vec2(ep), length, payload=entity),
        gap_tol,
    )


@make_edge_2d.register(et.Ellipse)
def _edge_from_ellipse(entity: et.Ellipse, *, gap_tol=GAP_TOL) -> em.Edge | None:
    try:
        ct1 = entity.construction_tool()
    except ValueError:
        return None
    if ct1.major_axis.magnitude < LEN_TOL or ct1.minor_axis.magnitude < LEN_TOL:
        return None
    span = ellipse_param_span(ct1.start_param, ct1.end_param)
    num = max(3, round(span / 0.1745))  # resolution of ~1 deg
    # length of elliptic arc is an approximation:
    # ellipse projected onto the xy-plane
    points = Vec2.list(ct1.vertices(ct1.params(num)))
    length = sum(a.distance(b) for a, b in zip(points, points[1:]))
    return _validate_edge(
        em.make_edge(points[0], points[-1], length, payload=entity), gap_tol
    )


@make_edge_2d.register(et.Spline)
def _edge_from_spline(entity: et.Spline, *, gap_tol=GAP_TOL) -> em.Edge | None:
    try:
        ct2 = entity.construction_tool()
    except ValueError:
        return None
    # spline projected onto the xy-plane
    start = Vec2(ct2.control_points[0])
    end = Vec2(ct2.control_points[-1])
    points = Vec2.list(ct2.control_points)
    # length of B-spline is a very rough approximation:
    length = sum(a.distance(b) for a, b in zip(points, points[1:]))
    return _validate_edge(em.make_edge(start, end, length, payload=entity), gap_tol)


@make_edge_2d.register(et.LWPolyline)
def _edge_from_lwpolyline(entity: et.LWPolyline, *, gap_tol=GAP_TOL) -> em.Edge | None:
    if _is_closed_lwpolyline(entity):
        return None
    # polyline projected onto the xy-plane
    points = Vec2.list(entity.vertices_in_wcs())
    if len(points) < 2:
        return None
    start = points[0]
    end = points[-1]
    # length of LWPolyline does not include bulge length:
    length = sum(a.distance(b) for a, b in zip(points, points[1:]))
    return _validate_edge(em.make_edge(start, end, length, payload=entity), gap_tol)


@make_edge_2d.register(et.Polyline)
def _edge_from_polyline(entity: et.Polyline, *, gap_tol=GAP_TOL) -> em.Edge | None:
    if not (entity.is_2d_polyline or entity.is_3d_polyline):
        return None
    if _is_closed_polyline2d(entity):
        return None
    # polyline projected onto the xy-plane
    points = Vec2.list(entity.points_in_wcs())
    if len(points) < 2:
        return None

    start = points[0]
    end = points[-1]
    # length of Polyline does not include bulge length:
    length = sum(a.distance(b) for a, b in zip(points, points[1:]))
    return _validate_edge(em.make_edge(start, end, length, payload=entity), gap_tol)


def edges_from_entities_2d(
    entities: Iterable[et.DXFEntity], *, gap_tol=GAP_TOL
) -> Iterator[em.Edge]:
    """Yields all DXF entities as 2D edges in the xy-plane of the :ref:`WCS`.

    Skips all entities that have a closed shape or can not be represented as edge.
    """
    for entity in entities:
        edge = make_edge_2d(entity, gap_tol=gap_tol)
        if edge is not None:
            yield edge


def chain_vertices(edges: Sequence[em.Edge], *, gap_tol=GAP_TOL) -> Sequence[Vec3]:
    """Returns all vertices from a sequence of connected edges.

    Adds line segments between edges when the gap is bigger than `gap_tol`.
    """
    if not edges:
        return tuple()
    vertices: list[Vec3] = [edges[0].start]
    for edge in edges:
        if not em.isclose(vertices[-1], edge.start, gap_tol=gap_tol):
            vertices.append(edge.start)
        vertices.append(edge.end)
    return vertices


def lwpolyline_from_chain(
    edges: Sequence[em.Edge],
    *,
    dxfattribs: Any = None,
    max_sagitta: float = -1,
) -> et.LWPolyline:
    """Returns a new virtual :class:`~ezdxf.entities.LWPolyline` entity.

    This function assumes the building blocks as simple DXF entities attached as payload
    to the edges. The edges are processed in order of the input sequence and the start-
    and end points of the edges should be connected. The output polyline is projected
    onto the xy-plane of the :ref:`WCS`.

        - :class:`~ezdxf.entities.Line` as line segment
        - :class:`~ezdxf.entities.Arc` as bulge
        - :class:`~ezdxf.entities.Ellipse` as bulge or as flattened line segments
        - :class:`~ezdxf.entities.Spline` as flattened line segments
        - :class:`~ezdxf.entities.LWPolyline` and :class:`~ezdxf.entities.Polyline`
          will be merged
        - Everything else will be added as line segment from :attr:`Edge.start` to
          :attr:`Edge.end`
        - Gaps between edges are connected by line segments.

    """
    polyline = et.LWPolyline.new(dxfattribs=dxfattribs)
    if len(edges) == 0:
        return polyline
    polyline.set_points(_make_polyline_points(edges, max_sagitta), format="vb")  # type: ignore
    return polyline


def polyline2d_from_chain(
    edges: Sequence[em.Edge], *, dxfattribs: Any = None, max_sagitta: float = -1
) -> et.Polyline:
    """Returns a new virtual :class:`Polyline` entity.

    This function assumes the building blocks as simple DXF entities attached as payload
    to the edges. The edges are processed in order of the input sequence and the start-
    and end points of the edges should be connected. The output polyline is projected
    onto the xy-plane of the :ref:`WCS`.

        - :class:`~ezdxf.entities.Line` as line segment
        - :class:`~ezdxf.entities.Arc` as bulge
        - :class:`~ezdxf.entities.Ellipse` as bulge or as flattened line segments
        - :class:`~ezdxf.entities.Spline` as flattened line segments
        - :class:`~ezdxf.entities.LWPolyline` and :class:`~ezdxf.entities.Polyline`
          will be merged
        - Everything else will be added as line segment from :attr:`Edge.start` to
          :attr:`Edge.end`
        - Gaps between edges are connected by line segments.

    """
    polyline = et.Polyline.new(dxfattribs=dxfattribs)
    if len(edges) == 0:
        return polyline
    polyline.append_formatted_vertices(
        _make_polyline_points(edges, max_sagitta), format="vb"
    )
    return polyline


BulgePoints: TypeAlias = list[tuple[Vec2, float]]


def _adjust_max_sagitta(max_sagitta: float, length: float) -> float:
    if max_sagitta < 0:
        max_sagitta = length / 100.0
    return max_sagitta


def _flatten_3d_entity(
    entity, length: float, max_sagitta: float, is_reverse: bool
) -> BulgePoints:
    points: BulgePoints = []
    try:
        entity_path = path.make_path(entity)
    except TypeError:
        return points
    max_sagitta = _adjust_max_sagitta(max_sagitta, length)
    if max_sagitta > LEN_TOL:
        points.extend((p, 0.0) for p in Vec2.list(entity_path.flattening(max_sagitta)))
    if is_reverse:
        # edge.start and edge.end are in correct order
        # start and end of the attached entity are NOT in correct order
        points.reverse()
    return points


def _arc_to_bulge_points(edge: em.Edge, max_sagitta: float) -> BulgePoints:
    arc: et.Arc = edge.payload
    if Z_AXIS.isclose(arc.dxf.extrusion):
        span = arc_angle_span_deg(arc.dxf.start_angle, arc.dxf.end_angle)
        bulge: float = 0.0
        if span > DEG_TOL:
            bulge = bulge_from_arc_angle(math.radians(span))
            if edge.is_reverse:
                bulge = -bulge
        return [
            (Vec2(edge.start), bulge),
            (Vec2(edge.end), 0.0),
        ]
    # flatten arc and project onto xy-plane
    return _flatten_3d_entity(arc, edge.length, max_sagitta, edge.is_reverse)


def _ellipse_to_bulge_points(edge: em.Edge, max_sagitta: float) -> BulgePoints:
    ellipse: et.Ellipse = edge.payload
    ratio = abs(ellipse.dxf.ratio)
    span = ellipse_param_span(ellipse.dxf.start_param, ellipse.dxf.end_param)
    if math.isclose(ratio, 1.0) and Z_AXIS.isclose(ellipse.dxf.extrusion):
        bulge = 0.0
        if span > RAD_TOL:
            bulge = bulge_from_arc_angle(span)
            if edge.is_reverse:
                bulge = -bulge
        return [
            (Vec2(edge.start), bulge),
            (Vec2(edge.end), 0.0),
        ]
    # flatten ellipse and project onto xy-plane
    return _flatten_3d_entity(ellipse, edge.length, max_sagitta, edge.is_reverse)


def _sum_distances(vertices: Sequence[Vec3]) -> float:
    if len(vertices) < 2:
        return 0.0
    return sum(p0.distance(p1) for p0, p1 in zip(vertices, vertices[1:]))


def _max_sagitta_spline(max_sagitta: float, control_points: Sequence[Vec3]):
    if max_sagitta > 0:
        return max_sagitta
    return _sum_distances(control_points) / 100.0


def _spline_to_bulge_points(edge: em.Edge, max_sagitta: float) -> BulgePoints:
    spline: et.Spline = edge.payload
    points: BulgePoints = []
    try:
        ct = spline.construction_tool()
    except ValueError:
        return points

    max_sagitta = _max_sagitta_spline(max_sagitta, ct.control_points)
    if max_sagitta > LEN_TOL:
        points.extend((Vec2(p), 0.0) for p in ct.flattening(max_sagitta))
    if edge.is_reverse:
        # edge.start and edge.end are in correct order
        # start and end of the attached entity are NOT in correct order
        points.reverse()
    return points


def _lwpolyline_to_bulge_points(edge: em.Edge, max_sagitta: float) -> BulgePoints:
    pl: et.LWPolyline = edge.payload
    if Z_AXIS.isclose(pl.dxf.extrusion):
        points = [(Vec2(x, y), b) for x, y, b in pl.get_points(format="xyb")]
        if edge.is_reverse:
            return _revert_bulge_points(points)
        return points
    return _flatten_3d_entity(pl, edge.length, max_sagitta, edge.is_reverse)


def _polyline2d_to_bulge_points(edge: em.Edge, max_sagitta: float) -> BulgePoints:
    pl: et.Polyline = edge.payload

    if pl.is_2d_polyline and Z_AXIS.isclose(pl.dxf.extrusion):
        points = [(Vec2(v.dxf.location), v.dxf.bulge) for v in pl.vertices]
        if edge.is_reverse:
            return _revert_bulge_points(points)
        return points
    return _flatten_3d_entity(pl, edge.length, max_sagitta, edge.is_reverse)


def _revert_bulge_points(pts: BulgePoints) -> BulgePoints:
    if len(pts) < 2:
        return pts
    pts.reverse()
    bulges = [pnt[1] for pnt in pts]
    if any(bulges):
        # shift bulge values to previous vertex
        first = bulges.pop(0)
        bulges.append(first)
        return list(zip((pnt[0] for pnt in pts), bulges))
    return pts


def _extend_bulge_points(points: BulgePoints, extension: BulgePoints) -> BulgePoints:
    if len(extension) < 2:
        return points
    try:
        current_end, _ = points[-1]
    except IndexError:
        points.extend(extension)
        return points

    connection_point, _ = extension[0]
    if current_end.distance(connection_point) < LEN_TOL:
        points.pop()
    points.extend(extension)
    return points


POLYLINE_CONVERTERS = {
    et.Arc: _arc_to_bulge_points,
    et.Ellipse: _ellipse_to_bulge_points,
    et.Spline: _spline_to_bulge_points,
    et.LWPolyline: _lwpolyline_to_bulge_points,
    et.Polyline: _polyline2d_to_bulge_points,
}


def _make_polyline_points(edges: Sequence[em.Edge], max_sagitta: float) -> BulgePoints:
    """Returns the polyline points to create a LWPolyline or a 2D Polyline entity."""
    points: BulgePoints = []
    extension: BulgePoints = []
    for edge in edges:
        extension.clear()
        converter = POLYLINE_CONVERTERS.get(type(edge.payload))
        if converter:
            extension = converter(edge, max_sagitta)
        if len(extension) < 2:
            extension = [
                (Vec2(edge.start), 0.0),
                (Vec2(edge.end), 0.0),
            ]
        points = _extend_bulge_points(points, extension)
    return points


def polyline_path_from_chain(
    edges: Sequence[em.Edge], *, max_sagitta: float = -1, is_closed=True, flags: int = 1
) -> bp.PolylinePath:
    """Returns a new :class:`~ezdxf.entities.PolylinePath` for :class:`~ezdxf.entities.Hatch`
    entities.

    This function assumes the building blocks as simple DXF entities attached as payload
    to the edges. The edges are processed in order of the input sequence and the start-
    and end points of the edges should be connected. The output path is projected onto
    the xy-plane of the :ref:`WCS`.

        - :class:`~ezdxf.entities.Line` as line segment
        - :class:`~ezdxf.entities.Arc` as bulge
        - :class:`~ezdxf.entities.Ellipse` as bulge or flattened line segments
        - :class:`~ezdxf.entities.Spline` as flattened line segments
        - :class:`~ezdxf.entities.LWPolyline` and :class:`~ezdxf.entities.Polyline` are
          merged
        - Everything else will be added as line segment from :attr:`Edge.start` to
          :attr:`Edge.end`
        - Gaps between edges are connected by line segments.

    Args:
        edges: Sequence of :class:`~ezdxf.edgeminer.Edge` instances with DXF primitives
            attached as payload
        max_sagitta (float): curve flattening parameter
        is_closed (bool): ``True`` if path is implicit closed
        flags (int): default(0), external(1), derived(4), textbox(8) or outermost(16)

    """
    polyline_path = bp.PolylinePath()
    polyline_path.is_closed = bool(is_closed)
    polyline_path.path_type_flags |= flags
    if len(edges) == 0:
        return polyline_path
    bulge_points = _make_polyline_points(edges, max_sagitta)
    polyline_path.set_vertices([(p.x, p.y, b) for p, b in bulge_points])
    polyline_path.is_closed = True
    return polyline_path


def edge_path_from_chain(
    edges: Sequence[em.Edge], *, max_sagitta: float = -1, flags: int = 1
) -> bp.EdgePath:
    """Returns a new :class:`~ezdxf.entities.EdgePath` for :class:`~ezdxf.entities.Hatch`
    entities.

    This function assumes the building blocks as simple DXF entities attached as payload
    to the edges. The edges are processed in order of the input sequence and the start-
    and end points of the edges should be connected. The output path is projected onto
    the xy-plane of the :ref:`WCS`.

        - :class:`~ezdxf.entities.Line` as :class:`~ezdxf.entities.LineEdge`
        - :class:`~ezdxf.entities.Arc` as :class:`~ezdxf.entities.ArcEdge`
        - :class:`~ezdxf.entities.Ellipse` as :class:`~ezdxf.entities.EllipseEdge`
        - :class:`~ezdxf.entities.Spline` as :class:`~ezdxf.entities.SplineEdge`
        - :class:`~ezdxf.entities.LWPolyline` and :class:`~ezdxf.entities.Polyline` will
          be exploded and added as :class:`~ezdxf.entities.LineEdge` and
          :class:`~ezdxf.entities.ArcEdge`
        - Everything else will be added as line segment from :attr:`Edge.start` to
          :attr:`Edge.end`
        - Gaps between edges are connected by line segments.

    Args:
        edges: Sequence of :class:`~ezdxf.edgeminer.Edge` instances with DXF primitives
            attached as payload
        max_sagitta (float): curve flattening parameter
        flags (int): default(0), external(1), derived(4), textbox(8) or outermost(16)

    """
    edge_path = bp.EdgePath()
    edge_path.path_type_flags |= flags
    if len(edges) == 0:
        return edge_path

    for edge in edges:
        entity = edge.payload
        distance = _adjust_max_sagitta(max_sagitta, edge.length)
        reverse = edge.is_reverse
        if isinstance(entity, et.Line):
            _add_line_to_edge_path_2d(edge_path, entity, reverse)
        elif isinstance(entity, et.Arc):
            _add_arc_to_edge_path_2d(edge_path, entity, reverse, distance)
        elif isinstance(entity, et.Ellipse):
            _add_ellipse_to_edge_path_2d(edge_path, entity, reverse, distance)
        elif isinstance(entity, et.Spline):
            _add_spline_to_edge_path_2d(edge_path, entity, reverse)
        elif isinstance(entity, et.Polyline) and (
            entity.is_2d_polyline or entity.is_3d_polyline
        ):
            _add_polyline_parts_to_edge_path(
                edge_path, list(entity.virtual_entities()), reverse, max_sagitta
            )
        elif isinstance(entity, et.LWPolyline):
            _add_polyline_parts_to_edge_path(
                edge_path, list(entity.virtual_entities()), reverse, max_sagitta
            )
        else:
            edge_path.add_line(edge.start, edge.end)
    edge_path.close_gaps(LEN_TOL)
    return edge_path


def _add_line_to_edge_path_2d(
    edge_path: bp.EdgePath, line: et.Line, reverse: bool
) -> None:
    start = Vec2(line.dxf.start)
    end = Vec2(line.dxf.end)
    if reverse:
        start, end = end, start
    edge_path.add_line(start, end)


def _add_arc_to_edge_path_2d(
    edge_path: bp.EdgePath, arc: et.Arc, reverse: bool, max_sagitta: float
) -> None:
    if Z_AXIS.isclose(arc.dxf.extrusion):
        edge_path.add_arc(
            center=Vec2(arc.dxf.center),
            radius=arc.dxf.radius,
            start_angle=arc.dxf.start_angle,
            end_angle=arc.dxf.end_angle,
            ccw=not reverse,
        )
    else:
        vertices = Vec2.list(arc.flattening(max_sagitta))
        if reverse:
            vertices.reverse()
        _add_vertices_to_edge_path_2d(edge_path, vertices)


def _add_ellipse_to_edge_path_2d(
    edge_path: bp.EdgePath, ellipse: et.Ellipse, reverse: bool, max_sagitta: float
) -> None:
    if Z_AXIS.isclose(ellipse.dxf.extrusion):
        try:
            ct = ellipse.construction_tool()
        except ValueError:
            return
        edge_path.add_ellipse(
            center=Vec2(ct.center),
            major_axis=Vec2(ct.major_axis),
            ratio=ct.ratio,
            start_angle=math.degrees(ct.start_param),
            end_angle=math.degrees(ct.end_param),
            ccw=not reverse,
        )
    else:
        vertices = Vec2.list(ellipse.flattening(max_sagitta))
        if reverse:
            vertices.reverse()
        _add_vertices_to_edge_path_2d(edge_path, vertices)


def _add_spline_to_edge_path_2d(
    edge_path: bp.EdgePath, spline: et.Spline, reverse: bool
) -> None:
    try:
        ct = spline.construction_tool()
    except ValueError:
        return
    control_points = Vec2.list(ct.control_points)
    knots = list(ct.knots())
    weights = list(ct.weights())
    if reverse:
        control_points.reverse()
        knots.reverse()
        weights.reverse()

    edge_path.add_spline(
        control_points=control_points,
        knot_values=knots,
        weights=weights if weights else None,
        degree=ct.degree,
    )


def _add_vertices_to_edge_path_2d(edge_path: bp.EdgePath, vertices: list[Vec2]) -> None:
    for start, end in zip(vertices, vertices[1:]):
        edge_path.add_line(start, end)


def _add_polyline_parts_to_edge_path(
    edge_path: bp.EdgePath,
    entities: list[et.DXFGraphic],
    reverse: bool,
    max_sagitta: float,
) -> None:
    # reverse inverted extrusion vectors of ARC entities, this may reverse ARCs
    upright_all(entities)

    # re-connect reversed ARCs
    edges = list(edges_from_entities_2d(entities))
    edges = list(em.find_sequential_chain(edges))
    if reverse:
        edges = em.reverse_chain(edges)

    for edge in edges:
        entity = edge.payload
        if isinstance(entity, et.Line):
            _add_line_to_edge_path_2d(edge_path, entity, edge.is_reverse)
        elif isinstance(entity, et.Arc):
            distance = _adjust_max_sagitta(max_sagitta, edge.length)
            _add_arc_to_edge_path_2d(edge_path, entity, edge.is_reverse, distance)


def path2d_from_chain(edges: Sequence[em.Edge]) -> path.Path:
    """Returns a new :class:`ezdxf.path.Path` entity.

    This function assumes the building blocks as simple DXF entities attached as payload
    to the edges. The edges are processed in order of the input sequence and the start-
    and end points of the edges should be connected. The output is a 2D path projected
    onto the xy-plane of the :ref:`WCS`.

        - :class:`~ezdxf.entities.Line` as line segment
        - :class:`~ezdxf.entities.Arc` as cubic Bézier curves
        - :class:`~ezdxf.entities.Ellipse` as cubic Bézier curves
        - :class:`~ezdxf.entities.Spine` cubic Bézier curves
        - :class:`~ezdxf.entities.LWPolyline` and :class:`~ezdxf.entities.Polyline`
          as line segments and cubic Bézier curves
        - Everything else will be added as line segment from :attr:`Edge.start` to
          :attr:`Edge.end`
        - Gaps between edges are connected by line segments.

    """
    main_path = path.Path()
    if len(edges) == 0:
        return main_path

    # project vertices onto the xy-plane by multiplying the z-axis by 0
    m = Matrix44.scale(1.0, 1.0, 0.0)
    for edge in edges:
        try:
            sub_path = path.make_path(edge.payload)
        except (ValueError, TypeError):
            continue
        sub_path = sub_path.transform(m)
        if edge.is_reverse:
            sub_path = sub_path.reversed()
        main_path.append_path(sub_path)
    return main_path


class IntersectingEdge(NamedTuple):
    edge: em.Edge
    distance: float


def bounding_box_2d(edges: Sequence[em.Edge]) -> BoundingBox2d:
    """Returns the :class:`~ezdxf.math.BoundingBox2d` of all start- and end vertices."""
    bbox = BoundingBox2d(e.start for e in edges)
    bbox.extend(e.end for e in edges)
    return bbox


def intersecting_edges_2d(
    edges: Sequence[em.Edge], p1: UVec, p2: UVec | None = None
) -> list[IntersectingEdge]:
    """Returns all edges that intersect a line from point `p1` to point `p2`.

    If `p2` is ``None`` an arbitrary point outside the bounding box of all start- and
    end vertices beyond extmax.x will be chosen.
    The edges are handled as straight lines from start- to end vertex, projected onto
    the xy-plane of the :ref:`WCS`. The result is sorted by the distance from
    intersection point to point `p1`.
    """
    intersections: list[IntersectingEdge] = []
    if len(edges) == 0:
        return intersections

    inner_point = Vec2(p1)
    if p2 is None:
        bbox = bounding_box_2d(edges)
        outer_point = Vec2(bbox.extmax.x + 1.0, inner_point.y)
    else:
        outer_point = Vec2(p2)

    test_line = (inner_point, outer_point)
    for edge in edges:
        start = Vec2(edge.start)
        end = Vec2(edge.end)
        ip = intersection_line_line_2d(test_line, (start, end), virtual=False)
        if ip is not None:
            intersections.append(IntersectingEdge(edge, inner_point.distance(ip)))
    intersections.sort(key=itemgetter(1))
    return intersections


def is_inside_polygon_2d(
    edges: Sequence[em.Edge], point: UVec, *, gap_tol=GAP_TOL
) -> bool:
    """Returns ``True`` when `point` is inside the polygon.

    The edges must be a closed loop. The polygon is build from edges as straight lines
    from start- to end vertex, independently whatever the edges really represent.
    A point at a boundary line is inside the polygon.

    Args:
        edges: sequence of :class:`~ezdxf.edgeminer.Edge` representing a closed loop
        point: point to test
        gap_tol: maximum vertex distance to consider two edges as connected

    Raises:
        ValueError: edges are not a closed loop

    """
    if not em.is_loop(edges, gap_tol=gap_tol):
        raise ValueError("edges are not a closed loop")
    polygon = Vec2.list(e.start for e in edges)
    # The function uses an open polygon for testing, no need to close the polygon.
    return is_point_in_polygon_2d(Vec2(point), polygon) >= 0.0


def loop_area(edges: Sequence[em.Edge], *, gap_tol=GAP_TOL) -> float:
    """Returns the area of a closed loop.

    Raises:
        ValueError: edges are not a closed loop

    """
    if not em.is_loop(edges, gap_tol=gap_tol):
        raise ValueError("edges are not a closed loop")
    return area(e.start for e in edges)
