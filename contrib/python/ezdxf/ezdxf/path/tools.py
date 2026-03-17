# Copyright (c) 2020-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    Optional,
    Sequence,
)
import math
import numpy as np

from ezdxf.math import (
    Vec2,
    Vec3,
    UVec,
    Z_AXIS,
    OCS,
    UCS,
    Matrix44,
    BoundingBox,
    ConstructionEllipse,
    cubic_bezier_from_ellipse,
    Bezier4P,
    Bezier3P,
    BSpline,
    reverse_bezier_curves,
    bulge_to_arc,
    linear_vertex_spacing,
    inscribe_circle_tangent_length,
    cubic_bezier_arc_parameters,
    cubic_bezier_bbox,
    quadratic_bezier_bbox,
)
from ezdxf.math.triangulation import mapbox_earcut_2d
from ezdxf.query import EntityQuery

from .path import Path
from .commands import Command
from . import converter, nesting

if TYPE_CHECKING:
    from ezdxf.query import EntityQuery
    from ezdxf.eztypes import GenericLayoutType


__all__ = [
    "bbox",
    "precise_bbox",
    "fit_paths_into_box",
    "transform_paths",
    "transform_paths_to_ocs",
    "render_lwpolylines",
    "render_polylines2d",
    "render_polylines3d",
    "render_lines",
    "render_hatches",
    "render_mpolygons",
    "render_splines_and_polylines",
    "add_bezier4p",
    "add_bezier3p",
    "add_ellipse",
    "add_2d_polyline",
    "add_spline",
    "to_multi_path",
    "single_paths",
    "have_close_control_vertices",
    "lines_to_curve3",
    "lines_to_curve4",
    "fillet",
    "polygonal_fillet",
    "chamfer",
    "chamfer2",
    "triangulate",
    "is_rectangular",
]

MAX_DISTANCE = 0.01
MIN_SEGMENTS = 4
G1_TOL = 1e-4
IS_CLOSE_TOL = 1e-10


def to_multi_path(paths: Iterable[Path]) -> Path:
    """Returns a multi-path object from all given paths and their sub-paths.
    Ignores paths without any commands (empty paths).
    """
    multi_path = Path()
    for p in paths:
        multi_path.extend_multi_path(p)
    return multi_path


def single_paths(paths: Iterable[Path]) -> Iterable[Path]:
    """Yields all given paths and their sub-paths as single path objects."""
    for p in paths:
        if p.has_sub_paths:
            yield from p.sub_paths()
        else:
            yield p


def transform_paths(paths: Iterable[Path], m: Matrix44) -> list[Path]:
    """Transform multiple path objects at once by transformation
    matrix `m`. Returns a list of the transformed path objects.

    Args:
        paths: iterable of :class:`Path` or :class:`Path2d` objects
        m: transformation matrix of type :class:`~ezdxf.math.Matrix44`

    """
    return [p.transform(m) for p in paths]


def transform_paths_to_ocs(paths: Iterable[Path], ocs: OCS) -> list[Path]:
    """Transform multiple :class:`Path` objects at once from WCS to OCS.
    Returns a list of the transformed :class:`Path` objects.

    Args:
        paths: iterable of :class:`Path` or :class:`Path2d` objects
        ocs: OCS transformation of type :class:`~ezdxf.math.OCS`

    """
    t = ocs.matrix.copy()
    t.transpose()
    return transform_paths(paths, t)


def bbox(paths: Iterable[Path], *, fast=False) -> BoundingBox:
    """Returns the :class:`~ezdxf.math.BoundingBox` for the given paths.

    Args:
        paths: iterable of :class:`Path` or :class:`Path2d` objects
        fast: calculates the precise bounding box of Bèzier curves if
            ``False``, otherwise uses the control points of Bézier curves to
            determine their bounding box.

    """
    box = BoundingBox()
    for p in paths:
        if fast:
            box.extend(p.control_vertices())
        else:
            bb = precise_bbox(p)
            if bb.has_data:
                box.extend((bb.extmin, bb.extmax))
    return box


def precise_bbox(path: Path) -> BoundingBox:
    """Returns the precise :class:`~ezdxf.math.BoundingBox` for the given paths."""
    if len(path) == 0:  # empty path
        return BoundingBox()
    start = path.start
    points: list[Vec3] = [start]
    for cmd in path.commands():
        if cmd.type == Command.LINE_TO:
            points.append(cmd.end)
        elif cmd.type == Command.CURVE4_TO:
            bb = cubic_bezier_bbox(
                Bezier4P((start, cmd.ctrl1, cmd.ctrl2, cmd.end))  # type: ignore
            )
            points.append(bb.extmin)
            points.append(bb.extmax)
        elif cmd.type == Command.CURVE3_TO:
            bb = quadratic_bezier_bbox(Bezier3P((start, cmd.ctrl, cmd.end)))  # type: ignore
            points.append(bb.extmin)
            points.append(bb.extmax)
        elif cmd.type == Command.MOVE_TO:
            points.append(cmd.end)
        start = cmd.end

    return BoundingBox(points)


def fit_paths_into_box(
    paths: Iterable[Path],
    size: tuple[float, float, float],
    uniform: bool = True,
    source_box: Optional[BoundingBox] = None,
) -> list[Path]:
    """Scale the given `paths` to fit into a box of the given `size`,
    so that all path vertices are inside these borders.
    If `source_box` is ``None`` the default source bounding box is calculated
    from the control points of the `paths`.

    `Note:` if the target size has a z-size of 0, the `paths` are
    projected into the xy-plane, same is true for the x-size, projects into
    the yz-plane and the y-size, projects into and xz-plane.

    Args:
        paths: iterable of :class:`~ezdxf.path.Path` objects
        size: target box size as tuple of x-, y- and z-size values
        uniform: ``True`` for uniform scaling
        source_box: pass precalculated source bounding box, or ``None`` to
            calculate the default source bounding box from the control vertices

    """
    paths = list(paths)
    if len(paths) == 0:
        return paths
    if source_box is None:
        current_box = bbox(paths, fast=True)
    else:
        current_box = source_box
    if not current_box.has_data or current_box.size == (0, 0, 0):
        return paths
    target_size = Vec3(size)
    if target_size == (0, 0, 0) or min(target_size) < 0:
        raise ValueError("invalid target size")

    if uniform:
        sx, sy, sz = _get_uniform_scaling(current_box.size, target_size)
    else:
        sx, sy, sz = _get_non_uniform_scaling(current_box.size, target_size)
    m = Matrix44.scale(sx, sy, sz)
    return transform_paths(paths, m)


def _get_uniform_scaling(current_size: Vec3, target_size: Vec3):
    TOL = 1e-6
    scale_x = math.inf
    if current_size.x > TOL and target_size.x > TOL:
        scale_x = target_size.x / current_size.x
    scale_y = math.inf
    if current_size.y > TOL and target_size.y > TOL:
        scale_y = target_size.y / current_size.y
    scale_z = math.inf
    if current_size.z > TOL and target_size.z > TOL:
        scale_z = target_size.z / current_size.z

    uniform_scale = min(scale_x, scale_y, scale_z)
    if uniform_scale is math.inf:
        raise ArithmeticError("internal error")
    scale_x = uniform_scale if target_size.x > TOL else 0
    scale_y = uniform_scale if target_size.y > TOL else 0
    scale_z = uniform_scale if target_size.z > TOL else 0
    return scale_x, scale_y, scale_z


def _get_non_uniform_scaling(current_size: Vec3, target_size: Vec3):
    TOL = 1e-6
    scale_x = 1.0
    if current_size.x > TOL:
        scale_x = target_size.x / current_size.x
    scale_y = 1.0
    if current_size.y > TOL:
        scale_y = target_size.y / current_size.y
    scale_z = 1.0
    if current_size.z > TOL:
        scale_z = target_size.z / current_size.z
    return scale_x, scale_y, scale_z


# Path to entity converter and render utilities:


def render_lwpolylines(
    layout: GenericLayoutType,
    paths: Iterable[Path],
    *,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    extrusion: UVec = Z_AXIS,
    dxfattribs=None,
) -> EntityQuery:
    """Render the given `paths` into `layout` as
    :class:`~ezdxf.entities.LWPolyline` entities.
    The `extrusion` vector is applied to all paths, all vertices are projected
    onto the plane normal to this extrusion vector. The default extrusion vector
    is the WCS z-axis. The plane elevation is the distance from the WCS origin
    to the start point of the first path.

    Args:
        layout: the modelspace, a paperspace layout or a block definition
        paths: iterable of :class:`Path` or :class:`Path2d` objects
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve
        extrusion: extrusion vector for all paths
        dxfattribs: additional DXF attribs

    Returns:
        created entities in an :class:`~ezdxf.query.EntityQuery` object

    """
    lwpolylines = list(
        converter.to_lwpolylines(
            paths,
            distance=distance,
            segments=segments,
            extrusion=extrusion,
            dxfattribs=dxfattribs,
        )
    )
    for lwpolyline in lwpolylines:
        layout.add_entity(lwpolyline)
    return EntityQuery(lwpolylines)


def render_polylines2d(
    layout: GenericLayoutType,
    paths: Iterable[Path],
    *,
    distance: float = 0.01,
    segments: int = 4,
    extrusion: UVec = Z_AXIS,
    dxfattribs=None,
) -> EntityQuery:
    """Render the given `paths` into `layout` as 2D
    :class:`~ezdxf.entities.Polyline` entities.
    The `extrusion` vector is applied to all paths, all vertices are projected
    onto the plane normal to this extrusion vector.The default extrusion vector
    is the WCS z-axis. The plane elevation is the distance from the WCS origin
    to the start point of the first path.

    Args:
        layout: the modelspace, a paperspace layout or a block definition
        paths: iterable of :class:`Path` or :class:`Path2d` objects
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve
        extrusion: extrusion vector for all paths
        dxfattribs: additional DXF attribs

    Returns:
        created entities in an :class:`~ezdxf.query.EntityQuery` object

    """
    polylines2d = list(
        converter.to_polylines2d(
            paths,
            distance=distance,
            segments=segments,
            extrusion=extrusion,
            dxfattribs=dxfattribs,
        )
    )
    for polyline2d in polylines2d:
        layout.add_entity(polyline2d)
    return EntityQuery(polylines2d)


def render_hatches(
    layout: GenericLayoutType,
    paths: Iterable[Path],
    *,
    edge_path: bool = True,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    g1_tol: float = G1_TOL,
    extrusion: UVec = Z_AXIS,
    dxfattribs=None,
) -> EntityQuery:
    """Render the given `paths` into `layout` as
    :class:`~ezdxf.entities.Hatch` entities.
    The `extrusion` vector is applied to all paths, all vertices are projected
    onto the plane normal to this extrusion vector. The default extrusion vector
    is the WCS z-axis. The plane elevation is the distance from the WCS origin
    to the start point of the first path.

    Args:
        layout: the modelspace, a paperspace layout or a block definition
        paths: iterable of :class:`Path` or :class:`Path2d`  objects
        edge_path: ``True`` for edge paths build of LINE and SPLINE edges,
            ``False`` for only LWPOLYLINE paths as boundary paths
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve to flatten polyline paths
        g1_tol: tolerance for G1 continuity check to separate SPLINE edges
        extrusion: extrusion vector for all paths
        dxfattribs: additional DXF attribs

    Returns:
        created entities in an :class:`~ezdxf.query.EntityQuery` object

    """
    hatches = list(
        converter.to_hatches(
            paths,
            edge_path=edge_path,
            distance=distance,
            segments=segments,
            g1_tol=g1_tol,
            extrusion=extrusion,
            dxfattribs=dxfattribs,
        )
    )
    for hatch in hatches:
        layout.add_entity(hatch)
    return EntityQuery(hatches)


def render_mpolygons(
    layout: GenericLayoutType,
    paths: Iterable[Path],
    *,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    extrusion: UVec = Z_AXIS,
    dxfattribs=None,
) -> EntityQuery:
    """Render the given `paths` into `layout` as
    :class:`~ezdxf.entities.MPolygon` entities. The MPOLYGON entity supports
    only polyline boundary paths. All curves will be approximated.

    The `extrusion` vector is applied to all paths, all vertices are projected
    onto the plane normal to this extrusion vector. The default extrusion vector
    is the WCS z-axis. The plane elevation is the distance from the WCS origin
    to the start point of the first path.

    Args:
        layout: the modelspace, a paperspace layout or a block definition
        paths: iterable of :class:`Path` or :class:`Path2d` objects
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve to flatten polyline paths
        extrusion: extrusion vector for all paths
        dxfattribs: additional DXF attribs

    Returns:
        created entities in an :class:`~ezdxf.query.EntityQuery` object

    """
    polygons = list(
        converter.to_mpolygons(
            paths,
            distance=distance,
            segments=segments,
            extrusion=extrusion,
            dxfattribs=dxfattribs,
        )
    )
    for polygon in polygons:
        layout.add_entity(polygon)
    return EntityQuery(polygons)


def render_polylines3d(
    layout: GenericLayoutType,
    paths: Iterable[Path],
    *,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    dxfattribs=None,
) -> EntityQuery:
    """Render the given `paths` into `layout` as 3D
    :class:`~ezdxf.entities.Polyline` entities.

    Args:
        layout: the modelspace, a paperspace layout or a block definition
        paths: iterable of :class:`Path`or :class:`Path2d` objects
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve
        dxfattribs: additional DXF attribs

    Returns:
        created entities in an :class:`~ezdxf.query.EntityQuery` object

    """

    polylines3d = list(
        converter.to_polylines3d(
            paths,
            distance=distance,
            segments=segments,
            dxfattribs=dxfattribs,
        )
    )
    for polyline3d in polylines3d:
        layout.add_entity(polyline3d)
    return EntityQuery(polylines3d)


def render_lines(
    layout: GenericLayoutType,
    paths: Iterable[Path],
    *,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    dxfattribs=None,
) -> EntityQuery:
    """Render the given `paths` into `layout` as
    :class:`~ezdxf.entities.Line` entities.

    Args:
        layout: the modelspace, a paperspace layout or a block definition
        paths: iterable of :class:`Path`or :class:`Path2d` objects
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve
        dxfattribs: additional DXF attribs

    Returns:
        created entities in an :class:`~ezdxf.query.EntityQuery` object

    """
    lines = list(
        converter.to_lines(
            paths,
            distance=distance,
            segments=segments,
            dxfattribs=dxfattribs,
        )
    )
    for line in lines:
        layout.add_entity(line)
    return EntityQuery(lines)


def render_splines_and_polylines(
    layout: GenericLayoutType,
    paths: Iterable[Path],
    *,
    g1_tol: float = G1_TOL,
    dxfattribs=None,
) -> EntityQuery:
    """Render the given `paths` into `layout` as :class:`~ezdxf.entities.Spline`
    and 3D :class:`~ezdxf.entities.Polyline` entities.

    Args:
        layout: the modelspace, a paperspace layout or a block definition
        paths: iterable of :class:`Path`or :class:`Path2d` objects
        g1_tol: tolerance for G1 continuity check
        dxfattribs: additional DXF attribs

    Returns:
        created entities in an :class:`~ezdxf.query.EntityQuery` object

    """
    entities = list(
        converter.to_splines_and_polylines(
            paths,
            g1_tol=g1_tol,
            dxfattribs=dxfattribs,
        )
    )
    for entity in entities:
        layout.add_entity(entity)
    return EntityQuery(entities)


def add_ellipse(
    path: Path, ellipse: ConstructionEllipse, segments=1, reset=True
) -> None:
    """Add an elliptical arc as multiple cubic Bèzier-curves to the given
    `path`, use :meth:`~ezdxf.math.ConstructionEllipse.from_arc` constructor
    of class :class:`~ezdxf.math.ConstructionEllipse` to add circular arcs.

    Auto-detect the connection point to the given `path`, if neither the start-
    nor the end point of the ellipse is close to the path end point, a line from
    the path end point to the ellipse start point will be added automatically
    (see :func:`add_bezier4p`).

    By default, the start of an **empty** path is set to the start point of
    the ellipse, setting argument `reset` to ``False`` prevents this
    behavior.

    Args:
        path: :class:`~ezdxf.path.Path` object
        ellipse: ellipse parameters as :class:`~ezdxf.math.ConstructionEllipse`
            object
        segments: count of Bèzier-curve segments, at least one segment for
            each quarter (pi/2), ``1`` for as few as possible.
        reset: set start point to start of ellipse if path is empty

    """
    if abs(ellipse.param_span) < 1e-9:
        return
    if len(path) == 0 and reset:
        path.start = ellipse.start_point
    add_bezier4p(path, cubic_bezier_from_ellipse(ellipse, segments))


def add_bezier4p(path: Path, curves: Iterable[Bezier4P]) -> None:
    """Add multiple cubic Bèzier-curves to the given `path`.

    Auto-detect the connection point to the given `path`, if neither the start-
    nor the end point of the curves is close to the path end point, a line from
    the path end point to the start point of the first curve will be added
    automatically.

    """
    rel_tol = 1e-15
    abs_tol = 0.0
    curves = list(curves)
    if not len(curves):
        return
    end = curves[-1].control_points[-1]
    if path.end.isclose(end):
        # connect to new curves end point
        curves = reverse_bezier_curves(curves)

    for curve in curves:
        start, ctrl1, ctrl2, end = curve.control_points
        if not start.isclose(path.end):
            path.line_to(start)

        # add linear bezier segments as LINE_TO commands
        if start.isclose(ctrl1, rel_tol=rel_tol, abs_tol=abs_tol) and end.isclose(
            ctrl2, rel_tol=rel_tol, abs_tol=abs_tol
        ):
            path.line_to(end)
        else:
            path.curve4_to(end, ctrl1, ctrl2)


def add_bezier3p(path: Path, curves: Iterable[Bezier3P]) -> None:
    """Add multiple quadratic Bèzier-curves to the given `path`.

    Auto-detect the connection point to the given `path`, if neither the start-
    nor the end point of the curves is close to the path end point, a line from
    the path end point to the start point of the first curve will be added
    automatically.

    """
    rel_tol = 1e-15
    abs_tol = 0.0
    curves = list(curves)
    if not len(curves):
        return
    end = curves[-1].control_points[-1]
    if path.end.isclose(end):
        # connect to new curves end point
        curves = reverse_bezier_curves(curves)

    for curve in curves:
        start, ctrl, end = curve.control_points
        if not start.isclose(path.end, rel_tol=rel_tol, abs_tol=abs_tol):
            path.line_to(start)

        if start.isclose(ctrl, rel_tol=rel_tol, abs_tol=abs_tol) or end.isclose(
            ctrl, rel_tol=rel_tol, abs_tol=abs_tol
        ):
            path.line_to(end)
        else:
            path.curve3_to(end, ctrl)


def add_2d_polyline(
    path: Path,
    points: Iterable[Sequence[float]],
    close: bool,
    ocs: OCS,
    elevation: float,
    segments: int = 1,
) -> None:
    """Internal API to add 2D polylines which may include bulges to an
    **empty** path.

    """

    def bulge_to(p1: Vec3, p2: Vec3, bulge: float, segments: int):
        if p1.isclose(p2, rel_tol=IS_CLOSE_TOL, abs_tol=0):
            return
        # each cubic_bezier adds 3 segments, need 1 minimum
        num_bez = math.ceil(segments / 3)
        center, start_angle, end_angle, radius = bulge_to_arc(p1, p2, bulge)
        # normalize angles into range 0 .. 2pi
        start_angle = start_angle % math.tau
        end_angle = end_angle % math.tau
        if start_angle > end_angle:
            end_angle += math.tau
        angles = list(np.linspace(start_angle, end_angle, num_bez + 1))
        curves = []
        for i in range(num_bez):
            ellipse = ConstructionEllipse.from_arc(
                center,
                radius,
                Z_AXIS,
                math.degrees(angles[i]),
                math.degrees(angles[i + 1]),
            )
            curves.extend(list(cubic_bezier_from_ellipse(ellipse)))
        curve0 = curves[0]
        cp0 = curve0.control_points[0]
        if cp0.isclose(p2, rel_tol=IS_CLOSE_TOL, abs_tol=0):
            curves = reverse_bezier_curves(curves)
        add_bezier4p(path, curves)

    if len(path):
        raise ValueError("Requires an empty path.")

    prev_point: Optional[Vec3] = None
    prev_bulge: float = 0
    for x, y, bulge in points:
        # Bulge values near 0 but != 0 cause crashes! #329
        if abs(bulge) < 1e-6:
            bulge = 0
        point = Vec3(x, y)
        if prev_point is None:
            path.start = point
            prev_point = point
            prev_bulge = bulge
            continue

        if prev_bulge:
            bulge_to(prev_point, point, prev_bulge, segments)
        else:
            path.line_to(point)
        prev_point = point
        prev_bulge = bulge

    if close and not path.start.isclose(path.end, rel_tol=IS_CLOSE_TOL, abs_tol=0):
        if prev_bulge:
            bulge_to(path.end, path.start, prev_bulge, segments)
        else:
            path.line_to(path.start)

    if ocs.transform or elevation:
        path.to_wcs(ocs, elevation)


def add_spline(path: Path, spline: BSpline, level=4, reset=True) -> None:
    """Add a B-spline as multiple cubic Bèzier-curves.

    Non-rational B-splines of 3rd degree gets a perfect conversion to
    cubic Bézier curves with a minimal count of curve segments, all other
    B-spline require much more curve segments for approximation.

    Auto-detect the connection point to the given `path`, if neither the start-
    nor the end point of the B-spline is close to the path end point, a line
    from the path end point to the start point of the B-spline will be added
    automatically. (see :meth:`add_bezier4p`).

    By default, the start of an **empty** path is set to the start point of
    the spline, setting argument `reset` to ``False`` prevents this
    behavior.

    Args:
        path: :class:`~ezdxf.path.Path` object
        spline: B-spline parameters as :class:`~ezdxf.math.BSpline` object
        level: subdivision level of approximation segments
        reset: set start point to start of spline if path is empty

    """
    if len(path) == 0 and reset:
        path.start = spline.point(0)
    curves: Iterable[Bezier4P]
    if spline.degree == 3 and not spline.is_rational and spline.is_clamped:
        curves = [Bezier4P(points) for points in spline.bezier_decomposition()]
    else:
        curves = spline.cubic_bezier_approximation(level=level)
    add_bezier4p(path, curves)


def have_close_control_vertices(
    a: Path, b: Path, *, rel_tol=1e-9, abs_tol=1e-12
) -> bool:
    """Returns ``True`` if the control vertices of given paths are close."""
    return all(
        cp_a.isclose(cp_b, rel_tol=rel_tol, abs_tol=abs_tol)
        for cp_a, cp_b in zip(a.control_vertices(), b.control_vertices())
    )


def lines_to_curve3(path: Path) -> Path:
    """Replaces all lines by quadratic Bézier curves.
    Returns a new :class:`Path` instance.
    """
    return _all_lines_to_curve(path, count=3)


def lines_to_curve4(path: Path) -> Path:
    """Replaces all lines by cubic Bézier curves.
    Returns a new :class:`Path` instance.
    """
    return _all_lines_to_curve(path, count=4)


def _all_lines_to_curve(path: Path, count: int = 4) -> Path:
    assert count == 4 or count == 3, f"invalid count: {count}"

    cmds = path.commands()
    size = len(cmds)
    if size == 0:  # empty path
        return Path()
    start = path.start
    line_to = Command.LINE_TO
    new_path = Path(path.start)
    for cmd in cmds:
        if cmd.type == line_to:
            if start.isclose(cmd.end):
                if size == 1:
                    # Path has only one LINE_TO command which should not be
                    # removed:
                    # 1. may represent a point
                    # 2. removing the last segment turns the path into
                    #    an empty path - unexpected behavior?
                    new_path.append_path_element(cmd)
                    return new_path
                # else remove line segment (start==end)
            else:
                vertices = linear_vertex_spacing(start, cmd.end, count)
                if count == 3:
                    new_path.curve3_to(vertices[2], ctrl=vertices[1])
                else:  # count == 4
                    new_path.curve4_to(
                        vertices[3],
                        ctrl1=vertices[1],
                        ctrl2=vertices[2],
                    )
        else:
            new_path.append_path_element(cmd)
        start = cmd.end
    return new_path


def _get_local_fillet_ucs(p0, p1, p2, radius) -> tuple[Vec3, float, UCS]:
    dir1 = (p0 - p1).normalize()
    dir2 = (p2 - p1).normalize()
    if dir1.isclose(dir2) or dir1.isclose(-dir2):
        raise ZeroDivisionError

    # arc start- and end points:
    angle = dir1.angle_between(dir2)
    tangent_length = inscribe_circle_tangent_length(dir1, dir2, radius)
    # starting point of the fillet arc
    arc_start_point = p1 + (dir1 * tangent_length)

    # create local coordinate system:
    # origin = center of the fillet arc
    # x-axis = arc_center -> arc_start_point
    local_z_axis = dir2.cross(dir1)
    # radius_vec points from arc_start_point to the center of the fillet arc
    radius_vec = local_z_axis.cross(-dir1).normalize(radius)
    arc_center = arc_start_point + radius_vec
    ucs = UCS(origin=arc_center, ux=-radius_vec, uz=local_z_axis)
    return arc_start_point, math.pi - angle, ucs


def fillet(points: Sequence[Vec3], radius: float) -> Path:
    """Returns a :class:`Path` with circular fillets of given `radius` between
    straight line segments.

    Args:
        points: coordinates of the line segments
        radius: fillet radius

    """
    if len(points) < 3:
        raise ValueError("at least 3 not coincident points required")
    if radius <= 0:
        raise ValueError(f"invalid radius: {radius}")
    lines = [(p0, p1) for p0, p1 in zip(points, points[1:])]
    p = Path(points[0])
    for (p0, p1), (p2, p3) in zip(lines, lines[1:]):
        try:
            start_point, angle, ucs = _get_local_fillet_ucs(p0, p1, p3, radius)
        except ZeroDivisionError:
            p.line_to(p1)
            continue

        # add path elements:
        p.line_to(start_point)
        for params in cubic_bezier_arc_parameters(0, angle):
            # scale arc parameters by radius:
            bez_points = tuple(ucs.points_to_wcs(v * radius for v in params))
            p.curve4_to(bez_points[-1], bez_points[1], bez_points[2])
    p.line_to(points[-1])
    return p


def _segment_count(angle: float, count: int) -> int:
    count = max(4, count)
    return max(int(angle / (math.tau / count)), 1)


def polygonal_fillet(points: Sequence[Vec3], radius: float, count: int = 32) -> Path:
    """
    Returns a :class:`Path` with polygonal fillets of given `radius` between
    straight line segments. The `count` argument defines the vertex count of the
    fillet for a full circle.

    Args:
        points: coordinates of the line segments
        radius: fillet radius
        count: polygon vertex count for a full circle, minimum is 4

    """
    if len(points) < 3:
        raise ValueError("at least 3 not coincident points required")
    if radius <= 0:
        raise ValueError(f"invalid radius: {radius}")
    lines = [(p0, p1) for p0, p1 in zip(points, points[1:])]
    p = Path(points[0])
    for (p0, p1), (p2, p3) in zip(lines, lines[1:]):
        try:
            _, angle, ucs = _get_local_fillet_ucs(p0, p1, p3, radius)
        except ZeroDivisionError:
            p.line_to(p1)
            continue
        segments = _segment_count(angle, count)
        delta = angle / segments
        # add path elements:
        for i in range(segments + 1):
            radius_vec = Vec3.from_angle(i * delta, radius)
            p.line_to(ucs.to_wcs(radius_vec))

    p.line_to(points[-1])
    return p


def chamfer(points: Sequence[Vec3], length: float) -> Path:
    """
    Returns a :class:`Path` with chamfers of given `length` between
    straight line segments.

    Args:
        points: coordinates of the line segments
        length: chamfer length

    """
    if len(points) < 3:
        raise ValueError("at least 3 not coincident points required")
    lines = [(p0, p1) for p0, p1 in zip(points, points[1:])]
    p = Path(points[0])
    for (p0, p1), (p2, p3) in zip(lines, lines[1:]):
        # p1 is p2 !
        try:
            dir1 = (p0 - p1).normalize()
            dir2 = (p3 - p2).normalize()
            if dir1.isclose(dir2) or dir1.isclose(-dir2):
                raise ZeroDivisionError
            angle = dir1.angle_between(dir2) / 2.0
            a = abs((length / 2.0) / math.sin(angle))
        except ZeroDivisionError:
            p.line_to(p1)
            continue
        p.line_to(p1 + (dir1 * a))
        p.line_to(p2 + (dir2 * a))
    p.line_to(points[-1])
    return p


def chamfer2(points: Sequence[Vec3], a: float, b: float) -> Path:
    """
    Returns a :class:`Path` with chamfers at the given distances `a` and `b`
    from the segment points between straight line segments.

    Args:
        points: coordinates of the line segments
        a: distance of the chamfer start point to the segment point
        b: distance of the chamfer end point to the segment point

    """
    if len(points) < 3:
        raise ValueError("at least 3 non-coincident points required")
    lines = [(p0, p1) for p0, p1 in zip(points, points[1:])]
    p = Path(points[0])
    for (p0, p1), (p2, p3) in zip(lines, lines[1:]):
        # p1 is p2 !
        try:
            dir1 = (p0 - p1).normalize()
            dir2 = (p3 - p2).normalize()
            if dir1.isclose(dir2) or dir1.isclose(-dir2):
                raise ZeroDivisionError
        except ZeroDivisionError:
            p.line_to(p1)
            continue
        p.line_to(p1 + (dir1 * a))
        p.line_to(p2 + (dir2 * b))
    p.line_to(points[-1])
    return p


def triangulate(
    paths: Iterable[Path], max_sagitta: float = 0.01, min_segments: int = 16
) -> Iterator[Sequence[Vec2]]:
    """Tessellate nested 2D paths into triangle-faces. For 3D paths the
    projection onto the xy-plane will be triangulated.

    Args:
        paths: iterable of nested Path instances
        max_sagitta: maximum distance from the center of the curve to the
            center of the line segment between two approximation points to determine if
            a segment should be subdivided.
        min_segments: minimum segment count per Bézier curve

    """
    for polygon in nesting.group_paths(single_paths(paths)):
        exterior = polygon[0].flattening(max_sagitta, min_segments)
        holes = [p.flattening(max_sagitta, min_segments) for p in polygon[1:]]
        yield from mapbox_earcut_2d(exterior, holes)


def is_rectangular(path: Path, aligned=True) -> bool:
    """Returns ``True`` if `path` is a rectangular quadrilateral (square or
    rectangle). If the argument `aligned` is ``True`` all sides of the
    quadrilateral have to be parallel to the x- and y-axis.
    """
    points = path.control_vertices()
    if len(points) < 4:
        return False
    if points[0].isclose(points[-1]):
        points.pop()
    if len(points) != 4:
        return False

    if aligned:
        first_side = points[1] - points[0]
        if not (abs(first_side.x) < 1e-12 or abs(first_side.y) < 1e-12):
            return False

    # horizontal sides
    v1 = points[0].distance(points[1])
    v2 = points[2].distance(points[3])
    if not math.isclose(v1, v2):
        return False
    # vertical sides
    v1 = points[1].distance(points[2])
    v2 = points[3].distance(points[0])
    if not math.isclose(v1, v2):
        return False
    # diagonals
    v1 = points[0].distance(points[2])
    v2 = points[1].distance(points[3])
    if not math.isclose(v1, v2):
        return False

    return True
