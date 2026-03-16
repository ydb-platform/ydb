# Copyright (c) 2020-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    List,
    Iterable,
    Iterator,
    Union,
    Optional,
    Callable,
    Type,
    TypeVar,
)
from typing_extensions import TypeAlias
from functools import singledispatch, partial
import logging

from ezdxf.math import (
    ABS_TOL,
    Vec2,
    Vec3,
    NULLVEC,
    Z_AXIS,
    OCS,
    Bezier3P,
    Bezier4P,
    ConstructionEllipse,
    BSpline,
    have_bezier_curves_g1_continuity,
    fit_points_to_cad_cv,
    UVec,
    Matrix44,
)
from ezdxf.lldxf import const
from ezdxf.entities import (
    LWPolyline,
    Polyline,
    Hatch,
    Line,
    Spline,
    Ellipse,
    Arc,
    Circle,
    Solid,
    Trace,
    Face3d,
    Viewport,
    Image,
    Helix,
    Wipeout,
    MPolygon,
    BoundaryPaths,
    AbstractBoundaryPath,
    PolylinePath,
    EdgePath,
    LineEdge,
    ArcEdge,
    EllipseEdge,
    SplineEdge,
)
from ezdxf.entities.polygon import DXFPolygon
from .path import Path
from .commands import Command
from . import tools
from .nesting import group_paths

__all__ = [
    "make_path",
    "to_lines",
    "to_polylines3d",
    "to_lwpolylines",
    "to_polylines2d",
    "to_hatches",
    "to_mpolygons",
    "to_bsplines_and_vertices",
    "to_splines_and_polylines",
    "from_hatch",
    "from_hatch_ocs",
    "from_hatch_boundary_path",
    "from_hatch_edge_path",
    "from_hatch_polyline_path",
    "from_vertices",
]

MAX_DISTANCE = 0.01
MIN_SEGMENTS = 4
G1_TOL = 1e-4
TPolygon = TypeVar("TPolygon", Hatch, MPolygon)
BoundaryFactory = Callable[[BoundaryPaths, Path, int], None]
logger = logging.getLogger("ezdxf")


@singledispatch
def make_path(entity, segments: int = 1, level: int = 4) -> Path:
    """Factory function to create a single :class:`Path` object from a DXF
    entity.

    Args:
        entity: DXF entity
        segments: minimal count of cubic Bézier-curves for elliptical arcs
        level: subdivide level for SPLINE approximation

    Raises:
        TypeError: for unsupported DXF types

    """
    # Complete documentation is path.rst, because Sphinx auto-function
    # renders for each overloaded function a signature, which is ugly
    # and wrong signatures for multiple overloaded function
    # e.g. 3 equal signatures for type Solid.
    raise TypeError(f"unsupported DXF type: {entity.dxftype()}")


@make_path.register(LWPolyline)
def _from_lwpolyline(lwpolyline: LWPolyline, **kwargs) -> Path:
    path = Path()
    tools.add_2d_polyline(
        path,
        lwpolyline.get_points("xyb"),
        close=lwpolyline.closed,
        ocs=lwpolyline.ocs(),
        elevation=lwpolyline.dxf.elevation,
        segments=kwargs.get("segments", 1),
    )
    return path


@make_path.register(Polyline)
def _from_polyline(polyline: Polyline, **kwargs) -> Path:
    if polyline.is_polygon_mesh or polyline.is_poly_face_mesh:
        raise TypeError("Unsupported DXF type PolyMesh or PolyFaceMesh")

    path = Path()
    if len(polyline.vertices) == 0:
        return path

    if polyline.is_3d_polyline:
        return from_vertices(polyline.points(), polyline.is_closed)

    points = [vertex.format("xyb") for vertex in polyline.vertices]
    ocs = polyline.ocs()
    if polyline.dxf.hasattr("elevation"):
        elevation = Vec3(polyline.dxf.elevation).z
    else:
        # the elevation attribute is mandatory, but if it's missing
        # take the elevation value of the first vertex.
        elevation = Vec3(polyline.vertices[0].dxf.location).z
    tools.add_2d_polyline(
        path,
        points,
        close=polyline.is_closed,
        ocs=ocs,
        elevation=elevation,
        segments=kwargs.get("segments", 1),
    )
    return path


@make_path.register(Helix)
@make_path.register(Spline)
def _from_spline(spline: Spline, **kwargs) -> Path:
    level = kwargs.get("level", 4)
    path = Path()
    tools.add_spline(path, spline.construction_tool(), level=level, reset=True)
    return path


@make_path.register(Ellipse)
def _from_ellipse(ellipse: Ellipse, **kwargs) -> Path:
    segments = kwargs.get("segments", 1)
    path = Path()
    tools.add_ellipse(path, ellipse.construction_tool(), segments=segments, reset=True)
    return path


@make_path.register(Line)
def _from_line(line: Line, **kwargs) -> Path:
    path = Path(line.dxf.start)
    path.line_to(line.dxf.end)
    return path


@make_path.register(Arc)
def _from_arc(arc: Arc, **kwargs) -> Path:
    segments = kwargs.get("segments", 1)
    path = Path()
    radius = abs(arc.dxf.radius)
    if radius > 1e-12:
        ellipse = ConstructionEllipse.from_arc(
            center=arc.dxf.center,
            radius=radius,
            extrusion=arc.dxf.extrusion,
            start_angle=arc.dxf.start_angle,
            end_angle=arc.dxf.end_angle,
        )
        tools.add_ellipse(path, ellipse, segments=segments, reset=True)
    return path


@make_path.register(Circle)
def _from_circle(circle: Circle, **kwargs) -> Path:
    segments = kwargs.get("segments", 1)
    path = Path()
    radius = abs(circle.dxf.radius)
    if radius > 1e-12:
        ellipse = ConstructionEllipse.from_arc(
            center=circle.dxf.center,
            radius=radius,
            extrusion=circle.dxf.extrusion,
        )
        tools.add_ellipse(path, ellipse, segments=segments, reset=True)
    return path


@make_path.register(Face3d)
@make_path.register(Trace)
@make_path.register(Solid)
def _from_quadrilateral(solid: "Solid", **kwargs) -> Path:
    vertices = solid.wcs_vertices()
    return from_vertices(vertices, close=True)


@make_path.register(Viewport)
def _from_viewport(vp: "Viewport", **kwargs) -> Path:
    if vp.has_extended_clipping_path:
        handle = vp.dxf.clipping_boundary_handle
        if handle != "0" and vp.doc:  # exist
            db = vp.doc.entitydb
            if db:  # exist
                # Many DXF entities can define a clipping path:
                clipping_entity = vp.doc.entitydb.get(handle)
                if clipping_entity:  # exist
                    return make_path(clipping_entity, **kwargs)
    # Return bounding box:
    return from_vertices(vp.clipping_rect_corners(), close=True)


@make_path.register(Wipeout)
@make_path.register(Image)
def _from_image(image: "Image", **kwargs) -> Path:
    return from_vertices(image.boundary_path_wcs(), close=True)


@make_path.register(MPolygon)
@make_path.register(Hatch)
def _from_hatch(hatch: Hatch, **kwargs) -> Path:
    ocs = hatch.ocs()
    elevation = hatch.dxf.elevation.z
    offset = NULLVEC
    if isinstance(hatch, MPolygon):
        offset = hatch.dxf.get("offset_vector", NULLVEC)
    try:
        paths = [
            from_hatch_boundary_path(boundary, ocs, elevation, offset=offset)
            for boundary in hatch.paths
        ]
    except const.DXFStructureError:  
        # TODO: fix problems beforehand in audit process? see issue #1081
        logger.warning(f"invalid data in {str(hatch)}")
        return Path()
    # looses the boundary path state:
    return tools.to_multi_path(paths)


def from_hatch(hatch: DXFPolygon, offset: Vec3 = NULLVEC) -> Iterator[Path]:
    """Yield all HATCH/MPOLYGON boundary paths as separated :class:`Path` objects in WCS
    coordinates.
    """
    ocs = hatch.ocs()
    elevation = hatch.dxf.elevation.z
    for boundary in hatch.paths:
        p = from_hatch_boundary_path(boundary, ocs, elevation=elevation, offset=offset)
        if p.has_sub_paths:
            yield from p.sub_paths()
        else:
            yield p


def from_hatch_ocs(hatch: DXFPolygon, offset: Vec3 = NULLVEC) -> Iterator[Path]:
    """Yield all HATCH/MPOLYGON boundary paths as separated :class:`Path` objects in OCS
    coordinates. Elevation and offset is applied to all vertices.

    .. versionadded:: 1.1

    """
    elevation = hatch.dxf.elevation.z
    for boundary in hatch.paths:
        p = from_hatch_boundary_path(boundary, elevation=elevation, offset=offset)
        if p.has_sub_paths:
            yield from p.sub_paths()
        else:
            yield p


def from_hatch_boundary_path(
    boundary: AbstractBoundaryPath,
    ocs: Optional[OCS] = None,
    elevation: float = 0,
    offset: Vec3 = NULLVEC,  # ocs offset!
) -> Path:
    """Returns a :class:`Path` object from a :class:`~ezdxf.entities.Hatch`
    polyline- or edge path.
    """
    if isinstance(boundary, EdgePath):
        p = from_hatch_edge_path(boundary, ocs, elevation)
    elif isinstance(boundary, PolylinePath):
        p = from_hatch_polyline_path(boundary, ocs, elevation)
    else:
        raise TypeError(type(boundary))

    if offset and ocs is not None:  # only for MPOLYGON
        # assume offset is in OCS
        offset = ocs.to_wcs(offset.replace(z=elevation))
        p = p.transform(Matrix44.translate(offset.x, offset.y, offset.z))

    # attach path type information
    p.user_data = const.BoundaryPathState.from_flags(boundary.path_type_flags)
    return p


def from_hatch_polyline_path(
    polyline: PolylinePath, ocs: Optional[OCS] = None, elevation: float = 0
) -> Path:
    """Returns a :class:`Path` object from a :class:`~ezdxf.entities.Hatch`
    polyline path.
    """
    path = Path()
    tools.add_2d_polyline(
        path,
        polyline.vertices,  # list[(x, y, bulge)]
        close=polyline.is_closed,
        ocs=ocs or OCS(),
        elevation=elevation,
    )
    return path


def from_hatch_edge_path(
    edges: EdgePath,
    ocs: Optional[OCS] = None,
    elevation: float = 0,
) -> Path:
    """Returns a :class:`Path` object from a :class:`~ezdxf.entities.Hatch`
    edge path.

    """

    def line(edge: LineEdge):
        start = wcs(edge.start)
        end = wcs(edge.end)
        segment = Path(start)
        segment.line_to(end)
        return segment

    def arc(edge: ArcEdge):
        x, y, *_ = edge.center
        # from_arc() requires OCS data:
        # Note: clockwise oriented arcs are converted to counter
        # clockwise arcs at the loading stage!
        # See: ezdxf.entities.boundary_paths.ArcEdge.load_tags()
        ellipse = ConstructionEllipse.from_arc(
            center=(x, y, elevation),
            radius=edge.radius,
            extrusion=extrusion,
            start_angle=edge.start_angle,
            end_angle=edge.end_angle,
        )
        segment = Path()
        tools.add_ellipse(segment, ellipse, reset=True)
        return segment

    def ellipse(edge: EllipseEdge):
        ocs_ellipse = edge.construction_tool()
        # ConstructionEllipse has WCS representation:
        # Note: clockwise oriented ellipses are converted to counter
        # clockwise ellipses at the loading stage!
        # See: ezdxf.entities.boundary_paths.EllipseEdge.load_tags()
        ellipse = ConstructionEllipse(
            center=wcs(ocs_ellipse.center.replace(z=float(elevation))),
            major_axis=wcs_tangent(ocs_ellipse.major_axis),
            ratio=ocs_ellipse.ratio,
            extrusion=extrusion,
            start_param=ocs_ellipse.start_param,
            end_param=ocs_ellipse.end_param,
        )
        segment = Path()
        tools.add_ellipse(segment, ellipse, reset=True)
        return segment

    def spline(edge: SplineEdge):
        control_points = [wcs(p) for p in edge.control_points]
        if len(control_points) == 0:
            fit_points = [wcs(p) for p in edge.fit_points]
            if len(fit_points):
                bspline = from_fit_points(edge, fit_points)
            else:
                # No control points and no fit points:
                # DXF structure error
                return
        else:
            bspline = from_control_points(edge, control_points)
        segment = Path()
        tools.add_spline(segment, bspline, reset=True)
        return segment

    def from_fit_points(edge: SplineEdge, fit_points):
        tangents = None
        if edge.start_tangent and edge.end_tangent:
            tangents = (
                wcs_tangent(edge.start_tangent),
                wcs_tangent(edge.end_tangent),
            )
        return fit_points_to_cad_cv(  # only a degree of 3 is supported
            fit_points,
            tangents=tangents,
        )

    def from_control_points(edge: SplineEdge, control_points):
        return BSpline(
            control_points=control_points,
            order=edge.degree + 1,
            knots=edge.knot_values,
            weights=edge.weights if edge.weights else None,
        )

    def wcs(vertex: UVec) -> Vec3:
        return _wcs(Vec3(vertex[0], vertex[1], elevation))

    def wcs_tangent(vertex: UVec) -> Vec3:
        return _wcs(Vec3(vertex[0], vertex[1], 0))

    def _wcs(vec3: Vec3) -> Vec3:
        if ocs and ocs.transform:
            return ocs.to_wcs(vec3)
        else:
            return vec3

    extrusion = ocs.uz if ocs else Z_AXIS
    path = Path()
    loop: Optional[Path] = None
    for edge in edges:
        next_segment: Optional[Path] = None
        if isinstance(edge, LineEdge):
            next_segment = line(edge)
        elif isinstance(edge, ArcEdge):
            if abs(edge.radius) > ABS_TOL:
                next_segment = arc(edge)
        elif isinstance(edge, EllipseEdge):
            if not Vec2(edge.major_axis).is_null:
                next_segment = ellipse(edge)
        elif isinstance(edge, SplineEdge):
            next_segment = spline(edge)
        else:
            raise TypeError(type(edge))

        if next_segment is None:
            continue

        if loop is None:
            loop = next_segment
            continue

        if loop.end.isclose(next_segment.start):
            # end of current loop connects to the start of the next segment
            loop.append_path(next_segment)
        elif loop.end.isclose(next_segment.end):
            # end of current loop connects to the end of the next segment
            loop.append_path(next_segment.reversed())
        elif loop.start.isclose(next_segment.end):
            # start of the current loop connects to the end of the next segment
            next_segment.append_path(loop)
            loop = next_segment
        elif loop.start.isclose(next_segment.start):
            # start of the current loop connects to the start of the next segment
            loop = loop.reversed()
            loop.append_path(next_segment)
        else:  # gap between current loop and next segment
            if loop.is_closed:  # start a new loop
                path.extend_multi_path(loop)
                loop = next_segment  # start a new loop
            # behavior changed in version v0.18 based on issue #706:
            else:  # close the gap by a straight line and append the segment
                loop.append_path(next_segment)

    if loop is not None:
        loop.close()
        path.extend_multi_path(loop)
    return path  # multi path


def from_vertices(vertices: Iterable[UVec], close=False) -> Path:
    """Returns a :class:`Path` object from the given `vertices`."""
    _vertices = Vec3.list(vertices)
    if len(_vertices) < 2:
        return Path()
    path = Path(start=_vertices[0])
    for vertex in _vertices[1:]:
        if not path.end.isclose(vertex):
            path.line_to(vertex)
    if close:
        path.close()
    return path


def to_lwpolylines(
    paths: Iterable[Path],
    *,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    extrusion: UVec = Z_AXIS,
    dxfattribs=None,
) -> Iterator[LWPolyline]:
    """Convert the given `paths` into :class:`~ezdxf.entities.LWPolyline`
    entities.
    The `extrusion` vector is applied to all paths, all vertices are projected
    onto the plane normal to this extrusion vector. The default extrusion vector
    is the WCS z-axis. The plane elevation is the distance from the WCS origin
    to the start point of the first path.

    Args:
        paths: iterable of :class:`Path` objects
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve
        extrusion: extrusion vector for all paths
        dxfattribs: additional DXF attribs

    Returns:
        iterable of :class:`~ezdxf.entities.LWPolyline` objects

    """
    if isinstance(paths, Path):
        paths = [paths]
    else:
        paths = list(paths)
    if len(paths) == 0:
        return
    extrusion = Vec3(extrusion)
    reference_point = Vec3(paths[0].start)
    dxfattribs = dict(dxfattribs or {})
    if not Z_AXIS.isclose(extrusion):
        ocs, elevation = _get_ocs(extrusion, reference_point)
        paths = tools.transform_paths_to_ocs(paths, ocs)
        dxfattribs["elevation"] = elevation
        dxfattribs["extrusion"] = extrusion
    elif reference_point.z != 0:
        dxfattribs["elevation"] = reference_point.z

    for path in tools.single_paths(paths):
        if len(path) > 0:
            p = LWPolyline.new(dxfattribs=dxfattribs)
            p.append_points(path.flattening(distance, segments), format="xy")
            yield p


def _get_ocs(extrusion: Vec3, reference_point: Vec3) -> tuple[OCS, float]:
    ocs = OCS(extrusion)
    elevation = ocs.from_wcs(reference_point).z
    return ocs, elevation


def to_polylines2d(
    paths: Iterable[Path],
    *,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    extrusion: UVec = Z_AXIS,
    dxfattribs=None,
) -> Iterator[Polyline]:
    """Convert the given `paths` into 2D :class:`~ezdxf.entities.Polyline`
    entities.
    The `extrusion` vector is applied to all paths, all vertices are projected
    onto the plane normal to this extrusion vector. The default extrusion vector
    is the WCS z-axis. The plane elevation is the distance from the WCS origin
    to the start point of the first path.

    Args:
        paths: iterable of :class:`Path` objects
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve
        extrusion: extrusion vector for all paths
        dxfattribs: additional DXF attribs

    Returns:
        iterable of 2D :class:`~ezdxf.entities.Polyline` objects

    """
    if isinstance(paths, Path):
        paths = [paths]
    else:
        paths = list(paths)
    if len(paths) == 0:
        return
    extrusion = Vec3(extrusion)
    reference_point = Vec3(paths[0].start)
    dxfattribs = dict(dxfattribs or {})
    if not Z_AXIS.isclose(extrusion):
        ocs, elevation = _get_ocs(extrusion, reference_point)
        paths = tools.transform_paths_to_ocs(paths, ocs)
        dxfattribs["elevation"] = Vec3(0, 0, elevation)
        dxfattribs["extrusion"] = extrusion
    elif reference_point.z != 0:
        dxfattribs["elevation"] = Vec3(0, 0, reference_point.z)

    for path in tools.single_paths(paths):
        if len(path) > 0:
            p = Polyline.new(dxfattribs=dxfattribs)
            p.append_vertices(path.flattening(distance, segments))
            p.new_seqend()
            yield p


def to_hatches(
    paths: Iterable[Path],
    *,
    edge_path: bool = True,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    g1_tol: float = G1_TOL,
    extrusion: UVec = Z_AXIS,
    dxfattribs=None,
) -> Iterator[Hatch]:
    """Convert the given `paths` into :class:`~ezdxf.entities.Hatch` entities.
    Uses LWPOLYLINE paths for boundaries without curves and edge paths, build
    of LINE and SPLINE edges, as boundary paths for boundaries including curves.
    The `extrusion` vector is applied to all paths, all vertices are projected
    onto the plane normal to this extrusion vector. The default extrusion vector
    is the WCS z-axis. The plane elevation is the distance from the WCS origin
    to the start point of the first path.

    Args:
        paths: iterable of :class:`Path` objects
        edge_path: ``True`` for edge paths build of LINE and SPLINE edges,
            ``False`` for only LWPOLYLINE paths as boundary paths
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve to flatten LWPOLYLINE paths
        g1_tol: tolerance for G1 continuity check to separate SPLINE edges
        extrusion: extrusion vector to all paths
        dxfattribs: additional DXF attribs

    Returns:
        iterable of :class:`~ezdxf.entities.Hatch` objects

    """
    boundary_factory: BoundaryFactory
    if edge_path:
        # noinspection PyTypeChecker
        boundary_factory = partial(
            build_edge_path, distance=distance, segments=segments, g1_tol=g1_tol
        )
    else:
        # noinspection PyTypeChecker
        boundary_factory = partial(
            build_poly_path, distance=distance, segments=segments
        )

    yield from _polygon_converter(Hatch, paths, boundary_factory, extrusion, dxfattribs)


def to_mpolygons(
    paths: Iterable[Path],
    *,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    extrusion: UVec = Z_AXIS,
    dxfattribs=None,
) -> Iterator[MPolygon]:
    """Convert the given `paths` into :class:`~ezdxf.entities.MPolygon` entities.
    In contrast to HATCH, MPOLYGON supports only polyline boundary paths.
    All curves will be approximated.

    The `extrusion` vector is applied to all paths, all vertices are projected
    onto the plane normal to this extrusion vector. The default extrusion vector
    is the WCS z-axis. The plane elevation is the distance from the WCS origin
    to the start point of the first path.

    Args:
        paths: iterable of :class:`Path` objects
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve to flatten LWPOLYLINE paths
        extrusion: extrusion vector to all paths
        dxfattribs: additional DXF attribs

    Returns:
        iterable of :class:`~ezdxf.entities.MPolygon` objects

    """
    # noinspection PyTypeChecker
    boundary_factory: BoundaryFactory = partial(
        build_poly_path, distance=distance, segments=segments
    )
    dxfattribs = dict(dxfattribs or {})
    dxfattribs.setdefault("fill_color", const.BYLAYER)

    yield from _polygon_converter(
        MPolygon, paths, boundary_factory, extrusion, dxfattribs
    )


def build_edge_path(
    boundaries: BoundaryPaths,
    path: Path,
    flags: int,
    distance: float,
    segments: int,
    g1_tol: float,
):
    if path.has_curves:  # Edge path with LINE and SPLINE edges
        edge_path = boundaries.add_edge_path(flags)
        for edge in to_bsplines_and_vertices(path, g1_tol=g1_tol):
            if isinstance(edge, BSpline):
                edge_path.add_spline(
                    control_points=edge.control_points,
                    degree=edge.degree,
                    knot_values=edge.knots(),
                )
            else:  # add LINE edges
                prev = edge[0]
                for p in edge[1:]:
                    edge_path.add_line(prev, p)
                    prev = p
    else:  # Polyline boundary path
        boundaries.add_polyline_path(
            Vec2.generate(path.flattening(distance, segments)), flags=flags
        )


def build_poly_path(
    boundaries: BoundaryPaths,
    path: Path,
    flags: int,
    distance: float,
    segments: int,
):
    boundaries.add_polyline_path(
        # Vec2 removes the z-axis, which would be interpreted as bulge value!
        Vec2.generate(path.flattening(distance, segments)),
        flags=flags,
    )


def _polygon_converter(
    cls: Type[TPolygon],
    paths: Iterable[Path],
    add_boundary: BoundaryFactory,
    extrusion: UVec = Z_AXIS,
    dxfattribs=None,
) -> Iterator[TPolygon]:
    if isinstance(paths, Path):
        paths = [paths]
    else:
        paths = list(paths)
    if len(paths) == 0:
        return

    extrusion = Vec3(extrusion)
    reference_point = paths[0].start
    _dxfattribs: dict = dict(dxfattribs or {})
    if not Z_AXIS.isclose(extrusion):
        ocs, elevation = _get_ocs(extrusion, reference_point)
        paths = tools.transform_paths_to_ocs(paths, ocs)
        _dxfattribs["elevation"] = Vec3(0, 0, elevation)
        _dxfattribs["extrusion"] = extrusion
    elif reference_point.z != 0:
        _dxfattribs["elevation"] = Vec3(0, 0, reference_point.z)
    _dxfattribs.setdefault("solid_fill", 1)
    _dxfattribs.setdefault("pattern_name", "SOLID")
    _dxfattribs.setdefault("color", const.BYLAYER)

    for group in group_paths(tools.single_paths(paths)):
        if len(group) == 0:
            continue
        polygon = cls.new(dxfattribs=_dxfattribs)
        boundaries = polygon.paths
        external = group[0]
        external.close()
        add_boundary(boundaries, external, 1)
        for hole in group[1:]:
            hole.close()
            add_boundary(boundaries, hole, 0)
        yield polygon


def to_polylines3d(
    paths: Iterable[Path],
    *,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    dxfattribs=None,
) -> Iterator[Polyline]:
    """Convert the given `paths` into 3D :class:`~ezdxf.entities.Polyline`
    entities.

    Args:
        paths: iterable of :class:`Path` objects
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve
        dxfattribs: additional DXF attribs

    Returns:
        iterable of 3D :class:`~ezdxf.entities.Polyline` objects

    """
    if isinstance(paths, Path):
        paths = [paths]

    dxfattribs = dict(dxfattribs or {})
    dxfattribs["flags"] = const.POLYLINE_3D_POLYLINE
    for path in tools.single_paths(paths):
        if len(path) > 0:
            p = Polyline.new(dxfattribs=dxfattribs)
            p.append_vertices(path.flattening(distance, segments))
            p.new_seqend()
            yield p


def to_lines(
    paths: Iterable[Path],
    *,
    distance: float = MAX_DISTANCE,
    segments: int = MIN_SEGMENTS,
    dxfattribs=None,
) -> Iterator[Line]:
    """Convert the given `paths` into :class:`~ezdxf.entities.Line` entities.

    Args:
        paths: iterable of :class:`Path` objects
        distance:  maximum distance, see :meth:`Path.flattening`
        segments: minimum segment count per Bézier curve
        dxfattribs: additional DXF attribs

    Returns:
        iterable of :class:`~ezdxf.entities.Line` objects

    """
    if isinstance(paths, Path):
        paths = [paths]
    dxfattribs = dict(dxfattribs or {})
    prev_vertex = None
    for path in tools.single_paths(paths):
        if len(path) == 0:
            continue
        for vertex in path.flattening(distance, segments):
            if prev_vertex is None:
                prev_vertex = vertex
                continue
            dxfattribs["start"] = prev_vertex
            dxfattribs["end"] = vertex
            yield Line.new(dxfattribs=dxfattribs)
            prev_vertex = vertex
        prev_vertex = None


PathParts: TypeAlias = Union[BSpline, List[Vec3]]


def to_bsplines_and_vertices(path: Path, g1_tol: float = G1_TOL) -> Iterator[PathParts]:
    """Convert a :class:`Path` object into multiple cubic B-splines and
    polylines as lists of vertices. Breaks adjacent Bèzier without G1
    continuity into separated B-splines.

    Args:
        path: :class:`Path` objects
        g1_tol: tolerance for G1 continuity check

    Returns:
        :class:`~ezdxf.math.BSpline` and lists of :class:`~ezdxf.math.Vec3`

    """
    from ezdxf.math import bezier_to_bspline

    def to_vertices():
        points = [polyline[0][0]]
        for line in polyline:
            points.append(line[1])
        return points

    def to_bspline():
        b1 = bezier[0]
        _g1_continuity_curves = [b1]
        for b2 in bezier[1:]:
            if have_bezier_curves_g1_continuity(b1, b2, g1_tol):
                _g1_continuity_curves.append(b2)
            else:
                yield bezier_to_bspline(_g1_continuity_curves)
                _g1_continuity_curves = [b2]
            b1 = b2

        if _g1_continuity_curves:
            yield bezier_to_bspline(_g1_continuity_curves)

    curves = []
    for path in tools.single_paths([path]):
        prev = path.start
        for cmd in path:
            if cmd.type == Command.CURVE3_TO:
                curve = Bezier3P([prev, cmd.ctrl, cmd.end])  # type: ignore
            elif cmd.type == Command.CURVE4_TO:
                curve = Bezier4P([prev, cmd.ctrl1, cmd.ctrl2, cmd.end])  # type: ignore
            elif cmd.type == Command.LINE_TO:
                curve = (prev, cmd.end)
            else:
                raise ValueError
            curves.append(curve)
            prev = cmd.end

    bezier: list = []
    polyline: list = []
    for curve in curves:
        if isinstance(curve, tuple):
            if bezier:
                yield from to_bspline()
                bezier.clear()
            polyline.append(curve)
        else:
            if polyline:
                yield to_vertices()
                polyline.clear()
            bezier.append(curve)

    if bezier:
        yield from to_bspline()
    if polyline:
        yield to_vertices()


def to_splines_and_polylines(
    paths: Iterable[Path],
    *,
    g1_tol: float = G1_TOL,
    dxfattribs=None,
) -> Iterator[Union[Spline, Polyline]]:
    """Convert the given `paths` into :class:`~ezdxf.entities.Spline` and 3D
    :class:`~ezdxf.entities.Polyline` entities.

    Args:
        paths: iterable of :class:`Path` objects
        g1_tol: tolerance for G1 continuity check
        dxfattribs: additional DXF attribs

    Returns:
        iterable of :class:`~ezdxf.entities.Line` objects

    """
    if isinstance(paths, Path):
        paths = [paths]
    dxfattribs = dict(dxfattribs or {})

    for path in tools.single_paths(paths):
        for data in to_bsplines_and_vertices(path, g1_tol):
            if isinstance(data, BSpline):
                spline = Spline.new(dxfattribs=dxfattribs)
                spline.apply_construction_tool(data)
                yield spline
            else:
                attribs = dict(dxfattribs)
                attribs["flags"] = const.POLYLINE_3D_POLYLINE
                polyline = Polyline.new(dxfattribs=dxfattribs)
                polyline.append_vertices(data)
                polyline.new_seqend()
                yield polyline
