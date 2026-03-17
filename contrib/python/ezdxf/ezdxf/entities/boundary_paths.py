#  Copyright (c) 2021-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Union, Iterable, Sequence, Optional, TYPE_CHECKING
import abc
import enum
import math

from ezdxf.lldxf import const
from ezdxf.lldxf.tags import Tags, group_tags
from ezdxf.math import (
    Vec2,
    Vec3,
    UVec,
    OCS,
    bulge_to_arc,
    ConstructionEllipse,
    ConstructionArc,
    BSpline,
    NonUniformScalingError,
    open_uniform_knot_vector,
    global_bspline_interpolation,
    arc_angle_span_deg,
    angle_to_param,
    param_to_angle,
)
from ezdxf.math.transformtools import OCSTransform

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = [
    "BoundaryPaths",
    "PolylinePath",
    "EdgePath",
    "LineEdge",
    "ArcEdge",
    "EllipseEdge",
    "SplineEdge",
    "EdgeType",
    "BoundaryPathType",
    "AbstractEdge",
    "AbstractBoundaryPath",
]


@enum.unique
class BoundaryPathType(enum.IntEnum):
    POLYLINE = 1
    EDGE = 2


@enum.unique
class EdgeType(enum.IntEnum):
    LINE = 1
    ARC = 2
    ELLIPSE = 3
    SPLINE = 4


class AbstractBoundaryPath(abc.ABC):
    type: BoundaryPathType
    path_type_flags: int
    source_boundary_objects: list[str]

    @abc.abstractmethod
    def clear(self) -> None: ...

    @classmethod
    @abc.abstractmethod
    def load_tags(cls, tags: Tags) -> AbstractBoundaryPath: ...

    @abc.abstractmethod
    def export_dxf(self, tagwriter: AbstractTagWriter, dxftype: str) -> None: ...

    @abc.abstractmethod
    def transform(self, ocs: OCSTransform, elevation: float) -> None: ...

    @abc.abstractmethod
    def is_valid(self) -> bool: ...


class AbstractEdge(abc.ABC):
    type: EdgeType

    @property
    @abc.abstractmethod
    def start_point(self) -> Vec2: ...

    @property
    @abc.abstractmethod
    def end_point(self) -> Vec2: ...

    @abc.abstractmethod
    def export_dxf(self, tagwriter: AbstractTagWriter) -> None: ...

    @abc.abstractmethod
    def transform(self, ocs: OCSTransform, elevation: float) -> None: ...

    @abc.abstractmethod
    def is_valid(self) -> bool: ...

    @property
    def real_start_point(self) -> Vec2:
        return self.start_point

    @property
    def real_end_point(self) -> Vec2:
        return self.end_point


class BoundaryPaths:
    def __init__(self, paths: Optional[list[AbstractBoundaryPath]] = None):
        self.paths: list[AbstractBoundaryPath] = paths or []

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, item):
        return self.paths[item]

    def __iter__(self):
        return iter(self.paths)

    def is_valid(self) -> bool:
        return all(p.is_valid() for p in self.paths)

    @classmethod
    def load_tags(cls, tags: Tags) -> BoundaryPaths:
        paths = []
        assert tags[0].code == 92
        grouped_path_tags = group_tags(tags, splitcode=92)
        for path_tags in grouped_path_tags:
            path_type_flags = path_tags[0].value
            is_polyline_path = bool(path_type_flags & 2)
            path: AbstractBoundaryPath = (
                PolylinePath.load_tags(path_tags)
                if is_polyline_path
                else EdgePath.load_tags(path_tags)
            )
            path.path_type_flags = path_type_flags
            paths.append(path)
        return cls(paths)

    @property
    def has_edge_paths(self) -> bool:
        return any(p.type == BoundaryPathType.EDGE for p in self.paths)

    def clear(self) -> None:
        """Remove all boundary paths."""
        self.paths = []

    def external_paths(self) -> Iterable[AbstractBoundaryPath]:
        """Iterable of external paths, could be empty."""
        for b in self.paths:
            if b.path_type_flags & const.BOUNDARY_PATH_EXTERNAL:
                yield b

    def outermost_paths(self) -> Iterable[AbstractBoundaryPath]:
        """Iterable of outermost paths, could be empty."""
        for b in self.paths:
            if b.path_type_flags & const.BOUNDARY_PATH_OUTERMOST:
                yield b

    def default_paths(self) -> Iterable[AbstractBoundaryPath]:
        """Iterable of default paths, could be empty."""
        not_default = const.BOUNDARY_PATH_OUTERMOST + const.BOUNDARY_PATH_EXTERNAL
        for b in self.paths:
            if bool(b.path_type_flags & not_default) is False:
                yield b

    def rendering_paths(
        self, hatch_style: int = const.HATCH_STYLE_NESTED
    ) -> Iterable[AbstractBoundaryPath]:
        """Iterable of paths to process for rendering, filters unused
        boundary paths according to the given hatch style:

        - NESTED: use all boundary paths
        - OUTERMOST: use EXTERNAL and OUTERMOST boundary paths
        - IGNORE: ignore all paths except EXTERNAL boundary paths

        Yields paths in order of EXTERNAL, OUTERMOST and DEFAULT.

        """

        def path_type_enum(flags) -> int:
            if flags & const.BOUNDARY_PATH_EXTERNAL:
                return 0
            elif flags & const.BOUNDARY_PATH_OUTERMOST:
                return 1
            return 2

        paths = sorted(
            (path_type_enum(p.path_type_flags), i, p) for i, p in enumerate(self.paths)
        )
        ignore = 1  # EXTERNAL only
        if hatch_style == const.HATCH_STYLE_NESTED:
            ignore = 3
        elif hatch_style == const.HATCH_STYLE_OUTERMOST:
            ignore = 2
        return (p for path_type, _, p in paths if path_type < ignore)

    def append(self, path: AbstractBoundaryPath) -> None:
        """Append a new boundary path.

        .. versionadded:: 1.4
        """
        if not isinstance(path, AbstractBoundaryPath):
            raise TypeError(f"invalid path type: {type(path)}")
        self.paths.append(path)

    def add_polyline_path(
        self,
        path_vertices: Iterable[tuple[float, ...]],
        is_closed: bool = True,
        flags: int = 1,
    ) -> PolylinePath:
        """Create and add a new :class:`PolylinePath` object.

        Args:
            path_vertices: iterable of polyline vertices as (x, y) or
                (x, y, bulge)-tuples.
            is_closed: 1 for a closed polyline else 0
            flags: default(0), external(1), derived(4), textbox(8) or outermost(16)

        """
        new_path = PolylinePath.from_vertices(path_vertices, is_closed, flags)
        self.append(new_path)
        return new_path

    def add_edge_path(self, flags: int = 1) -> EdgePath:
        """Create and add a new :class:`EdgePath` object.

        Args:
            flags: default(0), external(1), derived(4), textbox(8) or outermost(16)

        """
        new_path = EdgePath()
        new_path.path_type_flags = flags
        self.append(new_path)
        return new_path

    def export_dxf(self, tagwriter: AbstractTagWriter, dxftype: str) -> None:
        tagwriter.write_tag2(91, len(self.paths))
        for path in self.paths:
            path.export_dxf(tagwriter, dxftype)

    def transform(self, ocs: OCSTransform, elevation: float = 0) -> None:
        """Transform HATCH boundary paths.

        These paths are 2d elements, placed in the OCS of the HATCH.

        """
        if not ocs.scale_uniform:
            self.polyline_to_edge_paths(just_with_bulge=True)
            self.arc_edges_to_ellipse_edges()

        for path in self.paths:
            path.transform(ocs, elevation=elevation)

    def polyline_to_edge_paths(self, just_with_bulge=True) -> None:
        """Convert polyline paths including bulge values to line- and arc edges.

        Args:
            just_with_bulge: convert only polyline paths including bulge
                values if ``True``

        """

        def _edges(points) -> Iterable[Union[LineEdge, ArcEdge]]:
            prev_point: Vec3 | None = None
            prev_bulge: float = 0.0
            for x, y, bulge in points:
                point = Vec3(x, y)
                if prev_point is None:
                    prev_point = point
                    prev_bulge = bulge
                    continue

                if prev_bulge != 0.0:
                    arc = ArcEdge()
                    # bulge_to_arc returns always counter-clockwise oriented
                    # start- and end angles:
                    (
                        arc.center,
                        start_angle,
                        end_angle,
                        arc.radius,
                    ) = bulge_to_arc(prev_point, point, prev_bulge)
                    chk_point = arc.center + Vec2.from_angle(start_angle, arc.radius)
                    arc.ccw = chk_point.isclose(prev_point, abs_tol=1e-9)
                    arc.start_angle = math.degrees(start_angle) % 360.0
                    arc.end_angle = math.degrees(end_angle) % 360.0
                    if math.isclose(arc.start_angle, arc.end_angle) and math.isclose(
                        arc.start_angle, 0
                    ):
                        arc.end_angle = 360.0
                    yield arc
                else:
                    line = LineEdge()
                    line.start = (prev_point.x, prev_point.y)
                    line.end = (point.x, point.y)
                    yield line

                prev_point = point
                prev_bulge = bulge

        def to_edge_path(polyline_path) -> EdgePath:
            edge_path = EdgePath()
            vertices: list = list(polyline_path.vertices)
            if polyline_path.is_closed:
                vertices.append(vertices[0])
            edge_path.edges = list(_edges(vertices))
            return edge_path

        for path_index, path in enumerate(self.paths):
            if isinstance(path, PolylinePath):
                if just_with_bulge and not path.has_bulge():
                    continue
                self.paths[path_index] = to_edge_path(path)

    def edge_to_polyline_paths(self, distance: float, segments: int = 16):
        """Convert all edge paths to simple polyline paths without bulges.

        Args:
            distance: maximum distance from the center of the curve to the
                center of the line segment between two approximation points to
                determine if a segment should be subdivided.
            segments: minimum segment count per curve

        """
        converted = []
        for path in self.paths:
            if path.type == BoundaryPathType.EDGE:
                path = flatten_to_polyline_path(path, distance, segments)
            converted.append(path)
        self.paths = converted

    def arc_edges_to_ellipse_edges(self) -> None:
        """Convert all arc edges to ellipse edges."""

        def to_ellipse(arc: ArcEdge) -> EllipseEdge:
            ellipse = EllipseEdge()
            ellipse.center = arc.center
            ellipse.ratio = 1.0
            ellipse.major_axis = (arc.radius, 0.0)
            ellipse.start_angle = arc.start_angle
            ellipse.end_angle = arc.end_angle
            ellipse.ccw = arc.ccw
            return ellipse

        for path in self.paths:
            if isinstance(path, EdgePath):
                edges = path.edges
                for edge_index, edge in enumerate(edges):
                    if isinstance(edge, ArcEdge):
                        edges[edge_index] = to_ellipse(edge)

    def ellipse_edges_to_spline_edges(self, num: int = 32) -> None:
        """
        Convert all ellipse edges to spline edges (approximation).

        Args:
            num: count of control points for a **full** ellipse, partial
                ellipses have proportional fewer control points but at least 3.

        """

        def to_spline_edge(e: EllipseEdge) -> SplineEdge:
            # No OCS transformation needed, source ellipse and target spline
            # reside in the same OCS.
            ellipse = ConstructionEllipse(
                center=e.center,
                major_axis=e.major_axis,
                ratio=e.ratio,
                start_param=e.start_param,
                end_param=e.end_param,
            )
            count = max(int(float(num) * ellipse.param_span / math.tau), 3)
            tool = BSpline.ellipse_approximation(ellipse, count)
            spline = SplineEdge()
            spline.degree = tool.degree
            if not e.ccw:
                tool = tool.reverse()

            spline.control_points = Vec2.list(tool.control_points)
            spline.knot_values = tool.knots()  # type: ignore
            spline.weights = tool.weights()  # type: ignore
            return spline

        for path_index, path in enumerate(self.paths):
            if isinstance(path, EdgePath):
                edges = path.edges
                for edge_index, edge in enumerate(edges):
                    if isinstance(edge, EllipseEdge):
                        edges[edge_index] = to_spline_edge(edge)

    def spline_edges_to_line_edges(self, factor: int = 8) -> None:
        """Convert all spline edges to line edges (approximation).

        Args:
            factor: count of approximation segments = count of control
                points x factor

        """

        def to_line_edges(spline_edge: SplineEdge):
            weights = spline_edge.weights
            if len(spline_edge.control_points):
                bspline = BSpline(
                    control_points=spline_edge.control_points,
                    order=spline_edge.degree + 1,
                    knots=spline_edge.knot_values,
                    weights=weights if len(weights) else None,
                )
            elif len(spline_edge.fit_points):
                bspline = BSpline.from_fit_points(
                    spline_edge.fit_points, spline_edge.degree
                )
            else:
                raise const.DXFStructureError(
                    "SplineEdge() without control points or fit points."
                )
            segment_count = (max(len(bspline.control_points), 3) - 1) * factor
            vertices = list(bspline.approximate(segment_count))
            for v1, v2 in zip(vertices[:-1], vertices[1:]):
                edge = LineEdge()
                edge.start = v1.vec2
                edge.end = v2.vec2
                yield edge

        for path in self.paths:
            if isinstance(path, EdgePath):
                new_edges = []
                for edge in path.edges:
                    if isinstance(edge, SplineEdge):
                        new_edges.extend(to_line_edges(edge))
                    else:
                        new_edges.append(edge)
                path.edges = new_edges

    def ellipse_edges_to_line_edges(self, num: int = 64) -> None:
        """Convert all ellipse edges to line edges (approximation).

        Args:
            num: count of control points for a **full** ellipse, partial
                ellipses have proportional fewer control points but at least 3.

        """

        def to_line_edges(edge):
            # Start- and end params are always stored in counter clockwise order!
            ellipse = ConstructionEllipse(
                center=edge.center,
                major_axis=edge.major_axis,
                ratio=edge.ratio,
                start_param=edge.start_param,
                end_param=edge.end_param,
            )
            segment_count = max(int(float(num) * ellipse.param_span / math.tau), 3)
            params = ellipse.params(segment_count + 1)

            # Reverse path if necessary!
            if not edge.ccw:
                params = reversed(list(params))
            vertices = list(ellipse.vertices(params))
            for v1, v2 in zip(vertices[:-1], vertices[1:]):
                line = LineEdge()
                line.start = v1.vec2
                line.end = v2.vec2
                yield line

        for path in self.paths:
            if isinstance(path, EdgePath):
                new_edges = []
                for edge in path.edges:
                    if isinstance(edge, EllipseEdge):
                        new_edges.extend(to_line_edges(edge))
                    else:
                        new_edges.append(edge)
                path.edges = new_edges

    def all_to_spline_edges(self, num: int = 64) -> None:
        """Convert all bulge, arc and ellipse edges to spline edges
        (approximation).

        Args:
            num: count of control points for a **full** circle/ellipse, partial
                circles/ellipses have proportional fewer control points but at
                least 3.

        """
        self.polyline_to_edge_paths(just_with_bulge=True)
        self.arc_edges_to_ellipse_edges()
        self.ellipse_edges_to_spline_edges(num)

    def all_to_line_edges(self, num: int = 64, spline_factor: int = 8) -> None:
        """Convert all bulge, arc and ellipse edges to spline edges and
        approximate this splines by line edges.

        Args:
            num: count of control points for a **full** circle/ellipse, partial
                circles/ellipses have proportional fewer control points but at
                least 3.
            spline_factor: count of spline approximation segments = count of
                control points x spline_factor

        """
        self.polyline_to_edge_paths(just_with_bulge=True)
        self.arc_edges_to_ellipse_edges()
        self.ellipse_edges_to_line_edges(num)
        self.spline_edges_to_line_edges(spline_factor)

    def has_critical_elements(self) -> bool:
        """Returns ``True`` if any boundary path has bulge values or arc edges
        or ellipse edges.

        """
        for path in self.paths:
            if isinstance(path, PolylinePath):
                return path.has_bulge()
            elif isinstance(path, EdgePath):
                for edge in path.edges:
                    if edge.type in {EdgeType.ARC, EdgeType.ELLIPSE}:
                        return True
            else:
                raise TypeError(type(path))
        return False


def flatten_to_polyline_path(
    path: AbstractBoundaryPath, distance: float, segments: int = 16
) -> PolylinePath:
    import ezdxf.path  # avoid cyclic imports

    # keep path in original OCS!
    ez_path = ezdxf.path.from_hatch_boundary_path(path, ocs=OCS(), elevation=0)
    vertices = ((v.x, v.y) for v in ez_path.flattening(distance, segments))
    return PolylinePath.from_vertices(
        vertices,
        flags=path.path_type_flags,
    )


def pop_source_boundary_objects_tags(path_tags: Tags) -> list[str]:
    source_boundary_object_tags = []
    while len(path_tags):
        if path_tags[-1].code in (97, 330):
            last_tag = path_tags.pop()
            if last_tag.code == 330:
                source_boundary_object_tags.append(last_tag.value)
            else:  # code == 97
                # result list does not contain the length tag!
                source_boundary_object_tags.reverse()
                return source_boundary_object_tags
        else:
            break
            # No source boundary objects found - entity is not valid for AutoCAD
    return []


def export_source_boundary_objects(
    tagwriter: AbstractTagWriter, handles: Sequence[str]
):
    tagwriter.write_tag2(97, len(handles))
    for handle in handles:
        tagwriter.write_tag2(330, handle)


class PolylinePath(AbstractBoundaryPath):
    type = BoundaryPathType.POLYLINE

    def __init__(self) -> None:
        self.path_type_flags: int = const.BOUNDARY_PATH_POLYLINE
        self.is_closed = False
        # list of 2D coordinates with bulge values (x, y, bulge);
        # bulge default = 0.0
        self.vertices: list[tuple[float, float, float]] = []
        # MPOLYGON does not support source boundary objects, the MPOLYGON is
        # the source object!
        self.source_boundary_objects: list[str] = []  # (330, handle) tags

    def is_valid(self) -> bool:
        return True

    @classmethod
    def from_vertices(
        cls,
        path_vertices: Iterable[tuple[float, ...]],
        is_closed: bool = True,
        flags: int = 1,
    ) -> PolylinePath:
        """Create a new :class:`PolylinePath` object from vertices.

        Args:
            path_vertices: iterable of polyline vertices as (x, y) or
                (x, y, bulge)-tuples.
            is_closed: 1 for a closed polyline else 0
            flags: default(0), external(1), derived(4), textbox(8) or outermost(16)

        """
        new_path = PolylinePath()
        new_path.set_vertices(path_vertices, is_closed)
        new_path.path_type_flags = flags | const.BOUNDARY_PATH_POLYLINE
        return new_path

    @classmethod
    def load_tags(cls, tags: Tags) -> PolylinePath:
        path = PolylinePath()
        path.source_boundary_objects = pop_source_boundary_objects_tags(tags)
        for tag in tags:
            code, value = tag
            if code == 10:  # vertex coordinates
                # (x, y, bulge); bulge default = 0.0
                path.vertices.append((value[0], value[1], 0.0))
            elif code == 42:  # bulge value
                x, y, bulge = path.vertices.pop()
                # Last coordinates with new bulge value
                path.vertices.append((x, y, value))
            elif code == 72:
                pass  # ignore this value
            elif code == 73:
                path.is_closed = value
            elif code == 92:
                path.path_type_flags = value
            elif code == 93:  # number of polyline vertices
                pass  # ignore this value
        return path

    def set_vertices(
        self, vertices: Iterable[Sequence[float]], is_closed: bool = True
    ) -> None:
        """Set new `vertices` as new polyline path, a vertex has to be a
        (x, y) or a (x, y, bulge)-tuple.

        """
        new_vertices = []
        for vertex in vertices:
            if len(vertex) == 2:
                x, y = vertex
                bulge = 0.0
            elif len(vertex) == 3:
                x, y, bulge = vertex
            else:
                raise const.DXFValueError(
                    "Invalid vertex format, expected (x, y) or (x, y, bulge)"
                )
            new_vertices.append((x, y, bulge))
        self.vertices = new_vertices
        self.is_closed = is_closed

    def clear(self) -> None:
        """Removes all vertices and all handles to associated DXF objects
        (:attr:`source_boundary_objects`).
        """
        self.vertices = []
        self.is_closed = False
        self.source_boundary_objects = []

    def has_bulge(self) -> bool:
        return any(bulge for x, y, bulge in self.vertices)

    def export_dxf(self, tagwriter: AbstractTagWriter, dxftype: str) -> None:
        has_bulge = self.has_bulge()
        write_tag = tagwriter.write_tag2

        write_tag(92, int(self.path_type_flags))
        if dxftype == "HATCH":  # great design :<
            write_tag(72, int(has_bulge))
            write_tag(73, int(self.is_closed))
        elif dxftype == "MPOLYGON":
            write_tag(73, int(self.is_closed))
            write_tag(72, int(has_bulge))
        else:
            raise ValueError(f"unsupported DXF type {dxftype}")
        write_tag(93, len(self.vertices))
        for x, y, bulge in self.vertices:
            tagwriter.write_vertex(10, (float(x), float(y)))
            if has_bulge:
                write_tag(42, float(bulge))

        if dxftype == "HATCH":
            export_source_boundary_objects(tagwriter, self.source_boundary_objects)

    def transform(self, ocs: OCSTransform, elevation: float) -> None:
        """Transform polyline path."""
        has_non_uniform_scaling = not ocs.scale_uniform

        def _transform():
            for x, y, bulge in self.vertices:
                # PolylinePaths() with arcs should be converted to
                # EdgePath(in BoundaryPath.transform()).
                if bulge and has_non_uniform_scaling:
                    raise NonUniformScalingError(
                        "Polyline path with arcs does not support non-uniform "
                        "scaling"
                    )
                v = ocs.transform_vertex(Vec3(x, y, elevation))
                yield v.x, v.y, bulge

        if self.vertices:
            self.vertices = list(_transform())


class EdgePath(AbstractBoundaryPath):
    type = BoundaryPathType.EDGE

    def __init__(self) -> None:
        self.path_type_flags = const.BOUNDARY_PATH_DEFAULT
        self.edges: list[AbstractEdge] = []
        self.source_boundary_objects = []

    def __iter__(self):
        return iter(self.edges)

    def is_valid(self) -> bool:
        return all(e.is_valid() for e in self.edges)

    @classmethod
    def load_tags(cls, tags: Tags) -> EdgePath:
        edge_path = cls()
        edge_path.source_boundary_objects = pop_source_boundary_objects_tags(tags)
        for edge_tags in group_tags(tags, splitcode=72):
            if len(edge_tags) == 0:
                continue
            edge_type = edge_tags[0].value
            if 0 < edge_type < 5:
                edge_path.edges.append(EDGE_CLASSES[edge_type].load_tags(edge_tags[1:]))
        return edge_path

    def transform(self, ocs: OCSTransform, elevation: float) -> None:
        """Transform edge boundary paths."""
        for edge in self.edges:
            edge.transform(ocs, elevation=elevation)

    def add_line(self, start: UVec, end: UVec) -> LineEdge:
        """Add a :class:`LineEdge` from `start` to `end`.

        Args:
            start: start point of line, (x, y)-tuple
            end: end point of line, (x, y)-tuple

        """
        line = LineEdge()
        line.start = Vec2(start)
        line.end = Vec2(end)
        self.edges.append(line)
        return line

    def add_arc(
        self,
        center: UVec,
        radius: float = 1.0,
        start_angle: float = 0.0,
        end_angle: float = 360.0,
        ccw: bool = True,
    ) -> ArcEdge:
        """Add an :class:`ArcEdge`.

        **Adding Clockwise Oriented Arcs:**

        Clockwise oriented :class:`ArcEdge` objects are sometimes necessary to
        build closed loops, but the :class:`ArcEdge` objects are always
        represented in counter-clockwise orientation.
        To add a clockwise oriented :class:`ArcEdge` you have to swap the
        start- and end angle and set the `ccw` flag to ``False``,
        e.g. to add a clockwise oriented :class:`ArcEdge` from 180 to 90 degree,
        add the :class:`ArcEdge` in counter-clockwise orientation with swapped
        angles::

            edge_path.add_arc(center, radius, start_angle=90, end_angle=180, ccw=False)

        Args:
            center: center point of arc, (x, y)-tuple
            radius: radius of circle
            start_angle: start angle of arc in degrees (`end_angle` for a
                clockwise oriented arc)
            end_angle: end angle of arc in degrees (`start_angle` for a
                clockwise oriented arc)
            ccw: ``True`` for counter-clockwise ``False`` for
                clockwise orientation

        """
        arc = ArcEdge()
        arc.center = Vec2(center)
        arc.radius = radius
        # Start- and end angles always for counter-clockwise oriented arcs!
        arc.start_angle = start_angle
        arc.end_angle = end_angle
        # Flag to export the counter-clockwise oriented arc in
        # correct clockwise orientation:
        arc.ccw = bool(ccw)
        self.edges.append(arc)
        return arc

    def add_ellipse(
        self,
        center: UVec,
        major_axis: UVec = (1.0, 0.0),
        ratio: float = 1.0,
        start_angle: float = 0.0,
        end_angle: float = 360.0,
        ccw: bool = True,
    ) -> EllipseEdge:
        """Add an :class:`EllipseEdge`.

        **Adding Clockwise Oriented Ellipses:**

        Clockwise oriented :class:`EllipseEdge` objects are sometimes necessary
        to build closed loops, but the :class:`EllipseEdge` objects are always
        represented in counter-clockwise orientation.
        To add a clockwise oriented :class:`EllipseEdge` you have to swap the
        start- and end angle and set the `ccw` flag to ``False``,
        e.g. to add a clockwise oriented :class:`EllipseEdge` from 180 to 90
        degree, add the :class:`EllipseEdge` in counter-clockwise orientation
        with swapped angles::

            edge_path.add_ellipse(center, major_axis, ratio, start_angle=90, end_angle=180, ccw=False)

        Args:
            center: center point of ellipse, (x, y)-tuple
            major_axis: vector of major axis as (x, y)-tuple
            ratio: ratio of minor axis to major axis as float
            start_angle: start angle of ellipse in degrees (`end_angle` for a
                clockwise oriented ellipse)
            end_angle: end angle of ellipse in degrees (`start_angle` for a
                clockwise oriented ellipse)
            ccw: ``True`` for counter-clockwise ``False`` for
                clockwise orientation

        """

        if ratio > 1.0:
            raise const.DXFValueError("argument 'ratio' has to be <= 1.0")
        ellipse = EllipseEdge()
        ellipse.center = Vec2(center)
        ellipse.major_axis = Vec2(major_axis)
        ellipse.ratio = ratio
        # Start- and end angles are always stored in counter-clockwise
        # orientation!
        ellipse.start_angle = start_angle
        ellipse.end_angle = end_angle
        # Flag to export the counter-clockwise oriented ellipse in
        # correct clockwise orientation:
        ellipse.ccw = bool(ccw)
        self.edges.append(ellipse)
        return ellipse

    def add_spline(
        self,
        fit_points: Optional[Iterable[UVec]] = None,
        control_points: Optional[Iterable[UVec]] = None,
        knot_values: Optional[Iterable[float]] = None,
        weights: Optional[Iterable[float]] = None,
        degree: int = 3,
        periodic: int = 0,
        start_tangent: Optional[UVec] = None,
        end_tangent: Optional[UVec] = None,
    ) -> SplineEdge:
        """Add a :class:`SplineEdge`.

        Args:
            fit_points: points through which the spline must go, at least 3 fit
                points are required. list of (x, y)-tuples
            control_points: affects the shape of the spline, mandatory and
                AutoCAD crashes on invalid data. list of (x, y)-tuples
            knot_values: (knot vector) mandatory and AutoCAD crashes on invalid
                data. list of floats; `ezdxf` provides two tool functions to
                calculate valid knot values: :func:`ezdxf.math.uniform_knot_vector`,
                :func:`ezdxf.math.open_uniform_knot_vector` (default if ``None``)
            weights: weight of control point, not mandatory, list of floats.
            degree: degree of spline (int)
            periodic: 1 for periodic spline, 0 for none periodic spline
            start_tangent: start_tangent as 2d vector, optional
            end_tangent: end_tangent as 2d vector, optional

        .. warning::

            Unlike for the spline entity AutoCAD does not calculate the
            necessary `knot_values` for the spline edge itself. On the contrary,
            if the `knot_values` in the spline edge are missing or invalid
            AutoCAD **crashes**.

        """
        spline = SplineEdge()
        if fit_points is not None:
            spline.fit_points = Vec2.list(fit_points)
        if control_points is not None:
            spline.control_points = Vec2.list(control_points)
        if knot_values is not None:
            spline.knot_values = list(knot_values)
        else:
            spline.knot_values = list(
                open_uniform_knot_vector(len(spline.control_points), degree + 1)
            )
        if weights is not None:
            spline.weights = list(weights)
        spline.degree = degree
        spline.rational = int(bool(len(spline.weights)))
        spline.periodic = int(periodic)
        if start_tangent is not None:
            spline.start_tangent = Vec2(start_tangent)
        if end_tangent is not None:
            spline.end_tangent = Vec2(end_tangent)
        self.edges.append(spline)
        return spline

    def add_spline_control_frame(
        self,
        fit_points: Iterable[tuple[float, float]],
        degree: int = 3,
        method: str = "distance",
    ) -> SplineEdge:
        bspline = global_bspline_interpolation(
            fit_points=fit_points, degree=degree, method=method
        )
        return self.add_spline(
            fit_points=fit_points,
            control_points=bspline.control_points,
            knot_values=bspline.knots(),
        )

    def clear(self) -> None:
        """Delete all edges."""
        self.edges = []

    def export_dxf(self, tagwriter: AbstractTagWriter, dxftype: str) -> None:
        tagwriter.write_tag2(92, int(self.path_type_flags))
        tagwriter.write_tag2(93, len(self.edges))
        for edge in self.edges:
            edge.export_dxf(tagwriter)
        export_source_boundary_objects(tagwriter, self.source_boundary_objects)

    def close_gaps(self, len_tol: float) -> None:
        """Insert line-edges between the existing edges if the gap between these edges
        are bigger than `len_tol`.

        .. versionadded:: 1.4

        """
        if len(self.edges) < 2:
            return
        current_edges = list(self.edges)
        first_edge = current_edges.pop(0)
        current_edges.append(first_edge)

        new_edges: list[AbstractEdge] = [first_edge]
        end_point = first_edge.real_end_point
        for edge in current_edges:
            start_point = edge.real_start_point
            if end_point.distance(start_point) > len_tol:
                line_edge = LineEdge()
                line_edge.start = end_point
                line_edge.end = start_point
                new_edges.append(line_edge)
            if edge is not first_edge:
                new_edges.append(edge)
            end_point = edge.real_end_point
        self.edges = new_edges


class LineEdge(AbstractEdge):
    EDGE_TYPE = "LineEdge"  # 2021-05-31: deprecated use type
    type = EdgeType.LINE

    def __init__(self):
        self.start = Vec2(0, 0)  # OCS!
        self.end = Vec2(0, 0)  # OCS!

    def is_valid(self) -> bool:
        return True

    @property
    def start_point(self) -> Vec2:
        return self.start

    @property
    def end_point(self) -> Vec2:
        return self.end

    @classmethod
    def load_tags(cls, tags: Tags) -> LineEdge:
        edge = cls()
        for tag in tags:
            code, value = tag
            if code == 10:
                edge.start = Vec2(value)
            elif code == 11:
                edge.end = Vec2(value)
        return edge

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        tagwriter.write_tag2(72, 1)  # edge type

        x, y, *_ = self.start
        tagwriter.write_tag2(10, float(x))
        tagwriter.write_tag2(20, float(y))

        x, y, *_ = self.end
        tagwriter.write_tag2(11, float(x))
        tagwriter.write_tag2(21, float(y))

    def transform(self, ocs: OCSTransform, elevation: float) -> None:
        self.start = ocs.transform_2d_vertex(self.start, elevation)
        self.end = ocs.transform_2d_vertex(self.end, elevation)


class ArcEdge(AbstractEdge):
    type = EdgeType.ARC  # 2021-05-31: deprecated use type
    EDGE_TYPE = "ArcEdge"

    def __init__(self) -> None:
        self.center = Vec2(0.0, 0.0)
        self.radius: float = 1.0
        # Start- and end angles are always stored in counter-clockwise order!
        self.start_angle: float = 0.0
        self.end_angle: float = 360.0
        # Flag to preserve the required orientation for DXF export:
        self.ccw: bool = True

    def is_valid(self) -> bool:
        return True

    @property
    def start_point(self) -> Vec2:
        return self.construction_tool().start_point

    @property
    def real_start_point(self) -> Vec2:
        if self.ccw:
            return self.start_point
        return self.end_point

    @property
    def end_point(self) -> Vec2:
        return self.construction_tool().end_point

    @property
    def real_end_point(self) -> Vec2:
        if self.ccw:
            return self.end_point
        return self.start_point

    @classmethod
    def load_tags(cls, tags: Tags) -> ArcEdge:
        edge = cls()
        start = 0.0
        end = 0.0
        for tag in tags:
            code, value = tag
            if code == 10:
                edge.center = Vec2(value)
            elif code == 40:
                edge.radius = value
            elif code == 50:
                start = value
            elif code == 51:
                end = value
            elif code == 73:
                edge.ccw = bool(value)

        # The DXF format stores the clockwise oriented start- and end angles
        # for HATCH arc- and ellipse edges as complementary angle (360-angle).
        # This is a problem in many ways for processing clockwise oriented
        # angles correct, especially rotation transformation won't work.
        # Solution: convert clockwise angles into counter-clockwise angles
        # and swap start- and end angle at loading and exporting, the ccw flag
        # preserves the required orientation of the arc:
        if edge.ccw:
            edge.start_angle = start
            edge.end_angle = end
        else:
            edge.start_angle = 360.0 - end
            edge.end_angle = 360.0 - start
        return edge

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        tagwriter.write_tag2(72, 2)  # edge type
        x, y, *_ = self.center
        if self.ccw:
            start = self.start_angle
            end = self.end_angle
        else:
            # swap and convert to complementary angles: see ArcEdge.load_tags()
            # for explanation
            start = 360.0 - self.end_angle
            end = 360.0 - self.start_angle
        tagwriter.write_tag2(10, float(x))
        tagwriter.write_tag2(20, float(y))
        tagwriter.write_tag2(40, self.radius)
        tagwriter.write_tag2(50, start)
        tagwriter.write_tag2(51, end)
        tagwriter.write_tag2(73, int(self.ccw))

    def transform(self, ocs: OCSTransform, elevation: float) -> None:
        self.center = ocs.transform_2d_vertex(self.center, elevation)
        self.radius = ocs.transform_length(Vec3(self.radius, 0, 0))
        if not math.isclose(
            arc_angle_span_deg(self.start_angle, self.end_angle), 360.0
        ):  # open arc
            # The transformation of the ccw flag is not necessary for the current
            # implementation of OCS transformations. The arc angles have always
            # a counter clockwise orientation around the extrusion vector and
            # this orientation is preserved even for mirroring, which flips the
            # extrusion vector to (0, 0, -1) for entities in the xy-plane.

            self.start_angle = ocs.transform_deg_angle(self.start_angle)
            self.end_angle = ocs.transform_deg_angle(self.end_angle)
        else:  # full circle
            # Transform only start point to preserve the connection point to
            # adjacent edges:
            self.start_angle = ocs.transform_deg_angle(self.start_angle)
            # ArcEdge is represented in counter-clockwise orientation:
            self.end_angle = self.start_angle + 360.0

    def construction_tool(self) -> ConstructionArc:
        """Returns ConstructionArc() for the OCS representation."""
        return ConstructionArc(
            center=self.center,
            radius=self.radius,
            start_angle=self.start_angle,
            end_angle=self.end_angle,
        )


class EllipseEdge(AbstractEdge):
    EDGE_TYPE = "EllipseEdge"  # 2021-05-31: deprecated use type
    type = EdgeType.ELLIPSE

    def __init__(self) -> None:
        self.center = Vec2((0.0, 0.0))
        # Endpoint of major axis relative to center point (in OCS)
        self.major_axis = Vec2((1.0, 0.0))
        self.ratio: float = 1.0
        # Start- and end angles are always stored in counter-clockwise order!
        self.start_angle: float = 0.0  # start param, not a real angle
        self.end_angle: float = 360.0  # end param, not a real angle
        # Flag to preserve the required orientation for DXF export:
        self.ccw: bool = True

    def is_valid(self) -> bool:
        return True

    @property
    def start_point(self) -> Vec2:
        return self.construction_tool().start_point.vec2

    @property
    def end_point(self) -> Vec2:
        return self.construction_tool().end_point.vec2

    @property
    def real_start_point(self) -> Vec2:
        if self.ccw:
            return self.start_point
        return self.end_point

    @property
    def real_end_point(self) -> Vec2:
        if self.ccw:
            return self.end_point
        return self.start_point

    @property
    def start_param(self) -> float:
        return angle_to_param(self.ratio, math.radians(self.start_angle))

    @start_param.setter
    def start_param(self, param: float) -> None:
        self.start_angle = math.degrees(param_to_angle(self.ratio, param))

    @property
    def end_param(self) -> float:
        return angle_to_param(self.ratio, math.radians(self.end_angle))

    @end_param.setter
    def end_param(self, param: float) -> None:
        self.end_angle = math.degrees(param_to_angle(self.ratio, param))

    @classmethod
    def load_tags(cls, tags: Tags) -> EllipseEdge:
        edge = cls()
        start = 0.0
        end = 0.0
        for tag in tags:
            code, value = tag
            if code == 10:
                edge.center = Vec2(value)
            elif code == 11:
                edge.major_axis = Vec2(value)
            elif code == 40:
                edge.ratio = value
            elif code == 50:
                start = value
            elif code == 51:
                end = value
            elif code == 73:
                edge.ccw = bool(value)

        if edge.ccw:
            edge.start_angle = start
            edge.end_angle = end
        else:
            # The DXF format stores the clockwise oriented start- and end angles
            # for HATCH arc- and ellipse edges as complementary angle (360-angle).
            # This is a problem in many ways for processing clockwise oriented
            # angles correct, especially rotation transformation won't work.
            # Solution: convert clockwise angles into counter-clockwise angles
            # and swap start- and end angle at loading and exporting, the ccw flag
            # preserves the required orientation of the ellipse:
            edge.start_angle = 360.0 - end
            edge.end_angle = 360.0 - start

        return edge

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        tagwriter.write_tag2(72, 3)  # edge type
        x, y, *_ = self.center
        tagwriter.write_tag2(10, float(x))
        tagwriter.write_tag2(20, float(y))
        x, y, *_ = self.major_axis
        tagwriter.write_tag2(11, float(x))
        tagwriter.write_tag2(21, float(y))
        tagwriter.write_tag2(40, self.ratio)
        if self.ccw:
            start = self.start_angle
            end = self.end_angle
        else:
            # swap and convert to complementary angles: see EllipseEdge.load_tags()
            # for explanation
            start = 360.0 - self.end_angle
            end = 360.0 - self.start_angle

        tagwriter.write_tag2(50, start)
        tagwriter.write_tag2(51, end)
        tagwriter.write_tag2(73, int(self.ccw))

    def construction_tool(self) -> ConstructionEllipse:
        """Returns ConstructionEllipse() for the OCS representation."""
        return ConstructionEllipse(
            center=Vec3(self.center),
            major_axis=Vec3(self.major_axis),
            extrusion=Vec3(0, 0, 1),
            ratio=self.ratio,
            # 1. ConstructionEllipse() is always in ccw orientation
            # 2. Start- and end params are always stored in ccw orientation
            start_param=self.start_param,
            end_param=self.end_param,
        )

    def transform(self, ocs: OCSTransform, elevation: float) -> None:
        e = self.construction_tool()

        # Transform old OCS representation to WCS
        ocs_to_wcs = ocs.old_ocs.to_wcs
        e.center = ocs_to_wcs(e.center.replace(z=elevation))
        e.major_axis = ocs_to_wcs(e.major_axis)
        e.extrusion = ocs.old_extrusion

        # Apply matrix transformation
        e.transform(ocs.m)

        # Transform WCS representation to new OCS
        wcs_to_ocs = ocs.new_ocs.from_wcs
        self.center = wcs_to_ocs(e.center).vec2
        self.major_axis = wcs_to_ocs(e.major_axis).vec2
        self.ratio = e.ratio

        # ConstructionEllipse() is always in ccw orientation
        # Start- and end params are always stored in ccw orientation
        self.start_param = e.start_param
        self.end_param = e.end_param

        # The transformation of the ccw flag is not necessary for the current
        # implementation of OCS transformations.
        # An ellipse as boundary edge is an OCS entity!
        # The ellipse angles have always a counter clockwise orientation around
        # the extrusion vector and this orientation is preserved even for
        # mirroring, which flips the extrusion vector to (0, 0, -1) for
        # entities in the xy-plane.

        # normalize angles in range 0 to 360 degrees
        self.start_angle = self.start_angle % 360.0
        self.end_angle = self.end_angle % 360.0
        if math.isclose(self.end_angle, 0):
            self.end_angle = 360.0


class SplineEdge(AbstractEdge):
    EDGE_TYPE = "SplineEdge"  # 2021-05-31: deprecated use type
    type = EdgeType.SPLINE

    def __init__(self) -> None:
        self.degree: int = 3  # code = 94
        self.rational: int = 0  # code = 73
        self.periodic: int = 0  # code = 74
        self.knot_values: list[float] = []
        self.control_points: list[Vec2] = []
        self.fit_points: list[Vec2] = []
        self.weights: list[float] = []
        # do not set tangents by default to (0, 0)
        self.start_tangent: Optional[Vec2] = None
        self.end_tangent: Optional[Vec2] = None

    def is_valid(self) -> bool:
        if len(self.control_points):
            order = self.degree + 1
            count = len(self.control_points)
            if order > count:
                return False
            required_knot_count = count + order
            if len(self.knot_values) != required_knot_count:
                return False
        elif len(self.fit_points) < 2:
            return False
        return True

    @property
    def start_point(self) -> Vec2:
        return self.control_points[0]

    @property
    def end_point(self) -> Vec2:
        return self.control_points[-1]

    @classmethod
    def load_tags(cls, tags: Tags) -> SplineEdge:
        edge = cls()
        for tag in tags:
            code, value = tag
            if code == 94:
                edge.degree = value
            elif code == 73:
                edge.rational = value
            elif code == 74:
                edge.periodic = value
            elif code == 40:
                edge.knot_values.append(value)
            elif code == 42:
                edge.weights.append(value)
            elif code == 10:
                edge.control_points.append(Vec2(value))
            elif code == 11:
                edge.fit_points.append(Vec2(value))
            elif code == 12:
                edge.start_tangent = Vec2(value)
            elif code == 13:
                edge.end_tangent = Vec2(value)
        return edge

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        def set_required_tangents(points: list[Vec2]):
            if len(points) > 1:
                if self.start_tangent is None:
                    self.start_tangent = points[1] - points[0]
                if self.end_tangent is None:
                    self.end_tangent = points[-1] - points[-2]

        if len(self.weights):
            if len(self.weights) == len(self.control_points):
                self.rational = 1
            else:
                raise const.DXFValueError(
                    "SplineEdge: count of control points and count of weights "
                    "mismatch"
                )
        else:
            self.rational = 0

        write_tag = tagwriter.write_tag2
        write_tag(72, 4)  # edge type
        write_tag(94, int(self.degree))
        write_tag(73, int(self.rational))
        write_tag(74, int(self.periodic))
        write_tag(95, len(self.knot_values))  # number of knots
        write_tag(96, len(self.control_points))  # number of control points
        # build knot values list
        # knot values have to be present and valid, otherwise AutoCAD crashes
        if len(self.knot_values):
            for value in self.knot_values:
                write_tag(40, float(value))
        else:
            raise const.DXFValueError("SplineEdge: missing required knot values")

        # build control points
        # control points have to be present and valid, otherwise AutoCAD crashes
        cp = Vec2.list(self.control_points)
        if self.rational:
            for point, weight in zip(cp, self.weights):
                write_tag(10, float(point.x))
                write_tag(20, float(point.y))
                write_tag(42, float(weight))
        else:
            for x, y in cp:
                write_tag(10, float(x))
                write_tag(20, float(y))

        # build optional fit points
        if len(self.fit_points) > 0:
            set_required_tangents(cp)
            write_tag(97, len(self.fit_points))
            for x, y, *_ in self.fit_points:
                write_tag(11, float(x))
                write_tag(21, float(y))
        elif tagwriter.dxfversion >= const.DXF2010:
            # (97, 0) len tag required by AutoCAD 2010+
            write_tag(97, 0)

        if self.start_tangent is not None:
            x, y, *_ = self.start_tangent
            write_tag(12, float(x))
            write_tag(22, float(y))

        if self.end_tangent is not None:
            x, y, *_ = self.end_tangent
            write_tag(13, float(x))
            write_tag(23, float(y))

    def transform(self, ocs: OCSTransform, elevation: float) -> None:
        self.control_points = list(
            ocs.transform_2d_vertex(v, elevation) for v in self.control_points
        )
        self.fit_points = list(
            ocs.transform_2d_vertex(v, elevation) for v in self.fit_points
        )
        if self.start_tangent is not None:
            t = Vec3(self.start_tangent).replace(z=elevation)
            self.start_tangent = ocs.transform_direction(t).vec2
        if self.end_tangent is not None:
            t = Vec3(self.end_tangent).replace(z=elevation)
            self.end_tangent = ocs.transform_direction(t).vec2

    def construction_tool(self) -> BSpline:
        """Returns BSpline() for the OCS representation."""
        return BSpline(
            control_points=self.control_points,
            knots=self.knot_values,
            order=self.degree + 1,
            weights=self.weights,
        )


EDGE_CLASSES = [None, LineEdge, ArcEdge, EllipseEdge, SplineEdge]
