# Copyright (c) 2020-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    Tuple,
    Union,
    cast,
    Sequence,
    Optional,
)
from typing_extensions import TypeAlias
from abc import abstractmethod
from collections import namedtuple
import math
import numpy as np

from ezdxf.lldxf.const import VTX_EXTRA_VERTEX_CREATED, VTX_SPLINE_FRAME_CONTROL_POINT
from ezdxf.math import (
    Vec2,
    Vec3,
    UVec,
    BSpline,
    ConstructionRay,
    OCS,
    ParallelRaysError,
    bulge_to_arc,
    ConstructionArc,
)

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFGraphic, Solid, Trace, Face3d, LWPolyline, Polyline

__all__ = ["TraceBuilder", "LinearTrace", "CurvedTrace"]

LinearStation = namedtuple("LinearStation", ("vertex", "start_width", "end_width"))
# start_width of the next (following) segment
# end_width of the next (following) segment

CurveStation = namedtuple("CurveStation", ("vertex0", "vertex1"))

Face: TypeAlias = Tuple[Vec2, Vec2, Vec2, Vec2]
Polygon: TypeAlias = Sequence[Vec2]
Quadrilateral: TypeAlias = Union["Solid", "Trace", "Face3d"]


class AbstractTrace:
    @abstractmethod
    def faces(self) -> Iterable[Face]:
        # vertex order: up1, down1, down2, up2
        # faces connections:
        # up2 -> next up1
        # down2 -> next down1
        pass

    def polygon(self) -> Polygon:
        def merge(vertices: Polygon) -> Iterable[UVec]:
            if not len(vertices):
                return

            _vertices = iter(vertices)
            prev = next(_vertices)
            yield prev
            for vertex in _vertices:
                if not prev.isclose(vertex):
                    yield vertex
                    prev = vertex

        forward_contour: list[Vec2] = []
        backward_contour: list[Vec2] = []
        for up1, down1, down2, up2 in self.faces():
            forward_contour.extend((down1, down2))
            backward_contour.extend((up1, up2))

        contour = list(merge(forward_contour))
        contour.extend(reversed(list(merge(backward_contour))))
        return contour

    def virtual_entities(
        self, dxftype="TRACE", dxfattribs=None, doc: Optional[Drawing] = None
    ) -> Iterable[Quadrilateral]:
        """
        Yields faces as SOLID, TRACE or 3DFACE entities with DXF attributes
        given in `dxfattribs`.

        If a document is given, the doc attribute of the new entities will be
        set and the new entities will be automatically added to the entity
        database of that document.

        Args:
            dxftype: DXF type as string, "SOLID", "TRACE" or "3DFACE"
            dxfattribs: DXF attributes for SOLID, TRACE or 3DFACE entities
            doc: associated document

        """
        from ezdxf.entities.factory import new

        if dxftype not in {"SOLID", "TRACE", "3DFACE"}:
            raise TypeError(f"Invalid dxftype {dxftype}.")
        dxfattribs = dict(dxfattribs or {})
        for face in self.faces():
            for i in range(4):
                dxfattribs[f"vtx{i}"] = face[i]

            if dxftype != "3DFACE":
                # weird vertex order for SOLID and TRACE
                dxfattribs["vtx2"] = face[3]
                dxfattribs["vtx3"] = face[2]
            entity = new(dxftype, dxfattribs, doc)
            if doc:
                doc.entitydb.add(entity)
            yield entity  # type: ignore


class LinearTrace(AbstractTrace):
    """Linear 2D banded lines like polylines with start- and end width.

    Accepts 3D input, but z-axis is ignored.

    """

    def __init__(self) -> None:
        self._stations: list[LinearStation] = []
        self.abs_tol = 1e-12

    def __len__(self):
        return len(self._stations)

    def __getitem__(self, item):
        return self._stations[item]

    @property
    def is_started(self) -> bool:
        """`True` if at least one station exist."""
        return bool(self._stations)

    def add_station(
        self, point: UVec, start_width: float, end_width: Optional[float] = None
    ) -> None:
        """Add a trace station (like a vertex) at location `point`,
        `start_width` is the width of the next segment starting at this station,
        `end_width` is the end width of the next segment.

        Adding the last location again, replaces the actual last location e.g.
        adding lines (a, b), (b, c), creates only 3 stations (a, b, c), this is
        very important to connect to/from splines.

        Args:
            point: 2D location (vertex), z-axis of 3D vertices is ignored.
            start_width: start width of next segment
            end_width:  end width of next segment

        """
        if end_width is None:
            end_width = start_width
        point = Vec2(point)
        stations = self._stations

        if bool(stations) and stations[-1].vertex.isclose(point, abs_tol=self.abs_tol):
            # replace last station
            stations.pop()
        stations.append(LinearStation(point, float(start_width), float(end_width)))

    def faces(self) -> Iterable[Face]:
        """Yields all faces as 4-tuples of :class:`~ezdxf.math.Vec2` objects.

        First and last miter is 90 degrees if the path is not closed, otherwise
        the intersection of first and last segment is taken into account,
        a closed path has to have explicit the same last and first vertex.

        """
        stations = self._stations
        count = len(stations)
        if count < 2:  # Two or more stations required to create faces
            return

        def offset_rays(
            segment: int,
        ) -> tuple[ConstructionRay, ConstructionRay]:
            """Create offset rays from segment offset vertices."""

            def ray(v1, v2):
                if v1.isclose(v2):
                    # vertices too close to define a ray, offset ray is parallel to segment:
                    angle = (
                        stations[segment].vertex - stations[segment + 1].vertex
                    ).angle
                    return ConstructionRay(v1, angle)
                else:
                    return ConstructionRay(v1, v2)

            left1, left2, right1, right2 = segments[segment]
            return ray(left1, left2), ray(right1, right2)

        def intersect(
            ray1: ConstructionRay, ray2: ConstructionRay, default: Vec2
        ) -> Vec2:
            """Intersect two rays but take parallel rays into account."""
            # check for nearly parallel rays pi/100 ~1.8 degrees
            angle = abs(ray1.direction.angle_between(ray2.direction))
            if angle < 0.031415 or abs(math.pi - angle) < 0.031415:
                return default
            try:
                return ray1.intersect(ray2)
            except ParallelRaysError:
                return default

        # Path has to be explicit closed by vertices:
        is_closed = stations[0].vertex.isclose(stations[-1].vertex)

        segments = []
        # Each segment has 4 offset vertices normal to the line from start- to
        # end vertex
        # 1st vertex left of line at the start, distance = start_width/2
        # 2nd vertex left of line at the end, distance = end_width/2
        # 3rd vertex right of line at the start, distance = start_width/2
        # 4th vertex right of line at the end, distance = end_width/2
        for station in range(count - 1):
            start_vertex, start_width, end_width = stations[station]
            end_vertex = stations[station + 1].vertex
            # Start- and end vertex are never to close together, close stations
            # will be merged in method LinearTrace.add_station().
            segments.append(
                _normal_offset_points(start_vertex, end_vertex, start_width, end_width)
            )

        # offset rays:
        # 1 is the upper or left of line
        # 2 is the lower or right of line
        offset_ray1, offset_ray2 = offset_rays(0)
        prev_offset_ray1 = None
        prev_offset_ray2 = None

        # Store last vertices explicit, they get modified for closed paths.
        last_up1, last_up2, last_down1, last_down2 = segments[-1]

        for i in range(len(segments)):
            up1, up2, down1, down2 = segments[i]
            if i == 0:
                # Set first vertices of the first face.
                if is_closed:
                    # Compute first two vertices as intersection of first and
                    # last segment
                    last_offset_ray1, last_offset_ray2 = offset_rays(len(segments) - 1)
                    vtx0 = intersect(last_offset_ray1, offset_ray1, up1)
                    vtx1 = intersect(last_offset_ray2, offset_ray2, down1)

                    # Store last vertices for the closing face.
                    last_up2 = vtx0
                    last_down2 = vtx1
                else:
                    # Set first two vertices of the first face for an open path.
                    vtx0 = up1
                    vtx1 = down1
                prev_offset_ray1 = offset_ray1
                prev_offset_ray2 = offset_ray2
            else:
                # Compute first two vertices for the actual face.
                vtx0 = intersect(prev_offset_ray1, offset_ray1, up1)  # type: ignore
                vtx1 = intersect(prev_offset_ray2, offset_ray2, down1)  # type: ignore

            if i < len(segments) - 1:
                # Compute last two vertices for the actual face.
                next_offset_ray1, next_offset_ray2 = offset_rays(i + 1)
                vtx2 = intersect(next_offset_ray2, offset_ray2, down2)
                vtx3 = intersect(next_offset_ray1, offset_ray1, up2)
                prev_offset_ray1 = offset_ray1
                prev_offset_ray2 = offset_ray2
                offset_ray1 = next_offset_ray1
                offset_ray2 = next_offset_ray2
            else:
                # Pickup last two vertices for the last face.
                vtx2 = last_down2
                vtx3 = last_up2
            yield vtx0, vtx1, vtx2, vtx3


def _normal_offset_points(
    start: Vec2, end: Vec2, start_width: float, end_width: float
) -> Face:
    dir_vector = (end - start).normalize()
    ortho = dir_vector.orthogonal(True)
    offset_start = ortho.normalize(start_width / 2)
    offset_end = ortho.normalize(end_width / 2)
    return (
        start + offset_start,
        end + offset_end,
        start - offset_start,
        end - offset_end,
    )


_NULLVEC2 = Vec2((0, 0))


class CurvedTrace(AbstractTrace):
    """2D banded curves like arcs or splines with start- and end width.

    Represents always only one curved entity and all miter of curve segments
    are perpendicular to curve tangents.

    Accepts 3D input, but z-axis is ignored.

    """

    def __init__(self) -> None:
        self._stations: list[CurveStation] = []

    def __len__(self):
        return len(self._stations)

    def __getitem__(self, item):
        return self._stations[item]

    @classmethod
    def from_spline(
        cls,
        spline: BSpline,
        start_width: float,
        end_width: float,
        segments: int,
    ) -> CurvedTrace:
        """
        Create curved trace from a B-spline.

        Args:
            spline: :class:`~ezdxf.math.BSpline` object
            start_width: start width
            end_width: end width
            segments: count of segments for approximation

        """
        curve_trace = cls()
        count = segments + 1
        t = np.linspace(0, spline.max_t, count)
        for (point, derivative), width in zip(
            spline.derivatives(t, n=1), np.linspace(start_width, end_width, count)
        ):
            normal = Vec2(derivative).orthogonal(True)
            curve_trace._append(Vec2(point), normal, width)  # type: ignore
        return curve_trace

    @classmethod
    def from_arc(
        cls,
        arc: ConstructionArc,
        start_width: float,
        end_width: float,
        segments: int = 64,
    ) -> CurvedTrace:
        """
        Create curved trace from an arc.

        Args:
            arc: :class:`~ezdxf.math.ConstructionArc` object
            start_width: start width
            end_width: end width
            segments: count of segments for full circle (360 degree)
                approximation, partial arcs have proportional less segments,
                but at least 3

        Raises:
            ValueError: if arc.radius <= 0

        """
        if arc.radius <= 0:
            raise ValueError(f"Invalid radius: {arc.radius}.")
        curve_trace = cls()
        count = max(math.ceil(arc.angle_span / 360.0 * segments), 3) + 1
        center = Vec2(arc.center)
        for point, width in zip(
            arc.vertices(arc.angles(count)),
            np.linspace(start_width, end_width, count),
        ):
            curve_trace._append(point, point - center, width)  # type: ignore
        return curve_trace

    def _append(self, point: Vec2, normal: Vec2, width: float) -> None:
        """
        Add a curve trace station (like a vertex) at location `point`.

        Args:
            point: 2D curve location (vertex), z-axis of 3D vertices is ignored.
            normal: curve normal
            width:  width of station

        """
        if _NULLVEC2.isclose(normal):
            normal = _NULLVEC2
        else:
            normal = normal.normalize(width / 2)
        self._stations.append(CurveStation(point + normal, point - normal))

    def faces(self) -> Iterable[Face]:
        """Yields all faces as 4-tuples of :class:`~ezdxf.math.Vec2` objects."""
        count = len(self._stations)
        if count < 2:  # Two or more stations required to create faces
            return

        vtx0 = None
        vtx1 = None
        for vtx2, vtx3 in self._stations:
            if vtx0 is None:
                vtx0 = vtx3
                vtx1 = vtx2
                continue
            yield vtx0, vtx1, vtx2, vtx3
            vtx0 = vtx3
            vtx1 = vtx2


def should_include_vertex_for_rendering(vertex_flags: int) -> bool:
    """Determine if a vertex should be included in rendering based on its flags.

    According to the DXF specification:
    - Flag 0: Straight-segment vertex (include)
    - Flag 1: Extra curve-fit vertex (exclude - auto-generated)
    - Flag 2: Curve-fit tangent vertex (include - carries tangent info)
    - Flag 8: Spline-vertex (fit-point) for 2D spline (include)
    - Flag 16: Spline-frame control point (exclude - internal calculation only)

    Args:
        vertex_flags: The vertex flags value (group code 70)

    Returns:
        True if the vertex should be included in rendering, False otherwise
    """
    # Exclude extra vertices created by curve-fitting (flag 1)
    if vertex_flags & VTX_EXTRA_VERTEX_CREATED:
        return False

    # Exclude spline frame control points (flag 16)
    if vertex_flags & VTX_SPLINE_FRAME_CONTROL_POINT:
        return False

    # Include all other vertex types:
    # - Flag 0: Regular straight-segment vertices
    # - Flag 2: Curve-fit tangent vertices (carry tangent direction)
    # - Flag 8: Spline fit-points
    return True


class TraceBuilder(Sequence):
    """Sequence of 2D banded lines like polylines with start- and end width or
    curves with start- and end width.


    .. note::

        Accepts 3D input, but z-axis is ignored. The :class:`TraceBuilder` is a
        2D only object and uses only the :ref:`OCS` coordinates!

    """

    def __init__(self) -> None:
        self._traces: list[AbstractTrace] = []
        self.abs_tol = 1e-12

    def __len__(self):
        return len(self._traces)

    def __getitem__(self, item):
        return self._traces[item]

    def append(self, trace: AbstractTrace) -> None:
        """Append a new trace."""
        self._traces.append(trace)

    def faces(self) -> Iterable[Face]:
        """Yields all faces as 4-tuples of :class:`~ezdxf.math.Vec2` objects
        in :ref:`OCS`.
        """
        for trace in self._traces:
            yield from trace.faces()

    def faces_wcs(self, ocs: OCS, elevation: float) -> Iterable[Sequence[Vec3]]:
        """Yields all faces as 4-tuples of :class:`~ezdxf.math.Vec3` objects
        in :ref:`WCS`.
        """
        for face in self.faces():
            yield tuple(ocs.points_to_wcs(Vec3(v.x, v.y, elevation) for v in face))

    def polygons(self) -> Iterable[Polygon]:
        """Yields for each sub-trace a single polygon as sequence of
        :class:`~ezdxf.math.Vec2` objects in :ref:`OCS`.
        """
        for trace in self._traces:
            yield trace.polygon()

    def polygons_wcs(self, ocs: OCS, elevation: float) -> Iterable[Sequence[Vec3]]:
        """Yields for each sub-trace a single polygon as sequence of
        :class:`~ezdxf.math.Vec3` objects in :ref:`WCS`.
        """
        for trace in self._traces:
            yield tuple(
                ocs.points_to_wcs(Vec3(v.x, v.y, elevation) for v in trace.polygon())
            )

    def virtual_entities(
        self, dxftype="TRACE", dxfattribs=None, doc: Optional[Drawing] = None
    ) -> Iterable[Quadrilateral]:
        """Yields faces as SOLID, TRACE or 3DFACE entities with DXF attributes
        given in `dxfattribs`.

        If a document is given, the doc attribute of the new entities will be
        set and the new entities will be automatically added to the entity
        database of that document.

        .. note::

            The :class:`TraceBuilder` is a 2D only object and uses only the
            :ref:`OCS` coordinates!

        Args:
            dxftype: DXF type as string, "SOLID", "TRACE" or "3DFACE"
            dxfattribs: DXF attributes for SOLID, TRACE or 3DFACE entities
            doc: associated document

        """
        for trace in self._traces:
            yield from trace.virtual_entities(dxftype, dxfattribs, doc)

    def close(self):
        """Close multi traces by merging first and last trace, if linear traces."""
        traces = self._traces
        if len(traces) < 2:
            return
        if isinstance(traces[0], LinearTrace) and isinstance(traces[-1], LinearTrace):
            first = cast(LinearTrace, traces.pop(0))
            last = cast(LinearTrace, traces[-1])
            for point, start_width, end_width in first:
                last.add_station(point, start_width, end_width)

    @classmethod
    def from_polyline(cls, polyline: DXFGraphic, segments: int = 64) -> TraceBuilder:
        """
        Create a complete trace from a LWPOLYLINE or a 2D POLYLINE entity, the
        trace consist of multiple sub-traces if :term:`bulge` values are
        present. Uses only the :ref:`OCS` coordinates!

        Args:
            polyline: :class:`~ezdxf.entities.LWPolyline` or 2D
                :class:`~ezdxf.entities.Polyline`
            segments: count of segments for bulge approximation, given count is
                for a full circle, partial arcs have proportional less segments,
                but at least 3

        """
        dxftype = polyline.dxftype()
        if dxftype == "LWPOLYLINE":
            polyline = cast("LWPolyline", polyline)
            const_width = polyline.dxf.const_width
            points = []
            for x, y, start_width, end_width, bulge in polyline.lwpoints:
                location = Vec2(x, y)
                if const_width:
                    # This is AutoCAD behavior, BricsCAD uses const width
                    # only for missing width values.
                    start_width = const_width
                    end_width = const_width
                points.append((location, start_width, end_width, bulge))
            closed = polyline.closed
        elif dxftype == "POLYLINE":
            polyline = cast("Polyline", polyline)
            if not polyline.is_2d_polyline:
                raise TypeError("2D POLYLINE required")
            closed = polyline.is_closed
            default_start_width = polyline.dxf.default_start_width
            default_end_width = polyline.dxf.default_end_width

            filtered_vertices = [
                vertex for vertex in polyline.vertices
                if should_include_vertex_for_rendering(vertex.dxf.get("flags", 0))
            ]

            points = []
            for vertex in filtered_vertices:
                location = Vec2(vertex.dxf.location)
                if vertex.dxf.hasattr("start_width"):
                    start_width = vertex.dxf.start_width
                else:
                    start_width = default_start_width
                if vertex.dxf.hasattr("end_width"):
                    end_width = vertex.dxf.end_width
                else:
                    end_width = default_end_width
                bulge = vertex.dxf.bulge
                points.append((location, start_width, end_width, bulge))
        else:
            raise TypeError(f"Invalid DXF type {dxftype}")

        if closed and not points[0][0].isclose(points[-1][0]):
            # close polyline explicit
            points.append(points[0])

        trace = cls()
        store_bulge = 0.0
        store_start_width = 0.0
        store_end_width = 0.0
        store_point: UVec | None = None

        linear_trace = LinearTrace()
        for point, start_width, end_width, bulge in points:
            if store_bulge != 0.0:
                center, start_angle, end_angle, radius = bulge_to_arc(
                    store_point, point, store_bulge
                )
                if radius > 0:
                    arc = ConstructionArc(
                        center,
                        radius,
                        math.degrees(start_angle),
                        math.degrees(end_angle),
                        is_counter_clockwise=True,
                    )
                    if arc.start_point.isclose(point):
                        sw = store_end_width
                        ew = store_start_width
                    else:
                        ew = store_end_width
                        sw = store_start_width
                    trace.append(CurvedTrace.from_arc(arc, sw, ew, segments))
                store_bulge = 0.0

            if bulge != 0.0:  # arc from prev_point to point
                if linear_trace.is_started:
                    linear_trace.add_station(point, start_width, end_width)
                    trace.append(linear_trace)
                    linear_trace = LinearTrace()
                store_bulge = bulge
                store_start_width = start_width
                store_end_width = end_width
                store_point = point
                continue

            linear_trace.add_station(point, start_width, end_width)
        if linear_trace.is_started:
            trace.append(linear_trace)

        if closed and len(trace) > 1:
            # This is required for traces with multiple paths to create the correct
            # miter at the closing point. (only linear to linear trace).
            trace.close()
        return trace
