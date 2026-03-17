#  Copyright (c) 2021-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterable, Sequence, Optional, Iterator, Callable
from typing_extensions import Protocol
import math
import enum

from ezdxf.math import (
    Vec2,
    UVec,
    intersection_line_line_2d,
    is_point_in_polygon_2d,
    has_clockwise_orientation,
    point_to_line_relation,
    TOLERANCE,
    BoundingBox2d,
)
from ezdxf.tools import take2, pairwise


__all__ = [
    "greiner_hormann_union",
    "greiner_hormann_difference",
    "greiner_hormann_intersection",
    "Clipping",
    "ConvexClippingPolygon2d",
    "ConcaveClippingPolygon2d",
    "ClippingRect2d",
    "InvertedClippingPolygon2d",
    "CohenSutherlandLineClipping2d",
]


class Clipping(Protocol):
    def clip_polygon(self, polygon: Sequence[Vec2]) -> Sequence[Sequence[Vec2]]:
        """Returns the parts of the clipped polygon."""

    def clip_polyline(self, polyline: Sequence[Vec2]) -> Sequence[Sequence[Vec2]]:
        """Returns the parts of the clipped polyline."""

    def clip_line(self, start: Vec2, end: Vec2) -> Sequence[tuple[Vec2, Vec2]]:
        """Returns the parts of the clipped line."""

    def is_inside(self, point: Vec2) -> bool:
        """Returns ``True`` if `point` is inside the clipping path."""


def _clip_polyline(
    polyline: Sequence[Vec2],
    line_clipper: Callable[[Vec2, Vec2], Sequence[tuple[Vec2, Vec2]]],
    abs_tol: float,
) -> Sequence[Sequence[Vec2]]:
    """Returns the parts of the clipped polyline."""
    if len(polyline) < 2:
        return []
    result: list[Vec2] = []
    parts: list[list[Vec2]] = []
    next_start = polyline[0]
    for end in polyline[1:]:
        start = next_start
        next_start = end
        for clipped_line in line_clipper(start, end):
            if len(clipped_line) != 2:
                continue
            if result:
                clip_start, clip_end = clipped_line
                if result[-1].isclose(clip_start, abs_tol=abs_tol):
                    result.append(clip_end)
                    continue
                parts.append(result)
            result = list(clipped_line)
    if result:
        parts.append(result)
    return parts


class ConvexClippingPolygon2d:
    """The clipping path is an arbitrary convex 2D polygon."""

    def __init__(self, vertices: Iterable[Vec2], ccw_check=True, abs_tol=TOLERANCE):
        self.abs_tol = abs_tol
        clip = list(vertices)
        if len(clip) > 1:
            if clip[0].isclose(clip[-1], abs_tol=self.abs_tol):
                clip.pop()
        if len(clip) < 3:
            raise ValueError("more than 3 vertices as clipping polygon required")
        if ccw_check and has_clockwise_orientation(clip):
            clip.reverse()
        self._clipping_polygon: list[Vec2] = clip

    def clip_polyline(self, polyline: Sequence[Vec2]) -> Sequence[Sequence[Vec2]]:
        """Returns the parts of the clipped polyline."""
        return _clip_polyline(polyline, self.clip_line, abs_tol=self.abs_tol)

    def clip_line(self, start: Vec2, end: Vec2) -> Sequence[tuple[Vec2, Vec2]]:
        """Returns the parts of the clipped line."""

        def is_inside(point: Vec2) -> bool:
            # is point left of line:
            return (clip_end.x - clip_start.x) * (point.y - clip_start.y) - (
                clip_end.y - clip_start.y
            ) * (point.x - clip_start.x) >= 0.0

        def edge_intersection(default: Vec2) -> Vec2:
            ip = intersection_line_line_2d(
                (edge_start, edge_end), (clip_start, clip_end), abs_tol=self.abs_tol
            )
            if ip is None:
                return default
            return ip

        # The clipping polygon is always treated as a closed polyline!
        clip_start = self._clipping_polygon[-1]
        edge_start = start
        edge_end = end
        for clip_end in self._clipping_polygon:
            if is_inside(edge_start):
                if not is_inside(edge_end):
                    edge_end = edge_intersection(edge_end)
            elif is_inside(edge_end):
                if not is_inside(edge_start):
                    edge_start = edge_intersection(edge_start)
            else:
                return tuple()
            clip_start = clip_end
        return ((edge_start, edge_end),)

    def clip_polygon(self, polygon: Sequence[Vec2]) -> Sequence[Sequence[Vec2]]:
        """Returns the parts of the clipped polygon. A polygon is a closed polyline."""

        def is_inside(point: Vec2) -> bool:
            # is point left of line:
            return (clip_end.x - clip_start.x) * (point.y - clip_start.y) - (
                clip_end.y - clip_start.y
            ) * (point.x - clip_start.x) > 0.0

        def edge_intersection() -> None:
            ip = intersection_line_line_2d(
                (edge_start, edge_end), (clip_start, clip_end), abs_tol=self.abs_tol
            )
            if ip is not None:
                clipped.append(ip)

        # The clipping polygon is always treated as a closed polyline!
        clip_start = self._clipping_polygon[-1]
        clipped = list(polygon)
        for clip_end in self._clipping_polygon:
            # next clipping edge to test: clip_start -> clip_end
            if not clipped:  # no subject vertices left to test
                break

            vertices = clipped.copy()
            if len(vertices) > 1 and vertices[0].isclose(
                vertices[-1], abs_tol=self.abs_tol
            ):
                vertices.pop()

            clipped.clear()
            edge_start = vertices[-1]
            for edge_end in vertices:
                # next polygon edge to test: edge_start -> edge_end
                if is_inside(edge_end):
                    if not is_inside(edge_start):
                        edge_intersection()
                    clipped.append(edge_end)
                elif is_inside(edge_start):
                    edge_intersection()
                edge_start = edge_end
            clip_start = clip_end
        return (clipped,)

    def is_inside(self, point: Vec2) -> bool:
        """Returns ``True`` if `point` is inside the clipping polygon."""
        return is_point_in_polygon_2d(point, self._clipping_polygon) >= 0


class ClippingRect2d:
    """The clipping path is an axis-aligned rectangle, where all sides are parallel to
    the x- and y-axis.
    """

    def __init__(self, bottom_left: Vec2, top_right: Vec2, abs_tol=TOLERANCE):
        self.abs_tol = abs_tol
        self._bbox = BoundingBox2d((bottom_left, top_right))
        bottom_left = self._bbox.extmin
        top_right = self._bbox.extmax
        self._clipping_polygon = ConvexClippingPolygon2d(
            [
                bottom_left,
                Vec2(top_right.x, bottom_left.y),
                top_right,
                Vec2(bottom_left.x, top_right.y),
            ],
            ccw_check=False,
            abs_tol=self.abs_tol,
        )
        self._line_clipper = CohenSutherlandLineClipping2d(
            self._bbox.extmin, self._bbox.extmax
        )

    def clip_polygon(self, polygon: Sequence[Vec2]) -> Sequence[Sequence[Vec2]]:
        """Returns the parts of the clipped polygon. A polygon is a closed polyline."""
        return self._clipping_polygon.clip_polygon(polygon)

    def clip_polyline(self, polyline: Sequence[Vec2]) -> Sequence[Sequence[Vec2]]:
        """Returns the parts of the clipped polyline."""
        return _clip_polyline(polyline, self.clip_line, self.abs_tol)

    def clip_line(self, start: Vec2, end: Vec2) -> Sequence[tuple[Vec2, Vec2]]:
        """Returns the clipped line."""
        result = self._line_clipper.clip_line(start, end)
        if len(result) == 2:
            return (result,)  # type: ignore
        return tuple()

    def is_inside(self, point: Vec2) -> bool:
        """Returns ``True`` if `point` is inside the clipping rectangle."""
        return self._bbox.inside(point)

    def has_intersection(self, other: BoundingBox2d) -> bool:
        """Returns ``True`` if `other` bounding box intersects the clipping rectangle."""
        return self._bbox.has_intersection(other)


class ConcaveClippingPolygon2d:
    """The clipping path is an arbitrary concave 2D polygon."""

    def __init__(self, vertices: Iterable[Vec2], abs_tol=TOLERANCE):
        self.abs_tol = abs_tol
        clip = list(vertices)
        if len(clip) > 1:
            if clip[0].isclose(clip[-1], abs_tol=self.abs_tol):
                clip.pop()
        if len(clip) < 3:
            raise ValueError("more than 3 vertices as clipping polygon required")
        # open polygon; clockwise or counter-clockwise oriented vertices
        self._clipping_polygon = clip
        self._bbox = BoundingBox2d(clip)

    def is_inside(self, point: Vec2) -> bool:
        """Returns ``True`` if `point` is inside the clipping polygon."""
        if not self._bbox.inside(point):
            return False
        return (
            is_point_in_polygon_2d(point, self._clipping_polygon, abs_tol=self.abs_tol)
            >= 0
        )

    def clip_line(self, start: Vec2, end: Vec2) -> Sequence[tuple[Vec2, Vec2]]:
        """Returns the clipped line."""
        abs_tol = self.abs_tol
        line = (start, end)
        if not self._bbox.has_overlap(BoundingBox2d(line)):
            return tuple()

        intersections = polygon_line_intersections_2d(self._clipping_polygon, line)
        start_is_inside = is_point_in_polygon_2d(start, self._clipping_polygon) >= 0
        if len(intersections) == 0:
            if start_is_inside:
                return (line,)
            return tuple()
        end_is_inside = (
            is_point_in_polygon_2d(end, self._clipping_polygon, abs_tol=abs_tol) >= 0
        )
        if end_is_inside and not intersections[-1].isclose(end, abs_tol=abs_tol):
            # last inside-segment ends at end
            intersections.append(end)
        if start_is_inside and not intersections[0].isclose(start, abs_tol=abs_tol):
            # first inside-segment begins at start
            intersections.insert(0, start)

        # REMOVE duplicate intersection points at the beginning and the end -
        # these are caused by clipping at the connection point of two edges.
        # KEEP duplicate intersection points in between - these are caused by the
        # coincident edges of inverted clipping polygons.  These intersections points
        # are required for the inside/outside rule to work properly!
        if len(intersections) > 1 and intersections[0].isclose(
            intersections[1], abs_tol=abs_tol
        ):
            intersections.pop(0)
        if len(intersections) > 1 and intersections[-1].isclose(
            intersections[-2], abs_tol=abs_tol
        ):
            intersections.pop()

        if has_collinear_edge(self._clipping_polygon, start, end):
            # slow detection: doesn't work with inside/outside rule!
            # test if mid-point of intersection-segment is inside the polygon.
            # intersection-segment collinear with a polygon edge is inside!
            segments: list[tuple[Vec2, Vec2]] = []
            for a, b in pairwise(intersections):
                if a.isclose(b, abs_tol=abs_tol):  # ignore zero-length segments
                    continue
                if (
                    is_point_in_polygon_2d(
                        a.lerp(b), self._clipping_polygon, abs_tol=abs_tol
                    )
                    >= 0
                ):
                    segments.append((a, b))
            return segments

        # inside/outside rule
        # intersection segments:
        # (0, 1) outside (2, 3) outside (4, 5) ...
        return list(take2(intersections))

    def clip_polyline(self, polyline: Sequence[Vec2]) -> Sequence[Sequence[Vec2]]:
        """Returns the parts of the clipped polyline."""
        abs_tol = self.abs_tol
        segments: list[list[Vec2]] = []
        for start, end in pairwise(polyline):
            for a, b in self.clip_line(start, end):
                if segments:
                    last_seg = segments[-1]
                    if last_seg[-1].isclose(a, abs_tol=abs_tol):
                        last_seg.append(b)
                        continue
                segments.append([a, b])
        return segments

    def clip_polygon(self, polygon: Sequence[Vec2]) -> Sequence[Sequence[Vec2]]:
        """Returns the parts of the clipped polygon. A polygon is a closed polyline."""
        vertices = list(polygon)
        abs_tol = self.abs_tol
        if len(vertices) > 1:
            if vertices[0].isclose(vertices[-1], abs_tol=abs_tol):
                vertices.pop()
        if len(vertices) < 3:
            return tuple()
        polygon_box = BoundingBox2d(vertices)
        if not self._bbox.has_intersection(polygon_box):
            return tuple()  # polygons do not overlap
        result = clip_arbitrary_polygons(self._clipping_polygon, vertices)
        if len(result) == 0:
            is_outside = any(
                is_point_in_polygon_2d(v, self._clipping_polygon, abs_tol=abs_tol) < 0
                for v in vertices
            )
            if is_outside:
                return tuple()
            return (vertices,)
            # return (self._clipping_polygon.copy(),)
        return result


def clip_arbitrary_polygons(
    clipper: list[Vec2], subject: list[Vec2]
) -> Sequence[Sequence[Vec2]]:
    """Returns the parts of the clipped subject. Both polygons can be concave

    Args:
        clipper: clipping window closed polygon
        subject: closed polygon to clip

    """
    # Caching of gh_clipper is not possible, because both GHPolygons get modified!
    gh_clipper = GHPolygon.from_vec2(clipper)
    gh_subject = GHPolygon.from_vec2(subject)
    return gh_clipper.intersection(gh_subject)


def has_collinear_edge(polygon: list[Vec2], start: Vec2, end: Vec2) -> bool:
    """Returns ``True`` if `polygon` has any collinear edge to line `start->end`."""
    a = polygon[-1]
    rel_a = point_to_line_relation(a, start, end)
    for b in polygon:
        rel_b = point_to_line_relation(b, start, end)
        if rel_a == 0 and rel_b == 0:
            return True
        a = b
        rel_a = rel_b
    return False


def polygon_line_intersections_2d(
    polygon: list[Vec2], line: tuple[Vec2, Vec2], abs_tol: float = TOLERANCE
) -> list[Vec2]:
    """Returns all intersections of polygon with line.
    All intersections points are ordered from start to end of line.
    Start and end points are not included if not explicit intersection points.

    .. Note::

        Returns duplicate intersections points when the line intersects at
        the connection point of two polygon edges!

    """
    intersection_points: list[Vec2] = []
    start, end = line
    size = len(polygon)
    for index in range(size):
        a = polygon[index - 1]
        b = polygon[index]
        ip = intersection_line_line_2d((a, b), line, virtual=False, abs_tol=abs_tol)
        if ip is None:
            continue
        # Note: do not remove duplicate vertices, because inverted clipping polygons
        # have coincident clipping edges inside the clipping polygon! #1101
        if ip.isclose(a, abs_tol=abs_tol):
            a_prev = polygon[index - 2]
            rel_prev = point_to_line_relation(a_prev, start, end, abs_tol=abs_tol)
            rel_next = point_to_line_relation(b, start, end, abs_tol=abs_tol)
            if rel_prev == rel_next:
                continue
        # edge case: line intersects "exact" in point b
        elif ip.isclose(b, abs_tol=abs_tol):
            b_next = polygon[(index + 1) % size]
            rel_prev = point_to_line_relation(a, start, end, abs_tol=abs_tol)
            rel_next = point_to_line_relation(b_next, start, end, abs_tol=abs_tol)
            if rel_prev == rel_next:
                continue
        intersection_points.append(ip)

    intersection_points.sort(key=lambda ip: ip.distance(start))
    return intersection_points


class InvertedClippingPolygon2d(ConcaveClippingPolygon2d):
    """This class represents an inverted clipping path.  Everything between the inner
    polygon and the outer extents is considered as inside.  The inner clipping path is
    an arbitrary 2D polygon.

    .. Important::

        The `outer_bounds` must be larger than the content to clip to work correctly.

    """

    def __init__(
        self,
        inner_polygon: Iterable[Vec2],
        outer_bounds: BoundingBox2d,
        abs_tol=TOLERANCE,
    ):
        self.abs_tol = abs_tol
        clip = list(inner_polygon)
        if len(clip) > 1:
            if not clip[0].isclose(clip[-1], abs_tol=abs_tol):  # close inner_polygon
                clip.append(clip[0])
        if len(clip) < 4:
            raise ValueError("more than 3 vertices as clipping polygon required")
        # requirements for inner_polygon:
        # arbitrary polygon (convex or concave)
        # closed polygon (first vertex == last vertex)
        # clockwise or counter-clockwise oriented vertices
        self._clipping_polygon = make_inverted_clipping_polygon(
            clip, outer_bounds, abs_tol
        )
        self._bbox = outer_bounds


def make_inverted_clipping_polygon(
    inner_polygon: list[Vec2], outer_bounds: BoundingBox2d, abs_tol=TOLERANCE
) -> list[Vec2]:
    """Creates a closed inverted clipping polygon by connecting the inner polygon with
    the surrounding rectangle at their closest vertices.
    """
    assert outer_bounds.has_data is True
    inner_polygon = inner_polygon.copy()
    if inner_polygon[0].isclose(inner_polygon[-1], abs_tol=abs_tol):
        inner_polygon.pop()
    assert len(inner_polygon) > 2
    outer_rect = list(outer_bounds.rect_vertices())  # counter-clockwise
    outer_rect.reverse()  # clockwise
    ci, co = find_closest_vertices(inner_polygon, outer_rect)
    result = inner_polygon[ci:]
    result.extend(inner_polygon[: ci + 1])
    result.extend(outer_rect[co:])
    result.extend(outer_rect[: co + 1])
    result.append(result[0])
    return result


def find_closest_vertices(
    vertices0: list[Vec2], vertices1: list[Vec2]
) -> tuple[int, int]:
    """Returns the indices of the closest vertices of both lists."""
    min_dist = math.inf
    result: tuple[int, int] = 0, 0
    for i0, v0 in enumerate(vertices0):
        for i1, v1 in enumerate(vertices1):
            distance = v0.distance(v1)
            if distance < min_dist:
                min_dist = distance
                result = i0, i1
    return result


# Based on the paper "Efficient Clipping of Arbitrary Polygons" by
# Günther Greiner and Kai Hormann,
# ACM Transactions on Graphics 1998;17(2):71-83
# Available at: http://www.inf.usi.ch/hormann/papers/Greiner.1998.ECO.pdf


class _Node:
    def __init__(
        self,
        vtx: Vec2,
        alpha: float = 0.0,
        intersect=False,
        entry=True,
        checked=False,
    ):
        self.vtx = vtx

        # Reference to the next vertex of the polygon
        self.next: _Node = None  # type: ignore

        # Reference to the previous vertex of the polygon
        self.prev: _Node = None  # type: ignore

        # Reference to the corresponding intersection vertex in the other polygon
        self.neighbor: _Node = None  # type: ignore

        # True if intersection is an entry point, False if exit
        self.entry: bool = entry

        # Intersection point's relative distance from previous vertex
        self.alpha: float = alpha

        # True if vertex is an intersection
        self.intersect: bool = intersect

        # True if the vertex has been checked (last phase)
        self.checked: bool = checked

    def set_checked(self):
        self.checked = True
        if self.neighbor and not self.neighbor.checked:
            self.neighbor.set_checked()


class IntersectionError(Exception):
    pass


class GHPolygon:
    first: _Node = None  # type: ignore
    max_x: float = 1e6

    def add(self, node: _Node):
        """Add a polygon vertex node."""

        self.max_x = max(self.max_x, node.vtx.x)
        if self.first is None:
            self.first = node
            self.first.next = node
            self.first.prev = node
        else:  # insert as last node
            first = self.first
            last = first.prev
            first.prev = node
            node.next = first
            node.prev = last
            last.next = node

    @staticmethod
    def build(vertices: Iterable[UVec]) -> GHPolygon:
        """Build a new GHPolygon from an iterable of vertices."""
        return GHPolygon.from_vec2(Vec2.list(vertices))

    @staticmethod
    def from_vec2(vertices: Sequence[Vec2]) -> GHPolygon:
        """Build a new GHPolygon from an iterable of vertices."""
        polygon = GHPolygon()
        for v in vertices:
            polygon.add(_Node(v))
        return polygon

    @staticmethod
    def insert(vertex: _Node, start: _Node, end: _Node):
        """Insert and sort an intersection node.

        This function inserts an intersection node between two other
        start- and end node of an edge. The start and end node cannot be
        an intersection node (that is, they must be actual vertex nodes of
        the original polygon). If there are multiple intersection nodes
        between the start- and end node, then the new node is inserted
        based on its alpha value.
        """
        curr = start
        while curr != end and curr.alpha < vertex.alpha:
            curr = curr.next

        vertex.next = curr
        prev = curr.prev
        vertex.prev = prev
        prev.next = vertex
        curr.prev = vertex

    def __iter__(self) -> Iterator[_Node]:
        assert self.first is not None
        s = self.first
        while True:
            yield s
            s = s.next
            if s is self.first:
                return

    @property
    def first_intersect(self) -> Optional[_Node]:
        for v in self:
            if v.intersect and not v.checked:
                return v
        return None

    @property
    def points(self) -> list[Vec2]:
        points = [v.vtx for v in self]
        if not points[0].isclose(points[-1]):
            points.append(points[0])
        return points

    def unprocessed(self):
        for v in self:
            if v.intersect and not v.checked:
                return True
        return False

    def union(self, clip: GHPolygon) -> list[list[Vec2]]:
        return self.clip(clip, False, False)

    def intersection(self, clip: GHPolygon) -> list[list[Vec2]]:
        return self.clip(clip, True, True)

    def difference(self, clip: GHPolygon) -> list[list[Vec2]]:
        return self.clip(clip, False, True)

    # pylint: disable=too-many-branches
    def clip(self, clip: GHPolygon, s_entry, c_entry) -> list[list[Vec2]]:
        """Clip this polygon using another one as a clipper.

        This is where the algorithm is executed. It allows you to make
        a UNION, INTERSECT or DIFFERENCE operation between two polygons.

        Given two polygons A, B the following operations may be performed:

        A|B ... A OR B  (Union of A and B)
        A&B ... A AND B (Intersection of A and B)
        A\\B ... A - B
        B\\A ... B - A

        The entry records store the direction the algorithm should take when
        it arrives at that entry point in an intersection. Depending on the
        operation requested, the direction is set as follows for entry points
        (f=forward, b=backward; exit points are always set to the opposite):

              Entry
              A   B
              -----
        A|B   b   b
        A&B   f   f
        A\\B  b   f
        B\\A  f   b

        f = True, b = False when stored in the entry record
        """
        # Phase 1: Find intersections
        for subject_vertex in self:
            if not subject_vertex.intersect:
                for clipper_vertex in clip:
                    if not clipper_vertex.intersect:
                        ip, us, uc = line_intersection(
                            subject_vertex.vtx,
                            next_vertex_node(subject_vertex.next).vtx,
                            clipper_vertex.vtx,
                            next_vertex_node(clipper_vertex.next).vtx,
                        )
                        if ip is None:
                            continue
                        subject_node = _Node(ip, us, intersect=True, entry=False)
                        clipper_node = _Node(ip, uc, intersect=True, entry=False)
                        subject_node.neighbor = clipper_node
                        clipper_node.neighbor = subject_node

                        self.insert(
                            subject_node,
                            subject_vertex,
                            next_vertex_node(subject_vertex.next),
                        )
                        clip.insert(
                            clipper_node,
                            clipper_vertex,
                            next_vertex_node(clipper_vertex.next),
                        )

        # Phase 2: Identify entry/exit points
        s_entry ^= is_inside_polygon(self.first.vtx, clip)
        for subject_vertex in self:
            if subject_vertex.intersect:
                subject_vertex.entry = s_entry
                s_entry = not s_entry

        c_entry ^= is_inside_polygon(clip.first.vtx, self)
        for clipper_vertex in clip:
            if clipper_vertex.intersect:
                clipper_vertex.entry = c_entry
                c_entry = not c_entry

        # Phase 3: Construct clipped polygons
        clipped_polygons: list[list[Vec2]] = []
        while self.unprocessed():
            current: _Node = self.first_intersect  # type: ignore
            clipped: list[Vec2] = [current.vtx]
            while True:
                current.set_checked()
                if current.entry:
                    while True:
                        current = current.next
                        clipped.append(current.vtx)
                        if current.intersect:
                            break
                else:
                    while True:
                        current = current.prev
                        clipped.append(current.vtx)
                        if current.intersect:
                            break

                current = current.neighbor
                if current.checked:
                    break
            clipped_polygons.append(clipped)
        return clipped_polygons


def next_vertex_node(v: _Node) -> _Node:
    """Return the next non-intersecting vertex after the one specified."""
    c = v
    while c.intersect:
        c = c.next
    return c


def is_inside_polygon(vertex: Vec2, polygon: GHPolygon) -> bool:
    """Returns ``True`` if  `vertex` is inside `polygon`."""
    # Possible issue: are points on the boundary inside or outside the polygon?
    #  this version: inside
    return is_point_in_polygon_2d(vertex, polygon.points, abs_tol=TOLERANCE) >= 0


_ERROR = None, 0, 0


def line_intersection(
    s1: Vec2, s2: Vec2, c1: Vec2, c2: Vec2, tol: float = TOLERANCE
) -> tuple[Optional[Vec2], float, float]:
    """Returns the intersection point between two lines.

    This special implementation excludes the line end points as intersection
    points!

    Algorithm based on: http://paulbourke.net/geometry/lineline2d/
    """
    den = (c2.y - c1.y) * (s2.x - s1.x) - (c2.x - c1.x) * (s2.y - s1.y)
    if abs(den) < tol:
        return _ERROR
    us = ((c2.x - c1.x) * (s1.y - c1.y) - (c2.y - c1.y) * (s1.x - c1.x)) / den
    lwr = 0.0 + tol
    upr = 1.0 - tol
    # Line end points are excluded as intersection points:
    # us =~ 0.0; us =~ 1.0
    if not (lwr < us < upr):
        return _ERROR
    # uc =~ 0.0; uc =~ 1.0
    uc = ((s2.x - s1.x) * (s1.y - c1.y) - (s2.y - s1.y) * (s1.x - c1.x)) / den
    if lwr < uc < upr:
        return (
            Vec2(s1.x + us * (s2.x - s1.x), s1.y + us * (s2.y - s1.y)),
            us,
            uc,
        )
    return _ERROR


class BooleanOperation(enum.Enum):
    UNION = "union"
    DIFFERENCE = "difference"
    INTERSECTION = "intersection"


def greiner_hormann_intersection(
    p1: Iterable[UVec], p2: Iterable[UVec]
) -> list[list[Vec2]]:
    """Returns the INTERSECTION of polygon `p1` &  polygon `p2`.
    This algorithm works only for polygons with real intersection points
    and line end points on face edges are not considered as such intersection
    points!

    """
    return greiner_hormann(p1, p2, BooleanOperation.INTERSECTION)


def greiner_hormann_difference(
    p1: Iterable[UVec], p2: Iterable[UVec]
) -> list[list[Vec2]]:
    """Returns the DIFFERENCE of polygon `p1` - polygon `p2`.
    This algorithm works only for polygons with real intersection points
    and line end points on face edges are not considered as such intersection
    points!

    """
    return greiner_hormann(p1, p2, BooleanOperation.DIFFERENCE)


def greiner_hormann_union(p1: Iterable[UVec], p2: Iterable[UVec]) -> list[list[Vec2]]:
    """Returns the UNION of polygon `p1` | polygon `p2`.
    This algorithm works only for polygons with real intersection points
    and line end points on face edges are not considered as such intersection
    points!

    """
    return greiner_hormann(p1, p2, BooleanOperation.UNION)


def greiner_hormann(
    p1: Iterable[UVec], p2: Iterable[UVec], op: BooleanOperation
) -> list[list[Vec2]]:
    """Implements a 2d clipping function to perform 3 boolean operations:

    - UNION: p1 | p2 ... p1 OR p2
    - INTERSECTION: p1 & p2 ... p1 AND p2
    - DIFFERENCE: p1 \\ p2 ... p1 - p2

    Based on the paper "Efficient Clipping of Arbitrary Polygons" by
    Günther Greiner and Kai Hormann.
    This algorithm works only for polygons with real intersection points
    and line end points on face edges are not considered as such intersection
    points!

    """
    polygon1 = GHPolygon.build(p1)
    polygon2 = GHPolygon.build(p2)

    if op == BooleanOperation.UNION:
        return polygon1.union(polygon2)
    elif op == BooleanOperation.DIFFERENCE:
        return polygon1.difference(polygon2)
    elif op == BooleanOperation.INTERSECTION:
        return polygon1.intersection(polygon2)
    raise ValueError(f"unknown or unsupported boolean operation: {op}")


LEFT = 0x1
RIGHT = 0x2
BOTTOM = 0x4
TOP = 0x8


class CohenSutherlandLineClipping2d:
    """Cohen-Sutherland 2D line clipping algorithm, source:
    https://en.wikipedia.org/wiki/Cohen%E2%80%93Sutherland_algorithm

    Args:
        w_min: bottom-left corner of the clipping rectangle
        w_max: top-right corner of the clipping rectangle

    """

    __slots__ = ("x_min", "x_max", "y_min", "y_max")

    def __init__(self, w_min: Vec2, w_max: Vec2) -> None:
        self.x_min, self.y_min = w_min
        self.x_max, self.y_max = w_max

    def encode(self, x: float, y: float) -> int:
        code: int = 0
        if x < self.x_min:
            code |= LEFT
        elif x > self.x_max:
            code |= RIGHT
        if y < self.y_min:
            code |= BOTTOM
        elif y > self.y_max:
            code |= TOP
        return code

    def clip_line(self, p0: Vec2, p1: Vec2) -> Sequence[Vec2]:
        """Returns the clipped line part as tuple[Vec2, Vec2] or an empty tuple.

        Args:
            p0: start-point of the line to clip
            p1: end-point of the line to clip

        """
        x0, y0 = p0
        x1, y1 = p1
        code0 = self.encode(x0, y0)
        code1 = self.encode(x1, y1)
        x = x0
        y = y0
        while True:
            if not code0 | code1:  # ACCEPT
                # bitwise OR is 0: both points inside window; trivially accept and
                # exit loop:
                return Vec2(x0, y0), Vec2(x1, y1)
            if code0 & code1:  # REJECT
                # bitwise AND is not 0: both points share an outside zone (LEFT,
                # RIGHT, TOP, or BOTTOM), so both must be outside window;
                # exit loop
                return tuple()

            # failed both tests, so calculate the line segment to clip
            # from an outside point to an intersection with clip edge
            # At least one endpoint is outside the clip rectangle; pick it
            code = code1 if code1 > code0 else code0

            # Now find the intersection point;
            # use formulas:
            # slope = (y1 - y0) / (x1 - x0)
            # x = x0 + (1 / slope) * (ym - y0), where ym is y_min or y_max
            # y = y0 + slope * (xm - x0), where xm is x_min or x_max
            # No need to worry about divide-by-zero because, in each case, the
            # code bit being tested guarantees the denominator is non-zero
            if code & TOP:  # point is above the clip window
                x = x0 + (x1 - x0) * (self.y_max - y0) / (y1 - y0)
                y = self.y_max
            elif code & BOTTOM:  # point is below the clip window
                x = x0 + (x1 - x0) * (self.y_min - y0) / (y1 - y0)
                y = self.y_min
            elif code & RIGHT:  # point is to the right of clip window
                y = y0 + (y1 - y0) * (self.x_max - x0) / (x1 - x0)
                x = self.x_max
            elif code & LEFT:  # point is to the left of clip window
                y = y0 + (y1 - y0) * (self.x_min - x0) / (x1 - x0)
                x = self.x_min

            if code == code0:
                x0 = x
                y0 = y
                code0 = self.encode(x0, y0)
            else:
                x1 = x
                y1 = y
                code1 = self.encode(x1, y1)
