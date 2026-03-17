# Source: https://github.com/mapbox/earcut
# License: ISC License (MIT compatible)
#
# Copyright (c) 2016, Mapbox
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
# INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
# OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
# TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF
# THIS SOFTWARE.
#
# The Algorithm
# -------------
# The library implements a modified ear slicing algorithm, optimized by z-order
# curve hashing and extended to handle holes, twisted polygons, degeneracies and
# self-intersections in a way that doesn't guarantee correctness of triangulation,
# but attempts to always produce acceptable results for practical data.
#
# Translation to Python:
# Copyright (c) 2022, Manfred Moitzi
# License: MIT License
#
# Notes
# -----
# Exterior path (outer path) vertices are stored in counter-clockwise order
# Hole vertices are stored in clockwise order
# Vertex order will be maintained by the algorithm automatically.
# Boundary behavior for holes:
#   - holes outside the exterior path are ignored
#   - invalid result for holes partially extending beyond the exterior path
#   - invalid result for overlapping holes
#   - invalid result for holes in holes
# Very stable in all circumstances - DOES NOT CRASH!
#
# Steiner Point
# -------------
# https://en.wikipedia.org/wiki/Steiner_point_(computational_geometry)
# A Steiner point is a point that is not part of the input to a geometric
# optimization problem but is added during the solution of the problem, to
# create a better solution than would be possible from the original points
# alone.
# A Steiner point is defined as a hole with a single point!
#
from __future__ import annotations
from typing import Sequence, Optional, Protocol, TypeVar

import math


class Point(Protocol):
    x: float
    y: float


class Node:
    def __init__(self, i: int, point: Point) -> None:
        self.i: int = i

        # store source point for output
        self.point = point

        # vertex coordinates
        self.x: float = point.x
        self.y: float = point.y

        # previous and next vertex nodes in a polygon ring
        self.prev: Node = None  # type: ignore
        self.next: Node = None  # type: ignore

        # z-order curve value
        self.z: int = 0

        # previous and next nodes in z-order
        self.prev_z: Node = None  # type: ignore
        self.next_z: Node = None  # type: ignore

        # indicates whether this is a steiner point
        self.steiner: bool = False

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y


T = TypeVar("T", bound=Point)


def earcut(exterior: list[T], holes: list[list[T]]) -> list[Sequence[T]]:
    """Implements a modified ear slicing algorithm, optimized by z-order
    curve hashing and extended to handle holes, twisted polygons, degeneracies
    and self-intersections in a way that doesn't guarantee correctness of
    triangulation, but attempts to always produce acceptable results for
    practical data.

    Source: https://github.com/mapbox/earcut

    Args:
        exterior: outer path as list of points as objects which provide a
            `x`- and a `y`-attribute
        holes: list of holes, each hole is list of points, a hole with
            a single points is a Steiner point

    Returns:
        Returns a list of triangles, each triangle is a tuple of three points,
        the output points are the same objects as the input points.

    """
    # exterior points in counter-clockwise order
    outer_node: Node = linked_list(exterior, 0, ccw=True)
    triangles: list[Sequence[T]] = []

    if outer_node is None or outer_node.next is outer_node.prev:
        return triangles

    if len(holes) > 0:
        outer_node = eliminate_holes(holes, len(exterior), outer_node)

    min_x: float = 0.0
    min_y: float = 0.0
    inv_size: float = 0.0

    # if the shape is not too simple, we'll use z-order curve hash later
    # calculate polygon bbox
    if len(exterior) > 80:
        min_x = max_x = exterior[0].x
        min_y = max_y = exterior[0].y
        for point in exterior:
            x = point.x
            y = point.y
            min_x = min(min_x, x)
            min_y = min(min_y, y)
            max_x = max(max_x, x)
            max_y = max(max_y, y)

        # min_x, min_y and inv_size are later used to transform coords into
        # integers for z-order calculation
        inv_size = max(max_x - min_x, max_y - min_y)
        inv_size = 32767 / inv_size if inv_size != 0 else 0

    earcut_linked(outer_node, triangles, min_x, min_y, inv_size, 0)  # type: ignore
    return triangles


def linked_list(points: Sequence[Point], start: int, ccw: bool) -> Node:
    """Create a circular doubly linked list from polygon points in the specified
    winding order
    """
    last: Node = None  # type: ignore
    if ccw is (signed_area(points) < 0):
        for point in points:
            last = insert_node(start, point, last)
            start += 1
    else:
        end = start + len(points)
        for point in reversed(points):
            last = insert_node(end, point, last)
            end -= 1

    # open polygon: where the 1st vertex is not coincident with the last vertex
    if last and last == last.next:  # true equals
        remove_node(last)
        last = last.next
    return last


def signed_area(points: Sequence[Point]) -> float:
    s: float = 0.0
    if not len(points):
        return s
    prev = points[-1]
    for point in points:
        s += (point.x - prev.x) * (point.y + prev.y)
        prev = point
    # s < 0 is counter-clockwise
    # s > 0 is clockwise
    return s


def area(p: Node, q: Node, r: Node) -> float:
    """Returns signed area of a triangle"""
    return (q.y - p.y) * (r.x - q.x) - (q.x - p.x) * (r.y - q.y)


def is_valid_diagonal(a: Node, b: Node):
    """Check if a diagonal between two polygon nodes is valid (lies in polygon
    interior)
    """
    return (
        a.next.i != b.i
        and a.prev.i != b.i
        and not intersects_polygon(a, b)  # doesn't intersect other edges
        and (
            locally_inside(a, b)
            and locally_inside(b, a)
            and middle_inside(a, b)
            and (
                area(a.prev, a, b.prev) or area(a, b.prev, b)
            )  # does not create opposite-facing sectors
            or a == b  # true equals
            and area(a.prev, a, a.next) > 0
            and area(b.prev, b, b.next) > 0
        )  # special zero-length case
    )


def intersects_polygon(a: Node, b: Node) -> bool:
    """Check if a polygon diagonal intersects any polygon segments"""
    p = a
    while True:
        if (
            p.i != a.i
            and p.next.i != a.i
            and p.i != b.i
            and p.next.i != b.i
            and intersects(p, p.next, a, b)
        ):
            return True
        p = p.next
        if p is a:
            break
    return False


def sign(num: float) -> int:
    if num < 0.0:
        return -1
    if num > 0.0:
        return 1
    return 0


def on_segment(p: Node, q: Node, r: Node) -> bool:
    return max(p.x, r.x) >= q.x >= min(p.x, r.x) and max(p.y, r.y) >= q.y >= min(
        p.y, r.y
    )


def intersects(p1: Node, q1: Node, p2: Node, q2: Node) -> bool:
    """check if two segments intersect"""
    o1 = sign(area(p1, q1, p2))
    o2 = sign(area(p1, q1, q2))
    o3 = sign(area(p2, q2, p1))
    o4 = sign(area(p2, q2, q1))

    if o1 != o2 and o3 != o4:
        return True  # general case

    if o1 == 0 and on_segment(p1, p2, q1):
        return True  # p1, q1 and p2 are collinear and p2 lies on p1q1
    if o2 == 0 and on_segment(p1, q2, q1):
        return True  # p1, q1 and q2 are collinear and q2 lies on p1q1
    if o3 == 0 and on_segment(p2, p1, q2):
        return True  # p2, q2 and p1 are collinear and p1 lies on p2q2
    if o4 == 0 and on_segment(p2, q1, q2):
        return True  # p2, q2 and q1 are collinear and q1 lies on p2q2
    return False


def insert_node(i: int, point: Point, last: Node) -> Node:
    """create a node and optionally link it with previous one (in a circular
    doubly linked list)
    """
    p = Node(i, point)

    if last is None:
        p.prev = p
        p.next = p
    else:
        p.next = last.next
        p.prev = last
        last.next.prev = p
        last.next = p
    return p


def remove_node(p: Node) -> None:
    p.next.prev = p.prev
    p.prev.next = p.next

    if p.prev_z:
        p.prev_z.next_z = p.next_z
    if p.next_z:
        p.next_z.prev_z = p.prev_z


def eliminate_holes(
    holes: Sequence[Sequence[Point]], start: int, outer_node: Node
) -> Node:
    """link every hole into the outer loop, producing a single-ring polygon
    without holes
    """
    queue: list[Node] = []
    for hole in holes:
        if len(hole) < 1:  # skip empty holes
            continue
        # hole vertices in clockwise order
        _list = linked_list(hole, start, ccw=False)
        if _list is _list.next:
            _list.steiner = True
        start += len(hole)
        queue.append(get_leftmost(_list))
    queue.sort(key=lambda node: (node.x, node.y))

    # process holes from left to right
    for hole_ in queue:
        outer_node = eliminate_hole(hole_, outer_node)
    return outer_node


def eliminate_hole(hole: Node, outer_node: Node) -> Node:
    """Find a bridge between vertices that connects hole with an outer ring and
    link it
    """
    bridge = find_hole_bridge(hole, outer_node)
    if bridge is None:
        return outer_node

    bridge_reverse = split_polygon(bridge, hole)

    # filter collinear points around the cuts
    filter_points(bridge_reverse, bridge_reverse.next)
    return filter_points(bridge, bridge.next)


def filter_points(start: Node, end: Optional[Node] = None) -> Node:
    """eliminate colinear or duplicate points"""
    if start is None:
        return start
    if end is None:
        end = start

    p = start

    while True:
        again = False
        if not p.steiner and (
            p == p.next or area(p.prev, p, p.next) == 0  # true equals
        ):
            remove_node(p)
            p = end = p.prev
            if p is p.next:
                break
            again = True
        else:
            p = p.next
        if not (again or p is not end):
            break
    return end


# main ear slicing loop which triangulates a polygon (given as a linked list)
def earcut_linked(
    ear: Node,
    triangles: list[Sequence[Point]],
    min_x: float,
    min_y: float,
    inv_size: float,
    pass_: int,
) -> None:
    if ear is None:
        return

    # interlink polygon nodes in z-order
    if not pass_ and inv_size:
        index_curve(ear, min_x, min_y, inv_size)

    stop = ear

    # iterate through ears, slicing them one by one
    while ear.prev is not ear.next:
        prev = ear.prev
        next = ear.next

        _is_ear = (
            is_ear_hashed(ear, min_x, min_y, inv_size) if inv_size else is_ear(ear)
        )
        if _is_ear:
            # cut off the triangle
            triangles.append((prev.point, ear.point, next.point))
            remove_node(ear)

            # skipping the next vertex leads to less sliver triangles
            ear = next.next
            stop = next.next
            continue

        ear = next

        # if we looped through the whole remaining polygon and can't find any more ears
        if ear is stop:
            # try filtering points and slicing again
            if not pass_:
                earcut_linked(
                    filter_points(ear),
                    triangles,
                    min_x,
                    min_y,
                    inv_size,
                    1,
                )

            # if this didn't work, try curing all small self-intersections locally
            elif pass_ == 1:
                ear = cure_local_intersections(filter_points(ear), triangles)
                earcut_linked(ear, triangles, min_x, min_y, inv_size, 2)

            # as a last resort, try splitting the remaining polygon into two
            elif pass_ == 2:
                split_ear_cut(ear, triangles, min_x, min_y, inv_size)
            break


def is_ear(ear: Node) -> bool:
    """check whether a polygon node forms a valid ear with adjacent nodes"""
    a: Node = ear.prev
    b: Node = ear
    c: Node = ear.next

    if area(a, b, c) >= 0:
        return False  # reflex, can't be an ear

    # now make sure we don't have other points inside the potential ear
    ax = a.x
    bx = b.x
    cx = c.x
    ay = a.y
    by = b.y
    cy = c.y

    # triangle bbox; min & max are calculated like this for speed
    x0 = min(ax, bx, cx)
    x1 = max(ax, bx, cx)
    y0 = min(ay, by, cy)
    y1 = max(ay, by, cy)
    p: Node = c.next

    while p is not a:
        if (
            x0 <= p.x <= x1
            and y0 <= p.y <= y1
            and point_in_triangle(ax, ay, bx, by, cx, cy, p.x, p.y)
            and area(p.prev, p, p.next) >= 0
        ):
            return False
        p = p.next

    return True


def is_ear_hashed(ear: Node, min_x: float, min_y: float, inv_size: float):
    a: Node = ear.prev
    b: Node = ear
    c: Node = ear.next

    if area(a, b, c) >= 0:
        return False  # reflex, can't be an ear

    ax = a.x
    bx = b.x
    cx = c.x
    ay = a.y
    by = b.y
    cy = c.y

    # triangle bbox; min & max are calculated like this for speed
    x0 = min(ax, bx, cx)
    x1 = max(ax, bx, cx)
    y0 = min(ay, by, cy)
    y1 = max(ay, by, cy)

    # z-order range for the current triangle bbox;
    min_z = z_order(x0, y0, min_x, min_y, inv_size)
    max_z = z_order(x1, y1, min_x, min_y, inv_size)

    p: Node = ear.prev_z
    n: Node = ear.next_z

    # look for points inside the triangle in both directions
    while p and p.z >= min_z and n and n.z <= max_z:
        if (
            x0 <= p.x <= x1
            and y0 <= p.y <= y1
            and p is not a
            and p is not c
            and point_in_triangle(ax, ay, bx, by, cx, cy, p.x, p.y)
            and area(p.prev, p, p.next) >= 0
        ):
            return False
        p = p.prev_z

        if (
            x0 <= n.x <= x1
            and y0 <= n.y <= y1
            and n is not a
            and n is not c
            and point_in_triangle(ax, ay, bx, by, cx, cy, n.x, n.y)
            and area(n.prev, n, n.next) >= 0
        ):
            return False
        n = n.next_z

    # look for remaining points in decreasing z-order
    while p and p.z >= min_z:
        if (
            x0 <= p.x <= x1
            and y0 <= p.y <= y1
            and p is not a
            and p is not c
            and point_in_triangle(ax, ay, bx, by, cx, cy, p.x, p.y)
            and area(p.prev, p, p.next) >= 0
        ):
            return False
        p = p.prev_z

    # look for remaining points in increasing z-order
    while n and n.z <= max_z:
        if (
            x0 <= n.x <= x1
            and y0 <= n.y <= y1
            and n is not a
            and n is not c
            and point_in_triangle(ax, ay, bx, by, cx, cy, n.x, n.y)
            and area(n.prev, n, n.next) >= 0
        ):
            return False
        n = n.next_z
    return True


def get_leftmost(start: Node) -> Node:
    """Find the leftmost node of a polygon ring"""
    p = start
    leftmost = start
    while True:
        if p.x < leftmost.x or (p.x == leftmost.x and p.y < leftmost.y):
            leftmost = p
        p = p.next
        if p is start:
            break
    return leftmost


def point_in_triangle(
    ax: float,
    ay: float,
    bx: float,
    by: float,
    cx: float,
    cy: float,
    px: float,
    py: float,
) -> bool:
    """Check if a point lies within a convex triangle"""
    return (
        (cx - px) * (ay - py) >= (ax - px) * (cy - py)
        and (ax - px) * (by - py) >= (bx - px) * (ay - py)
        and (bx - px) * (cy - py) >= (cx - px) * (by - py)
    )


def sector_contains_sector(m: Node, p: Node):
    """Whether sector in vertex m contains sector in vertex p in the same
    coordinates.
    """
    return area(m.prev, m, p.prev) < 0 and area(p.next, m, m.next) < 0


def index_curve(start: Node, min_x: float, min_y: float, inv_size: float):
    """Interlink polygon nodes in z-order"""
    p = start
    while True:
        if p.z == 0:
            p.z = z_order(p.x, p.y, min_x, min_y, inv_size)
        p.prev_z = p.prev
        p.next_z = p.next
        p = p.next
        if p is start:
            break

    p.prev_z.next_z = None  # type: ignore
    p.prev_z = None  # type: ignore

    sort_linked(p)


def z_order(x0: float, y0: float, min_x: float, min_y: float, inv_size: float) -> int:
    """Z-order of a point given coords and inverse of the longer side of data
    bbox.
    """
    # coords are transformed into non-negative 15-bit integer range
    x = int((x0 - min_x) * inv_size)
    y = int((y0 - min_y) * inv_size)

    x = (x | (x << 8)) & 0x00FF00FF
    x = (x | (x << 4)) & 0x0F0F0F0F
    x = (x | (x << 2)) & 0x33333333
    x = (x | (x << 1)) & 0x55555555

    y = (y | (y << 8)) & 0x00FF00FF
    y = (y | (y << 4)) & 0x0F0F0F0F
    y = (y | (y << 2)) & 0x33333333
    y = (y | (y << 1)) & 0x55555555

    return x | (y << 1)


# Simon Tatham's linked list merge sort algorithm
# http://www.chiark.greenend.org.uk/~sgtatham/algorithms/listsort.html
def sort_linked(head: Node) -> Node:
    in_size = 1
    tail: Node
    while True:
        p = head
        head = None  # type: ignore
        tail = None  # type: ignore
        num_merges = 0
        while p:
            num_merges += 1
            q = p
            p_size = 0
            for i in range(in_size):
                p_size += 1
                q = q.next_z
                if not q:
                    break
            q_size = in_size
            while p_size > 0 or (q_size > 0 and q):
                if p_size != 0 and (q_size == 0 or not q or p.z <= q.z):
                    e = p
                    p = p.next_z
                    p_size -= 1
                else:
                    e = q
                    q = q.next_z
                    q_size -= 1

                if tail:
                    tail.next_z = e
                else:
                    head = e
                e.prev_z = tail
                tail = e
            p = q
        tail.next_z = None  # type: ignore
        in_size *= 2
        if num_merges <= 1:
            break
    return head


def split_polygon(a: Node, b: Node) -> Node:
    """Link two polygon vertices with a bridge.

    If the vertices belong to the same ring, it splits polygon into two.
    If one belongs to the outer ring and another to a hole, it merges it into a
    single ring.
    """
    a2 = Node(a.i, a.point)
    b2 = Node(b.i, b.point)
    an = a.next
    bp = b.prev

    a.next = b
    b.prev = a

    a2.next = an
    an.prev = a2

    b2.next = a2
    a2.prev = b2

    bp.next = b2
    b2.prev = bp

    return b2


# go through all polygon nodes and cure small local self-intersections
def cure_local_intersections(start: Node, triangles: list[Sequence[Point]]) -> Node:
    p = start
    while True:
        a = p.prev
        b = p.next.next

        if (
            not a == b  # true equals
            and intersects(a, p, p.next, b)
            and locally_inside(a, b)
            and locally_inside(b, a)
        ):
            triangles.append((a.point, p.point, b.point))
            # remove two nodes involved
            remove_node(p)
            remove_node(p.next)
            p = start = b

        p = p.next
        if p is start:
            break
    return filter_points(p)


def split_ear_cut(
    start: Node,
    triangles: list[Sequence[Point]],
    min_x: float,
    min_y: float,
    inv_size: float,
) -> None:
    """Try splitting polygon into two and triangulate them independently"""
    # look for a valid diagonal that divides the polygon into two
    a = start
    while True:
        b = a.next.next
        while b is not a.prev:
            if a.i != b.i and is_valid_diagonal(a, b):
                # split the polygon in two by the diagonal
                c = split_polygon(a, b)

                # filter colinear points around the cuts
                a = filter_points(a, a.next)
                c = filter_points(c, c.next)

                # run earcut on each half
                earcut_linked(a, triangles, min_x, min_y, inv_size, 0)
                earcut_linked(c, triangles, min_x, min_y, inv_size, 0)
                return
            b = b.next
        a = a.next
        if a is start:
            break


# David Eberly's algorithm for finding a bridge between hole and outer polygon
def find_hole_bridge(hole: Node, outer_node: Node) -> Node:
    p = outer_node
    hx = hole.x
    hy = hole.y
    qx = -math.inf
    m: Node = None  # type: ignore
    # find a segment intersected by a ray from the hole's leftmost point to the left;
    # segment's endpoint with lesser x will be potential connection point
    while True:
        if p.y >= hy >= p.next.y != p.y:
            x = p.x + (hy - p.y) * (p.next.x - p.x) / (p.next.y - p.y)
            if hx >= x > qx:
                qx = x
                m = p if p.x < p.next.x else p.next
                if x == hx:  # ??? use math.isclose
                    # hole touches outer segment; pick leftmost endpoint
                    return m
        p = p.next
        if p is outer_node:
            break

    if m is None:
        return None

    # look for points inside the triangle of hole point, segment intersection and endpoint;
    # if there are no points found, we have a valid connection;
    # otherwise choose the point of the minimum angle with the ray as connection point
    stop = m
    mx = m.x
    my = m.y
    tan_min = math.inf
    p = m

    while True:
        if (
            hx >= p.x >= mx
            and hx != p.x
            and point_in_triangle(
                hx if hy < my else qx,
                hy,
                mx,
                my,
                qx if hy < my else hx,
                hy,
                p.x,
                p.y,
            )
        ):
            tan = abs(hy - p.y) / (hx - p.x)  # tangential

            if locally_inside(p, hole) and (
                tan < tan_min
                or (
                    tan == tan_min
                    and (p.x > m.x or (p.x == m.x and sector_contains_sector(m, p)))
                )
            ):
                m = p
                tan_min = tan

        p = p.next
        if p is stop:
            break
    return m


def locally_inside(a: Node, b: Node) -> bool:
    """Check if a polygon diagonal is locally inside the polygon"""
    return (
        area(a, b, a.next) >= 0 and area(a, a.prev, b) >= 0
        if area(a.prev, a, a.next) < 0
        else area(a, b, a.prev) < 0 or area(a, a.next, b) < 0
    )


def middle_inside(a: Node, b: Node) -> bool:
    """Check if the middle point of a polygon diagonal is inside the polygon"""
    p = a
    inside = False
    px = (a.x + b.x) / 2
    py = (a.y + b.y) / 2
    while True:
        if (
            ((p.y > py) != (p.next.y > py))
            and p.next.y != p.y
            and (px < (p.next.x - p.x) * (py - p.y) / (p.next.y - p.y) + p.x)
        ):
            inside = not inside
        p = p.next
        if p is a:
            break
    return inside
