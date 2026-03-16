# Copyright (c) 2020-2025, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Sequence, Iterable, Optional, Iterator
from enum import IntEnum
import math
import numpy as np

from ezdxf.math import (
    Vec3,
    Vec2,
    Matrix44,
    X_AXIS,
    Y_AXIS,
    Z_AXIS,
    UVec,
)


__all__ = [
    "is_planar_face",
    "subdivide_face",
    "subdivide_ngons",
    "Plane",
    "PlaneLocationState",
    "normal_vector_3p",
    "safe_normal_vector",
    "distance_point_line_3d",
    "intersection_line_line_3d",
    "intersection_ray_polygon_3d",
    "intersection_line_polygon_3d",
    "basic_transformation",
    "best_fit_normal",
    "BarycentricCoordinates",
    "linear_vertex_spacing",
    "has_matrix_3d_stretching",
    "spherical_envelope",
    "inscribe_circle_tangent_length",
    "bending_angle",
    "split_polygon_by_plane",
    "is_face_normal_pointing_outwards",
    "is_vertex_order_ccw_3d",
]
PI2 = math.pi / 2.0


def is_planar_face(face: Sequence[Vec3], abs_tol=1e-9) -> bool:
    """Returns ``True`` if sequence of vectors is a planar face.

    Args:
         face: sequence of :class:`~ezdxf.math.Vec3` objects
         abs_tol: tolerance for normals check

    """
    if len(face) < 3:
        return False
    if len(face) == 3:
        return True
    first_normal = None
    for index in range(len(face) - 2):
        a, b, c = face[index : index + 3]
        try:
            normal = (b - a).cross(c - b).normalize()
        except ZeroDivisionError:  # colinear edge
            continue
        if first_normal is None:
            first_normal = normal
        elif not first_normal.isclose(normal, abs_tol=abs_tol):
            return False
    if first_normal is not None:
        return True
    return False


def subdivide_face(
    face: Sequence[Vec3], quads: bool = True
) -> Iterator[Sequence[Vec3]]:
    """Subdivides faces by subdividing edges and adding a center vertex.

    Args:
        face: a sequence of :class:`Vec3`
        quads: create quad faces if ``True`` else create triangles

    """
    if len(face) < 3:
        raise ValueError("3 or more vertices required.")

    len_face: int = len(face)
    mid_pos = Vec3.sum(face) / len_face
    subdiv_location: list[Vec3] = [
        face[i].lerp(face[(i + 1) % len_face]) for i in range(len_face)
    ]

    for index, vertex in enumerate(face):
        if quads:
            yield vertex, subdiv_location[index], mid_pos, subdiv_location[index - 1]
        else:
            yield subdiv_location[index - 1], vertex, mid_pos
            yield vertex, subdiv_location[index], mid_pos


def subdivide_ngons(
    faces: Iterable[Sequence[Vec3]],
    max_vertex_count=4,
) -> Iterator[Sequence[Vec3]]:
    """Subdivides faces into triangles by adding a center vertex.

    Args:
        faces: iterable of faces as sequence of :class:`Vec3`
        max_vertex_count: subdivide only ngons with more vertices

    """
    for face in faces:
        if len(face) <= max_vertex_count:
            yield Vec3.tuple(face)
        else:
            mid_pos = Vec3.sum(face) / len(face)
            for index, vertex in enumerate(face):
                yield face[index - 1], vertex, mid_pos


def normal_vector_3p(a: Vec3, b: Vec3, c: Vec3) -> Vec3:
    """Returns normal vector for 3 points, which is the normalized cross
    product for: :code:`a->b x a->c`.
    """
    return (b - a).cross(c - a).normalize()


def safe_normal_vector(vertices: Sequence[Vec3]) -> Vec3:
    """Safe function to detect the normal vector for a face or polygon defined
    by 3 or more `vertices`.

    """
    if len(vertices) < 3:
        raise ValueError("3 or more vertices required")
    a, b, c, *_ = vertices
    try:  # fast path
        return (b - a).cross(c - a).normalize()
    except ZeroDivisionError:  # safe path, can still raise ZeroDivisionError
        return best_fit_normal(vertices)


def best_fit_normal(vertices: Iterable[UVec]) -> Vec3:
    """Returns the "best fit" normal for a plane defined by three or more
    vertices. This function tolerates imperfect plane vertices. Safe function
    to detect the extrusion vector of flat arbitrary polygons.

    """
    # Source: https://gamemath.com/book/geomprims.html#plane_best_fit (9.5.3)
    _vertices = Vec3.list(vertices)
    if len(_vertices) < 3:
        raise ValueError("3 or more vertices required")
    first = _vertices[0]
    if not first.isclose(_vertices[-1]):
        _vertices.append(first)  # close polygon
    prev_x, prev_y, prev_z = first.xyz
    nx = 0.0
    ny = 0.0
    nz = 0.0
    for v in _vertices[1:]:
        x, y, z = v.xyz
        nx += (prev_z + z) * (prev_y - y)
        ny += (prev_x + x) * (prev_z - z)
        nz += (prev_y + y) * (prev_x - x)
        prev_x = x
        prev_y = y
        prev_z = z
    return Vec3(nx, ny, nz).normalize()


def distance_point_line_3d(point: Vec3, start: Vec3, end: Vec3) -> float:
    """Returns the normal distance from a `point` to a 3D line.

    Args:
        point: point to test
        start: start point of the 3D line
        end: end point of the 3D line

    """
    if start.isclose(end):
        raise ZeroDivisionError("Not a line.")
    v1 = point - start
    # point projected onto line start to end:
    v2 = (end - start).project(v1)
    # Pythagoras:
    diff = v1.magnitude_square - v2.magnitude_square
    if diff <= 0.0:
        # This should not happen (abs(v1) > abs(v2)), but floating point
        # imprecision at very small values makes it possible!
        return 0.0
    else:
        return math.sqrt(diff)


def intersection_line_line_3d(
    line1: Sequence[Vec3],
    line2: Sequence[Vec3],
    virtual: bool = True,
    abs_tol: float = 1e-10,
) -> Optional[Vec3]:
    """
    Returns the intersection point of two 3D lines, returns ``None`` if lines
    do not intersect.

    Args:
        line1: first line as tuple of two points as :class:`Vec3` objects
        line2: second line as tuple of two points as :class:`Vec3` objects
        virtual: ``True`` returns any intersection point, ``False`` returns only
            real intersection points
        abs_tol: absolute tolerance for comparisons

    """
    from ezdxf.math import intersection_ray_ray_3d, BoundingBox

    res = intersection_ray_ray_3d(line1, line2, abs_tol)
    if len(res) != 1:
        return None

    point = res[0]
    if virtual:
        return point
    if BoundingBox(line1).inside(point) and BoundingBox(line2).inside(point):
        return point
    return None


def basic_transformation(
    move: UVec = (0, 0, 0),
    scale: UVec = (1, 1, 1),
    z_rotation: float = 0,
) -> Matrix44:
    """Returns a combined transformation matrix for translation, scaling and
    rotation about the z-axis.

    Args:
        move: translation vector
        scale: x-, y- and z-axis scaling as float triplet, e.g. (2, 2, 1)
        z_rotation: rotation angle about the z-axis in radians

    """
    sx, sy, sz = Vec3(scale)
    m = Matrix44.scale(sx, sy, sz)
    if z_rotation:
        m *= Matrix44.z_rotate(z_rotation)
    translate = Vec3(move)
    if not translate.is_null:
        m *= Matrix44.translate(translate.x, translate.y, translate.z)
    return m


PLANE_EPSILON = 1e-9


class PlaneLocationState(IntEnum):
    COPLANAR = 0  # all the vertices are within the plane
    FRONT = 1  # all the vertices are in front of the plane
    BACK = 2  # all the vertices are at the back of the plane
    SPANNING = 3  # some vertices are in front, some in the back


class Plane:
    """Construction tool for 3D planes.

    Represents a plane in 3D space as a normal vector and the perpendicular
    distance from the origin.
    """

    __slots__ = ("_normal", "_distance_from_origin")

    def __init__(self, normal: Vec3, distance: float):
        assert normal.is_null is False, "invalid plane normal"
        self._normal = normal
        # the (perpendicular) distance of the plane from (0, 0, 0)
        self._distance_from_origin = distance

    @property
    def normal(self) -> Vec3:
        """Normal vector of the plane."""
        return self._normal

    @property
    def distance_from_origin(self) -> float:
        """The (perpendicular) distance of the plane from origin (0, 0, 0)."""
        return self._distance_from_origin

    @property
    def vector(self) -> Vec3:
        """Returns the location vector."""
        return self._normal * self._distance_from_origin

    @classmethod
    def from_3p(cls, a: Vec3, b: Vec3, c: Vec3) -> "Plane":
        """Returns a new plane from 3 points in space."""
        try:
            n = (b - a).cross(c - a).normalize()
        except ZeroDivisionError:
            raise ValueError("undefined plane: colinear vertices")
        return Plane(n, n.dot(a))

    @classmethod
    def from_vector(cls, vector: UVec) -> "Plane":
        """Returns a new plane from the given location vector."""
        v = Vec3(vector)
        try:
            return Plane(v.normalize(), v.magnitude)
        except ZeroDivisionError:
            raise ValueError("invalid NULL vector")

    def __copy__(self) -> "Plane":
        """Returns a copy of the plane."""
        return self.__class__(self._normal, self._distance_from_origin)

    copy = __copy__

    def __repr__(self):
        return f"Plane({repr(self._normal)}, {self._distance_from_origin})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Plane):
            return NotImplemented
        return self.vector == other.vector

    def signed_distance_to(self, v: Vec3) -> float:
        """Returns signed distance of vertex `v` to plane, if distance is > 0,
        `v` is in 'front' of plane, in direction of the normal vector, if
        distance is < 0, `v` is at the 'back' of the plane, in the opposite
        direction of the normal vector.

        """
        return self._normal.dot(v) - self._distance_from_origin

    def distance_to(self, v: Vec3) -> float:
        """Returns absolute (unsigned) distance of vertex `v` to plane."""
        return math.fabs(self.signed_distance_to(v))

    def is_coplanar_vertex(self, v: Vec3, abs_tol=1e-9) -> bool:
        """Returns ``True`` if vertex `v` is coplanar, distance from plane to
        vertex `v` is 0.
        """
        return self.distance_to(v) < abs_tol

    def is_coplanar_plane(self, p: "Plane", abs_tol=1e-9) -> bool:
        """Returns ``True`` if plane `p` is coplanar, normal vectors in same or
        opposite direction.
        """
        n_is_close = self._normal.isclose
        return n_is_close(p._normal, abs_tol=abs_tol) or n_is_close(
            -p._normal, abs_tol=abs_tol
        )

    def intersect_line(
        self, start: Vec3, end: Vec3, *, coplanar=True, abs_tol=PLANE_EPSILON
    ) -> Optional[Vec3]:
        """Returns the intersection point of the 3D line from `start` to `end`
        and this plane or ``None`` if there is no intersection. If the argument
        `coplanar` is ``False`` the start- or end point of the line are ignored
        as intersection points.

        """
        state0 = self.vertex_location_state(start, abs_tol)
        state1 = self.vertex_location_state(end, abs_tol)
        if state0 is state1:
            return None
        if not coplanar and (
            state0 is PlaneLocationState.COPLANAR
            or state1 is PlaneLocationState.COPLANAR
        ):
            return None
        n = self.normal
        weight = (self.distance_from_origin - n.dot(start)) / n.dot(end - start)
        return start.lerp(end, weight)

    def intersect_ray(self, origin: Vec3, direction: Vec3) -> Optional[Vec3]:
        """Returns the intersection point of the infinite 3D ray defined by
        `origin` and the `direction` vector and this plane or ``None`` if there
        is no intersection. A coplanar ray does not intersect the plane!

        """
        n = self.normal
        try:
            weight = (self.distance_from_origin - n.dot(origin)) / n.dot(direction)
        except ZeroDivisionError:
            return None
        return origin + (direction * weight)

    def vertex_location_state(
        self, vertex: Vec3, abs_tol=PLANE_EPSILON
    ) -> PlaneLocationState:
        """Returns the :class:`PlaneLocationState` of the given `vertex` in
        relative to this plane.

        """
        distance = self._normal.dot(vertex) - self._distance_from_origin
        if distance < -abs_tol:
            return PlaneLocationState.BACK
        elif distance > abs_tol:
            return PlaneLocationState.FRONT
        else:
            return PlaneLocationState.COPLANAR


def split_polygon_by_plane(
    polygon: Iterable[Vec3],
    plane: Plane,
    *,
    coplanar=True,
    abs_tol=PLANE_EPSILON,
) -> tuple[Sequence[Vec3], Sequence[Vec3]]:
    """Split a convex `polygon` by the given `plane`.

    Returns a tuple of front- and back vertices (front, back).
    Returns also coplanar polygons if the
    argument `coplanar` is ``True``, the coplanar vertices goes into either
    front or back depending on their orientation with respect to this plane.

    """
    polygon_type = PlaneLocationState.COPLANAR
    vertex_types: list[PlaneLocationState] = []
    front_vertices: list[Vec3] = []
    back_vertices: list[Vec3] = []
    vertices = list(polygon)
    w = plane.distance_from_origin
    normal = plane.normal

    # Classify each point as well as the entire polygon into one of four classes:
    # COPLANAR, FRONT, BACK, SPANNING = FRONT + BACK
    for vertex in vertices:
        vertex_type = plane.vertex_location_state(vertex, abs_tol)
        polygon_type |= vertex_type  # type: ignore
        vertex_types.append(vertex_type)

    # Put the polygon in the correct list, splitting it when necessary.
    if polygon_type == PlaneLocationState.COPLANAR:
        if coplanar:
            polygon_normal = best_fit_normal(vertices)
            if normal.dot(polygon_normal) > 0:
                front_vertices = vertices
            else:
                back_vertices = vertices
    elif polygon_type == PlaneLocationState.FRONT:
        front_vertices = vertices
    elif polygon_type == PlaneLocationState.BACK:
        back_vertices = vertices
    elif polygon_type == PlaneLocationState.SPANNING:
        len_vertices = len(vertices)
        for index in range(len_vertices):
            next_index = (index + 1) % len_vertices
            vertex_type = vertex_types[index]
            next_vertex_type = vertex_types[next_index]
            vertex = vertices[index]
            next_vertex = vertices[next_index]
            if vertex_type != PlaneLocationState.BACK:  # FRONT or COPLANAR
                front_vertices.append(vertex)
            if vertex_type != PlaneLocationState.FRONT:  # BACK or COPLANAR
                back_vertices.append(vertex)
            if (vertex_type | next_vertex_type) == PlaneLocationState.SPANNING:
                interpolation_weight = (w - normal.dot(vertex)) / normal.dot(
                    next_vertex - vertex
                )
                plane_intersection_point = vertex.lerp(
                    next_vertex, interpolation_weight
                )
                front_vertices.append(plane_intersection_point)
                back_vertices.append(plane_intersection_point)
        if len(front_vertices) < 3:
            front_vertices = []
        if len(back_vertices) < 3:
            back_vertices = []
    return tuple(front_vertices), tuple(back_vertices)


def intersection_line_polygon_3d(
    start: Vec3,
    end: Vec3,
    polygon: Iterable[Vec3],
    *,
    coplanar=True,
    boundary=True,
    abs_tol=PLANE_EPSILON,
) -> Optional[Vec3]:
    """Returns the intersection point of the 3D line form `start` to `end` and
    the given `polygon`.

    Args:
        start: start point of 3D line as :class:`Vec3`
        end: end point of 3D line as :class:`Vec3`
        polygon: 3D polygon as iterable of :class:`Vec3`
        coplanar: if ``True`` a coplanar start- or end point as intersection
            point is valid
        boundary: if ``True`` an intersection point at the polygon boundary line
            is valid
        abs_tol: absolute tolerance for comparisons

    """
    vertices = list(polygon)
    if len(vertices) < 3:
        raise ValueError("3 or more vertices required")
    try:
        normal = safe_normal_vector(vertices)
    except ZeroDivisionError:
        return None
    plane = Plane(normal, normal.dot(vertices[0]))
    ip = plane.intersect_line(start, end, coplanar=coplanar, abs_tol=abs_tol)
    if ip is None:
        return None
    return _is_intersection_point_inside_3d_polygon(
        ip, vertices, normal, boundary, abs_tol
    )


def intersection_ray_polygon_3d(
    origin: Vec3,
    direction: Vec3,
    polygon: Iterable[Vec3],
    *,
    boundary=True,
    abs_tol=PLANE_EPSILON,
) -> Optional[Vec3]:
    """Returns the intersection point of the infinite 3D ray defined by `origin`
    and the `direction` vector and the given `polygon`.

    Args:
        origin: origin point of the 3D ray as :class:`Vec3`
        direction: direction vector of the 3D ray as :class:`Vec3`
        polygon: 3D polygon as iterable of :class:`Vec3`
        boundary: if ``True`` intersection points at the polygon boundary line
            are valid
        abs_tol: absolute tolerance for comparisons

    """

    vertices = list(polygon)
    if len(vertices) < 3:
        raise ValueError("3 or more vertices required")
    try:
        normal = safe_normal_vector(vertices)
    except ZeroDivisionError:
        return None
    plane = Plane(normal, normal.dot(vertices[0]))
    ip = plane.intersect_ray(origin, direction)
    if ip is None:
        return None
    return _is_intersection_point_inside_3d_polygon(
        ip, vertices, normal, boundary, abs_tol
    )


def _is_intersection_point_inside_3d_polygon(
    ip: Vec3, vertices: list[Vec3], normal: Vec3, boundary: bool, abs_tol: float
):
    from ezdxf.math import is_point_in_polygon_2d, OCS

    ocs = OCS(normal)
    ocs_vertices = Vec2.list(ocs.points_from_wcs(vertices))
    state = is_point_in_polygon_2d(
        Vec2(ocs.from_wcs(ip)), ocs_vertices, abs_tol=abs_tol
    )
    if state > 0 or (boundary and state == 0):
        return ip
    return None


class BarycentricCoordinates:
    """Barycentric coordinate calculation.

    The arguments `a`, `b` and `c` are the cartesian coordinates of an arbitrary
    triangle in 3D space. The barycentric coordinates (b1, b2, b3) define the
    linear combination of `a`, `b` and `c` to represent the point `p`::

        p = a * b1 + b * b2 + c * b3

    This implementation returns the barycentric coordinates of the normal
    projection of `p` onto the plane defined by (a, b, c).

    These barycentric coordinates have some useful properties:

    - if all barycentric coordinates (b1, b2, b3) are in the range [0, 1], then
      the point `p` is inside the triangle (a, b, c)
    - if one of the coordinates is negative, the point `p` is outside the
      triangle
    - the sum of b1, b2 and b3 is always 1
    - the center of "mass" has the barycentric coordinates (1/3, 1/3, 1/3) =
      (a + b + c)/3

    """

    # Source: https://gamemath.com/book/geomprims.html#triangle_barycentric_space

    def __init__(self, a: UVec, b: UVec, c: UVec):
        self.a = Vec3(a)
        self.b = Vec3(b)
        self.c = Vec3(c)
        self._e1 = self.c - self.b
        self._e2 = self.a - self.c
        self._e3 = self.b - self.a
        e1xe2 = self._e1.cross(self._e2)
        self._n = e1xe2.normalize()
        self._denom = e1xe2.dot(self._n)
        if abs(self._denom) < 1e-9:
            raise ValueError("invalid triangle")

    def from_cartesian(self, p: UVec) -> Vec3:
        p = Vec3(p)
        n = self._n
        denom = self._denom
        d1 = p - self.a
        d2 = p - self.b
        d3 = p - self.c
        b1 = self._e1.cross(d3).dot(n) / denom
        b2 = self._e2.cross(d1).dot(n) / denom
        b3 = self._e3.cross(d2).dot(n) / denom
        return Vec3(b1, b2, b3)

    def to_cartesian(self, b: UVec) -> Vec3:
        b1, b2, b3 = Vec3(b).xyz
        return self.a * b1 + self.b * b2 + self.c * b3


def linear_vertex_spacing(start: Vec3, end: Vec3, count: int) -> list[Vec3]:
    """Returns `count` evenly spaced vertices from `start` to `end`."""
    if count <= 2:
        return [start, end]
    distance = end - start
    if distance.is_null:
        return [start] * count

    vertices = [start]
    step = distance.normalize(distance.magnitude / (count - 1))
    for _ in range(1, count - 1):
        start += step
        vertices.append(start)
    vertices.append(end)
    return vertices


def has_matrix_3d_stretching(m: Matrix44) -> bool:
    """Returns ``True`` if matrix `m` performs a non-uniform xyz-scaling.
    Uniform scaling is not stretching in this context.

    Does not check if the target system is a cartesian coordinate system, use the
    :class:`~ezdxf.math.Matrix44` property :attr:`~ezdxf.math.Matrix44.is_cartesian`
    for that.
    """
    ux_mag_sqr = m.transform_direction(X_AXIS).magnitude_square
    uy = m.transform_direction(Y_AXIS)
    uz = m.transform_direction(Z_AXIS)
    return not math.isclose(ux_mag_sqr, uy.magnitude_square) or not math.isclose(
        ux_mag_sqr, uz.magnitude_square
    )


def spherical_envelope(points: Sequence[UVec]) -> tuple[Vec3, float]:
    """Calculate the spherical envelope for the given points.  Returns the
    centroid (a.k.a. geometric center) and the radius of the enclosing sphere.

    .. note::

        The result does not represent the minimal bounding sphere!

    """
    centroid = Vec3.sum(points) / len(points)
    radius = max(centroid.distance(p) for p in points)
    return centroid, radius


def inscribe_circle_tangent_length(dir1: Vec3, dir2: Vec3, radius: float) -> float:
    """Returns the tangent length of an inscribe-circle of the given `radius`.
    The direction `dir1` and `dir2` define two intersection tangents,
    The tangent length is the distance from the intersection point of the
    tangents to the touching point on the inscribe-circle.

    """
    alpha = dir1.angle_between(dir2)
    beta = PI2 - (alpha / 2.0)
    if math.isclose(abs(beta), PI2):
        return 0.0
    return abs(math.tan(beta) * radius)


def bending_angle(dir1: Vec3, dir2: Vec3, normal=Z_AXIS) -> float:
    """Returns the bending angle from `dir1` to `dir2` in radians.

    The normal vector is required to detect the bending orientation,
    an angle > 0 bends to the "left" an angle < 0 bends to the "right".

    """
    angle = dir1.angle_between(dir2)
    nn = dir1.cross(dir2)
    if nn.isclose(normal) or nn.is_null:
        return angle
    elif nn.isclose(-normal):
        return -angle
    raise ValueError("invalid normal vector")


def any_vertex_inside_face(vertices: Sequence[Vec3]) -> Vec3:
    """Returns a vertex from the "inside" of  the given face."""
    # Triangulation is for concave shapes important!
    from ezdxf.math.triangulation import mapbox_earcut_3d

    it = mapbox_earcut_3d(vertices)
    return Vec3.sum(next(it)) / 3.0


def front_faces_intersect_face_normal(
    faces: Sequence[Sequence[Vec3]],
    face: Sequence[Vec3],
    *,
    abs_tol=PLANE_EPSILON,
) -> int:
    """Returns the count of intersections of the normal-vector of the given
    `face` with the `faces` in front of this `face`.

    A counter-clockwise vertex order is assumed!

    """

    def is_face_in_front_of_detector(vertices: Sequence[Vec3]) -> bool:
        if len(vertices) < 3:
            return False
        return any(detector_plane.signed_distance_to(v) > abs_tol for v in vertices)

    # face-normal for counter-clockwise vertex order
    face_normal = safe_normal_vector(face)
    origin = any_vertex_inside_face(face)
    detector_plane = Plane(face_normal, face_normal.dot(origin))

    # collect all faces with at least one vertex in front of the detection plane
    front_faces = (f for f in faces if is_face_in_front_of_detector(f))

    # The detector face is excluded by the
    # is_face_in_front_of_detector() function!

    intersection_points: set[Vec3] = set()
    for face in front_faces:
        ip = intersection_ray_polygon_3d(
            origin, face_normal, face, boundary=True, abs_tol=abs_tol
        )
        if ip is None:
            continue
        if detector_plane.signed_distance_to(ip) > abs_tol:
            # Only count unique intersections points, the ip could lie on an
            # edge (2 ips) or even a corner vertex (3 or more ips).
            intersection_points.add(ip.round(6))
    return len(intersection_points)


def is_face_normal_pointing_outwards(
    faces: Sequence[Sequence[Vec3]],
    face: Sequence[Vec3],
    *,
    abs_tol=PLANE_EPSILON,
) -> bool:
    """Returns ``True`` if the face-normal for the given `face` of a
    closed surface is pointing outwards. A counter-clockwise vertex order is
    assumed, for faces with clockwise vertex order the result is inverted,
    therefore ``False`` is pointing outwards.

    This function does not check if the `faces` are a closed surface.

    """
    return front_faces_intersect_face_normal(faces, face, abs_tol=abs_tol) % 2 == 0


def is_vertex_order_ccw_3d(vertices: list[Vec3], normal: Vec3) -> bool:
    """Returns ``True`` when the given 3D vertices have a counter-clockwise order around
    the given normal vector.

    Works for convex and concave shapes. Does not check or care if all vertices are
    located in a flat plane or if the normal vector is really perpendicular to the
    shape, but the result may be incorrect in that cases.

    Args:
        vertices (list): corner vertices of a flat shape (polygon)
        normal (Vec3): normal vector of the shape

    Raises:
        ValueError: input has less than 3 vertices

    """
    if len(vertices) < 3:
        raise ValueError("3 or more vertices required")

    def signed_area() -> float:
        # using the shoelace formula.
        polygon = np.array(vertices)
        if dom == 0:  # dominant X, use YZ plane
            x, y = polygon[:, 1], polygon[:, 2]
        elif dom == 1:  # dominant Y, use XZ plane
            x, y = polygon[:, 0], polygon[:, 2]
        else:  # dominant Z, use XY plane
            x, y = polygon[:, 0], polygon[:, 1]
        # returns twice the area, but only the sign is relevant for this use case
        return np.dot(x, np.roll(y, -1)) - np.dot(y, np.roll(x, -1))

    # The polygon is maybe concave, direct cross-product checks between
    # adjacent edges are unreliable. Instead, use a projected signed area method.
    # Find dominant axis:
    abs_axis = abs(normal.x), abs(normal.y), abs(normal.z)
    dom = abs_axis.index(max(abs_axis))

    ccw = signed_area() > 0
    dom_axis = normal[dom]
    return dom_axis > 0 if ccw else dom_axis < 0
