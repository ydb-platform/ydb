# License
# Copyright (c) 2011 Evan Wallace (http://madebyevan.com/), under the MIT license.
# Python port Copyright (c) 2012 Tim Knip (http://www.floorplanner.com), under the MIT license.
# Additions by Alex Pletzer (Pennsylvania State University)
# Integration as ezdxf add-on, Copyright (c) 2020, Manfred Moitzi, MIT License.
from __future__ import annotations
from typing import Optional
from ezdxf.math import Vec3, best_fit_normal, normal_vector_3p
from ezdxf.render import MeshVertexMerger, MeshBuilder, MeshTransformer

# Implementation Details
# ----------------------
#
# All CSG operations are implemented in terms of two functions, clip_to() and
# invert(), which remove parts of a BSP tree inside another BSP tree and swap
# solid and empty space, respectively. To find the union of a and b, we
# want to remove everything in a inside b and everything in b inside a,
# then combine polygons from a and b into one solid:
#
#     a.clip_to(b)
#     b.clip_to(a)
#     a.build(b.all_polygons())
#
# The only tricky part is handling overlapping coplanar polygons in both trees.
# The code above keeps both copies, but we need to keep them in one tree and
# remove them in the other tree. To remove them from b we can clip the
# inverse of b against a. The code for union now looks like this:
#
#     a.clip_to(b)
#     b.clip_to(a)
#     b.invert()
#     b.clip_to(a)
#     b.invert()
#     a.build(b.all_polygons())
#
# Subtraction and intersection naturally follow from set operations. If
# union is A | B, subtraction is A - B = ~(~A | B) and intersection is
# A & B = ~(~A | ~B) where '~' is the complement operator.

__all__ = ["CSG"]

COPLANAR = 0  # all the vertices are within EPSILON distance from plane
FRONT = 1  # all the vertices are in front of the plane
BACK = 2  # all the vertices are at the back of the plane
SPANNING = 3  # some vertices are in front, some in the back
PLANE_EPSILON = 1e-5  # Tolerance used by split_polygon() to decide if a point is on the plane.


class Plane:
    """Represents a plane in 3D space."""

    __slots__ = ("normal", "w")

    def __init__(self, normal: Vec3, w: float):
        self.normal = normal
        # w is the (perpendicular) distance of the plane from (0, 0, 0)
        self.w = w

    @classmethod
    def from_points(cls, a: Vec3, b: Vec3, c: Vec3) -> Plane:
        n = normal_vector_3p(a, b, c)
        return Plane(n, n.dot(a))

    def clone(self) -> Plane:
        return Plane(self.normal, self.w)

    def flip(self) -> None:
        self.normal = -self.normal
        self.w = -self.w

    def __repr__(self) -> str:
        return f"Plane({self.normal}, {self.w})"

    def split_polygon(
        self,
        polygon: "Polygon",
        coplanar_front: list[Polygon],
        coplanar_back: list[Polygon],
        front: list[Polygon],
        back: list[Polygon],
    ) -> None:
        """
        Split `polygon` by this plane if needed, then put the polygon or polygon
        fragments in the appropriate lists. Coplanar polygons go into either
        `coplanarFront` or `coplanarBack` depending on their orientation with
        respect to this plane. Polygons in front or in back of this plane go into
        either `front` or `back`
        """
        polygon_type = 0
        vertex_types = []
        vertices = polygon.vertices
        meshid = polygon.meshid  # mesh ID of the associated mesh

        # Classify each point as well as the entire polygon into one of four classes:
        # COPLANAR, FRONT, BACK, SPANNING = FRONT + BACK
        for vertex in vertices:
            distance = self.normal.dot(vertex) - self.w
            if distance < -PLANE_EPSILON:
                vertex_type = BACK
            elif distance > PLANE_EPSILON:
                vertex_type = FRONT
            else:
                vertex_type = COPLANAR
            polygon_type |= vertex_type
            vertex_types.append(vertex_type)

        # Put the polygon in the correct list, splitting it when necessary.
        if polygon_type == COPLANAR:
            if self.normal.dot(polygon.plane.normal) > 0:
                coplanar_front.append(polygon)
            else:
                coplanar_back.append(polygon)
        elif polygon_type == FRONT:
            front.append(polygon)
        elif polygon_type == BACK:
            back.append(polygon)
        elif polygon_type == SPANNING:
            front_vertices = []
            back_vertices = []
            len_vertices = len(vertices)
            for index in range(len_vertices):
                next_index = (index + 1) % len_vertices
                vertex_type = vertex_types[index]
                next_vertex_type = vertex_types[next_index]
                vertex = vertices[index]
                next_vertex = vertices[next_index]
                if vertex_type != BACK:  # FRONT or COPLANAR
                    front_vertices.append(vertex)
                if vertex_type != FRONT:  # BACK or COPLANAR
                    back_vertices.append(vertex)
                if (vertex_type | next_vertex_type) == SPANNING:
                    interpolation_weight = (
                        self.w - self.normal.dot(vertex)
                    ) / self.normal.dot(next_vertex - vertex)
                    plane_intersection_point = vertex.lerp(
                        next_vertex, interpolation_weight
                    )
                    front_vertices.append(plane_intersection_point)
                    back_vertices.append(plane_intersection_point)
            if len(front_vertices) >= 3:
                front.append(Polygon(front_vertices, meshid=meshid))
            if len(back_vertices) >= 3:
                back.append(Polygon(back_vertices, meshid=meshid))


class Polygon:
    """
    Represents a convex polygon. The vertices used to initialize a polygon must
    be coplanar and form a convex loop, the `meshid` argument associates a polygon
    to a mesh.

    Args:
        vertices: polygon vertices as :class:`Vec3` objects
        meshid: id associated mesh

    """

    __slots__ = ("vertices", "plane", "meshid")

    def __init__(self, vertices: list[Vec3], meshid: int = 0):
        self.vertices = vertices
        try:
            normal = normal_vector_3p(vertices[0], vertices[1], vertices[2])
        except ZeroDivisionError:  # got three colinear vertices
            normal = best_fit_normal(vertices)
        self.plane = Plane(normal, normal.dot(vertices[0]))
        # number of mesh, this polygon is associated to
        self.meshid = meshid

    def clone(self) -> Polygon:
        return Polygon(list(self.vertices), meshid=self.meshid)

    def flip(self) -> None:
        self.vertices.reverse()
        self.plane.flip()

    def __repr__(self) -> str:
        v = ", ".join(repr(v) for v in self.vertices)
        return f"Polygon([{v}], mesh={self.meshid})"


class BSPNode:
    """
    Holds a node in a BSP tree. A BSP tree is built from a collection of polygons
    by picking a polygon to split along. That polygon (and all other coplanar
    polygons) are added directly to that node and the other polygons are added to
    the front and/or back subtrees. This is not a leafy BSP tree since there is
    no distinction between internal and leaf nodes.
    """

    __slots__ = ("plane", "front", "back", "polygons")

    def __init__(self, polygons: Optional[list[Polygon]] = None):
        self.plane: Optional[Plane] = None
        self.front: Optional[BSPNode] = None
        self.back: Optional[BSPNode] = None
        self.polygons: list[Polygon] = []
        if polygons:
            self.build(polygons)

    def clone(self) -> BSPNode:
        node = BSPNode()
        if self.plane:
            node.plane = self.plane.clone()
        if self.front:
            node.front = self.front.clone()
        if self.back:
            node.back = self.back.clone()
        node.polygons = [p.clone() for p in self.polygons]
        return node

    def invert(self) -> None:
        """Convert solid space to empty space and empty space to solid space."""
        for poly in self.polygons:
            poly.flip()
        assert self.plane is not None
        self.plane.flip()
        if self.front:
            self.front.invert()
        if self.back:
            self.back.invert()
        self.front, self.back = self.back, self.front

    def clip_polygons(self, polygons: list[Polygon]) -> list[Polygon]:
        """Recursively remove all polygons in `polygons` that are inside this
        BSP tree.

        """
        if self.plane is None:
            return polygons[:]

        front: list[Polygon] = []
        back: list[Polygon] = []
        for polygon in polygons:
            self.plane.split_polygon(polygon, front, back, front, back)

        if self.front:
            front = self.front.clip_polygons(front)

        if self.back:
            back = self.back.clip_polygons(back)
        else:
            back = []

        front.extend(back)
        return front

    def clip_to(self, bsp: BSPNode) -> None:
        """Remove all polygons in this BSP tree that are inside the other BSP
        tree `bsp`.
        """
        self.polygons = bsp.clip_polygons(self.polygons)
        if self.front:
            self.front.clip_to(bsp)
        if self.back:
            self.back.clip_to(bsp)

    def all_polygons(self) -> list[Polygon]:
        """Return a list of all polygons in this BSP tree."""
        polygons = self.polygons[:]
        if self.front:
            polygons.extend(self.front.all_polygons())
        if self.back:
            polygons.extend(self.back.all_polygons())
        return polygons

    def build(self, polygons: list[Polygon]) -> None:
        """
        Build a BSP tree out of `polygons`. When called on an existing tree, the
        new polygons are filtered down to the bottom of the tree and become new
        nodes there. Each set of polygons is partitioned using the first polygon
        (no heuristic is used to pick a good split).
        """
        if len(polygons) == 0:
            return
        if self.plane is None:
            # do a wise choice and pick the first polygon as split-plane ;)
            self.plane = polygons[0].plane.clone()
        # add first polygon to this node
        self.polygons.append(polygons[0])
        front: list[Polygon] = []
        back: list[Polygon] = []
        # split all other polygons at the split plane
        for poly in polygons[1:]:
            # coplanar front and back polygons go into self.polygons
            self.plane.split_polygon(
                poly, self.polygons, self.polygons, front, back
            )
        # recursively build the BSP tree
        if len(front) > 0:
            if self.front is None:
                self.front = BSPNode()
            self.front.build(front)
        if len(back) > 0:
            if self.back is None:
                self.back = BSPNode()
            self.back.build(back)


class CSG:
    """
    Constructive Solid Geometry (CSG) is a modeling technique that uses Boolean
    operations like union and intersection to combine 3D solids. This class
    implements CSG operations on meshes.

    New 3D solids are created from :class:`~ezdxf.render.MeshBuilder` objects
    and results can be exported as :class:`~ezdxf.render.MeshTransformer` objects
    to `ezdxf` by method :meth:`mesh`.

    Args:
        mesh: :class:`ezdxf.render.MeshBuilder` or inherited object
        meshid: individual mesh ID to separate result meshes, ``0`` is default

    """

    def __init__(self, mesh: Optional[MeshBuilder] = None, meshid: int = 0):
        if mesh is None:
            self.polygons: list[Polygon] = []
        else:
            mesh_copy = mesh.copy()
            mesh_copy.normalize_faces()
            self.polygons = [
                Polygon(face, meshid) for face in mesh_copy.faces_as_vertices()
            ]

    @classmethod
    def from_polygons(cls, polygons: list[Polygon]) -> CSG:
        csg = CSG()
        csg.polygons = polygons
        return csg

    def mesh(self, meshid: int = 0) -> MeshTransformer:
        """
        Returns a :class:`ezdxf.render.MeshTransformer` object.

        Args:
             meshid: individual mesh ID, ``0`` is default

        """
        mesh = MeshVertexMerger()
        for face in self.polygons:
            if meshid == face.meshid:
                mesh.add_face(face.vertices)
        return MeshTransformer.from_builder(mesh)

    def clone(self) -> CSG:
        return self.from_polygons([p.clone() for p in self.polygons])

    def union(self, other: CSG) -> CSG:
        """
        Return a new CSG solid representing space in either this solid or in the
        solid `other`. Neither this solid nor the solid `other` are modified::

            A.union(B)

            +-------+            +-------+
            |       |            |       |
            |   A   |            |       |
            |    +--+----+   =   |       +----+
            +----+--+    |       +----+       |
                 |   B   |            |       |
                 |       |            |       |
                 +-------+            +-------+
        """
        a = BSPNode(self.clone().polygons)
        b = BSPNode(other.clone().polygons)
        a.clip_to(b)
        b.clip_to(a)
        b.invert()
        b.clip_to(a)
        b.invert()
        a.build(b.all_polygons())
        return CSG.from_polygons(a.all_polygons())

    __add__ = union

    def subtract(self, other: CSG) -> CSG:
        """
        Return a new CSG solid representing space in this solid but not in the
        solid `other`. Neither this solid nor the solid `other` are modified::

            A.subtract(B)

            +-------+            +-------+
            |       |            |       |
            |   A   |            |       |
            |    +--+----+   =   |    +--+
            +----+--+    |       +----+
                 |   B   |
                 |       |
                 +-------+
        """
        a = BSPNode(self.clone().polygons)
        b = BSPNode(other.clone().polygons)
        a.invert()
        a.clip_to(b)
        b.clip_to(a)
        b.invert()
        b.clip_to(a)
        b.invert()
        a.build(b.all_polygons())
        a.invert()
        return CSG.from_polygons(a.all_polygons())

    __sub__ = subtract

    def intersect(self, other: CSG) -> CSG:
        """
        Return a new CSG solid representing space both this solid and in the
        solid `other`. Neither this solid nor the solid `other` are modified::

            A.intersect(B)

            +-------+
            |       |
            |   A   |
            |    +--+----+   =   +--+
            +----+--+    |       +--+
                 |   B   |
                 |       |
                 +-------+
        """
        a = BSPNode(self.clone().polygons)
        b = BSPNode(other.clone().polygons)
        a.invert()
        b.clip_to(a)
        b.invert()
        a.clip_to(b)
        b.clip_to(a)
        a.build(b.all_polygons())
        a.invert()
        return CSG.from_polygons(a.all_polygons())

    __mul__ = intersect

    def inverse(self) -> CSG:
        """
        Return a new CSG solid with solid and empty space switched. This solid is
        not modified.
        """
        csg = self.clone()
        for p in csg.polygons:
            p.flip()
        return csg
