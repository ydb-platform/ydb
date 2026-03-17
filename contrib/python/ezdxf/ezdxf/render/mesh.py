# Copyright (c) 2018-2022 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Dict,
    Iterable,
    Iterator,
    NamedTuple,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from typing_extensions import TypeAlias
from ezdxf.math import (
    BoundingBox,
    Matrix44,
    NULLVEC,
    OCS,
    UCS,
    UVec,
    Vec3,
    area,
    is_planar_face,
    normal_vector_3p,
    safe_normal_vector,
    subdivide_face,
    subdivide_ngons,
)

if TYPE_CHECKING:
    from ezdxf.entities import Polyface, Polymesh, Mesh, Solid3d
    from ezdxf.eztypes import GenericLayoutType

T = TypeVar("T")


class EdgeStat(NamedTuple):
    """Named tuple of edge statistics."""

    count: int  # type: ignore
    balance: int


Face: TypeAlias = Sequence[int]
Edge: TypeAlias = Tuple[int, int]
EdgeStats: TypeAlias = Dict[Edge, EdgeStat]


class MeshBuilderError(Exception):
    pass


class NodeMergingError(MeshBuilderError):
    pass


class MultipleMeshesError(MeshBuilderError):
    pass


class DegeneratedPathError(MeshBuilderError):
    pass


class NonManifoldMeshError(MeshBuilderError):
    pass


def open_faces(faces: Iterable[Face]) -> Iterator[Face]:
    """Yields all faces with more than two vertices as open faces
    (first vertex index != last vertex index).
    """
    for face in faces:
        if len(face) < 3:
            continue
        if face[0] == face[-1]:
            yield face[:-1]
        else:
            yield face


def normalize_faces(
    faces: list[Sequence[int]],
    *,
    close=False,
) -> Iterator[Sequence[int]]:
    """Removes duplicated vertices and returns closed or open faces according
    the `close` argument. Returns only faces with at least 3 edges.
    """
    for face in open_faces(faces):
        new_face = [face[0]]
        for index in face[1:]:
            if new_face[-1] != index:
                new_face.append(index)
        if len(new_face) < 3:
            continue
        if close:
            new_face.append(new_face[0])
        yield tuple(new_face)


def all_edges(faces: Iterable[Face]) -> Iterator[Edge]:
    """Yields all face edges as int tuples."""
    for face in open_faces(faces):
        yield from face_edges(face)


def face_edges(face: Face) -> Iterable[Edge]:
    """Yields all edges of a single open face as int tuples."""
    size = len(face)
    for index in range(size):
        yield face[index], face[(index + 1) % size]


def get_edge_stats(faces: Iterable[Face]) -> EdgeStats:
    """Returns the edge statistics.

    The Edge statistic contains for each edge `(a, b)` the :class:`EdgeStat` as
    tuple `(count, balance)` where the vertex index `a` is always smaller than
    the vertex index `b`.

    The edge count is how often this edge is used in faces as `(a, b)` or
    `(b, a)` and the balance is the count of edge `(a, b)` minus the count of
    edge `(b, a)` and should be 0 in "healthy" closed surfaces.
    A balance not 0 indicates an error which may be double coincident faces or
    mixed face vertex orders.

    """
    new_edge = EdgeStat(0, 0)
    stats: EdgeStats = {}
    for a, b in all_edges(faces):
        edge = a, b
        orientation = +1
        if a > b:
            edge = b, a
            orientation = -1
        # for all edges: count should be 2 and balance should be 0
        count, balance = stats.get(edge, new_edge)
        stats[edge] = EdgeStat(count + 1, balance + orientation)
    return stats


def estimate_face_normals_direction(
    vertices: Sequence[Vec3], faces: Sequence[Face]
) -> float:
    """Returns the estimated face-normals direction as ``float`` value
    in the range [-1.0, 1.0] for a closed surface.

    This heuristic works well for simple convex hulls but struggles with
    more complex structures like a torus (doughnut).

    A counter-clockwise (ccw) vertex arrangement is assumed but a
    clockwise (cw) arrangement works too but the values are reversed.

    The closer the value to 1.0 (-1.0 for cw) the more likely all normals
    pointing outwards from the surface.

    The closer the value to -1.0 (1.0 for cw) the more likely all normals
    pointing inwards from the surface.

    """
    n_vertices = len(vertices)
    if n_vertices == 0:
        return 0.0

    mesh_centroid = Vec3.sum(vertices) / n_vertices
    count = 0
    direction_sum = 0.0
    for face in faces:
        if len(face) < 3:
            continue
        try:
            face_vertices = tuple(vertices[i] for i in face)
        except IndexError:
            continue
        face_centroid = Vec3.sum(face_vertices) / len(face)
        try:
            face_normal = normal_vector_3p(
                face_vertices[0], face_vertices[1], face_vertices[2]
            )
        except ZeroDivisionError:
            continue
        try:
            outward_vec = (face_centroid - mesh_centroid).normalize()
        except ZeroDivisionError:
            continue
        direction_sum += face_normal.dot(outward_vec)
        count += 1
    if count > 0:
        return direction_sum / count
    return 0.0


def flip_face_normals(
    faces: Sequence[Sequence[int]],
) -> Iterator[Sequence[int]]:
    for face in faces:
        yield tuple(reversed(face))


def volume6(a: Vec3, b: Vec3, c: Vec3) -> float:
    """
    Returns six times the volume of the tetrahedron determined by abc
    and the origin (0, 0, 0).
    The volume is positive if the origin is on the negative side of abc,
    where the positive side is determined by the right-hand--rule.
    So the volume is positive if the ccw normal to abc points outside the
    tetrahedron.

    This code is taken from chull.c; see "Computational Geometry in C."
    """
    return (
        a.z * (b.x * c.y - b.y * c.x)
        + a.y * (b.z * c.x - b.x * c.z)
        + a.x * (b.y * c.z - b.z * c.y)
    )


def area_3d(polygon: Sequence[Vec3]) -> float:
    try:
        ocs = OCS(safe_normal_vector(polygon))
    except ZeroDivisionError:
        return 0.0
    return area(ocs.points_from_wcs(polygon))


# Mesh Topology Analysis using the Euler Characteristic
# https://max-limper.de/publications/Euler/index.html


class MeshDiagnose:
    def __init__(self, mesh: MeshBuilder):
        self._mesh = mesh
        self._edge_stats: EdgeStats = {}
        self._bbox = BoundingBox()
        self._face_normals: list[Vec3] = []

    @property
    def vertices(self) -> Sequence[Vec3]:
        """Sequence of mesh vertices as :class:`~ezdxf.math.Vec3` instances"""
        return self._mesh.vertices

    @property
    def faces(self) -> Sequence[Face]:
        """Sequence of faces as ``Sequence[int]``"""
        return self._mesh.faces

    @property
    def face_normals(self) -> Sequence[Vec3]:
        """Returns all face normal vectors  as sequence. The ``NULLVEC``
        instance is used as normal vector for degenerated faces. (cached data)

        """
        if len(self._face_normals) == 0:
            self._face_normals = list(self._mesh.face_normals())
        return self._face_normals

    @property
    def bbox(self) -> BoundingBox:
        """Returns the :class:`~ezdxf.math.BoundingBox` of the mesh. (cached data)"""
        if not self._bbox.has_data:
            self._bbox = self._mesh.bbox()
        return self._bbox

    @property
    def n_vertices(self) -> int:
        """Returns the vertex count."""
        return len(self.vertices)

    @property
    def n_faces(self) -> int:
        """Returns the face count."""
        return len(self.faces)

    @property
    def n_edges(self) -> int:
        """Returns the unique edge count. (cached data)"""
        return len(self.edge_stats)

    @property
    def edge_stats(self) -> EdgeStats:
        """Returns the edge statistics as a ``dict``. The dict-key is the edge
        as tuple of two vertex indices `(a, b)` where `a` is always smaller than
        `b`. The dict-value is an :class:`EdgeStat` tuple of edge count and edge
        balance, see :class:`EdgeStat` for the definition of edge count and
        edge balance. (cached data)
        """
        if len(self._edge_stats) == 0:
            self._edge_stats = get_edge_stats(self.faces)
        return self._edge_stats

    @property
    def euler_characteristic(self) -> int:
        """Returns the Euler characteristic:
        https://en.wikipedia.org/wiki/Euler_characteristic

        This number is always 2 for convex polyhedra.
        """
        return self.n_vertices - self.n_edges + self.n_faces

    @property
    def is_edge_balance_broken(self) -> bool:
        """Returns ``True`` if the edge balance is broken, this indicates a
        topology error for closed surfaces. A non-broken edge balance reflects
        that each edge connects two faces, where the edge is clockwise oriented
        in the first face and counter-clockwise oriented in the second face.
        A broken edge balance indicates possible topology errors like mixed
        face vertex orientations or a non-manifold mesh where an edge connects
        more than two faces. (cached data)

        """
        return any(e.balance != 0 for e in self.edge_stats.values())

    @property
    def is_manifold(self) -> bool:
        """Returns ``True`` if all edges have an edge count < 3. (cached data)

        A non-manifold mesh has edges with 3 or more connected faces.

        """
        return all(edge.count < 3 for edge in self.edge_stats.values())

    @property
    def is_closed_surface(self) -> bool:
        """Returns ``True`` if the mesh has a closed surface.
        This method does not require a unified face orientation.
        If multiple separated meshes are present the state is only ``True`` if
        **all** meshes have a closed surface. (cached data)

        Returns ``False`` for non-manifold meshes.

        """
        return all(edge.count == 2 for edge in self.edge_stats.values())

    def total_edge_count(self) -> int:
        """Returns the total edge count of all faces, shared edges are counted
        separately for each face. In closed surfaces this count should be 2x
        the unique edge count :attr:`n_edges`. (cached data)
        """
        return sum(e.count for e in self.edge_stats.values())

    def unique_edges(self) -> Iterable[Edge]:
        """Yields the unique edges of the mesh as int 2-tuples. (cached data)"""
        return self.edge_stats.keys()

    def estimate_face_normals_direction(self) -> float:
        """Returns the estimated face-normals direction as ``float`` value
        in the range [-1.0, 1.0] for a closed surface.

        This heuristic works well for simple convex hulls but struggles with
        more complex structures like a torus (doughnut).

        A counter-clockwise (ccw) vertex arrangement for outward pointing faces
        is assumed but a clockwise (cw) arrangement works too but the return
        values are reversed.

        The closer the value to 1.0 (-1.0 for cw) the more likely all normals
        pointing outwards from the surface.

        The closer the value to -1.0 (1.0 for cw) the more likely all normals
        pointing inwards from the surface.

        There are no exact confidence values if all faces pointing outwards,
        here some examples for surfaces created by :mod:`ezdxf.render.forms`
        functions:

            - :func:`~ezdxf.render.forms.cube` returns 1.0
            - :func:`~ezdxf.render.forms.cylinder` returns 0.9992
            - :func:`~ezdxf.render.forms.sphere` returns 0.9994
            - :func:`~ezdxf.render.forms.cone` returns 0.9162
            - :func:`~ezdxf.render.forms.cylinder` with all hull faces pointing
              outwards but caps pointing inwards returns 0.7785 but the
              property :attr:`is_edge_balance_broken` returns ``True`` which
              indicates the mixed vertex orientation
            - and the estimation of 0.0469 for a :func:`~ezdxf.render.forms.torus`
              is barely usable

        """
        return estimate_face_normals_direction(self.vertices, self.faces)

    def has_non_planar_faces(self) -> bool:
        """Returns ``True`` if any face is non-planar."""
        return not all(is_planar_face(face) for face in self._mesh.faces_as_vertices())

    def volume(self) -> float:
        """Returns the volume of a closed surface or 0 otherwise.

        .. warning::

            The face vertices have to be in counter-clockwise order, this
            requirement is not checked by this method.

            The result is not correct for multiple separated meshes in a single
            MeshBuilder object!!!

        """
        if self.is_closed_surface:
            volume = 0.0
            for face in self._mesh.tessellation(3):
                volume += volume6(face[0], face[1], face[2])
            return volume / 6.0
        return 0.0

    def surface_area(self) -> float:
        """Returns the surface area."""
        v = self.vertices
        surface_area = 0.0
        for face in self._mesh.open_faces():
            polygon = [v[i] for i in face]
            surface_area += area_3d(polygon)
        return surface_area

    def centroid(self) -> Vec3:
        """Returns the centroid of all vertices. (center of mass)"""
        if self.n_vertices > 0:
            return Vec3.sum(self.vertices) / self.n_vertices
        return NULLVEC


class MeshBuilder:
    """A simple Mesh builder. Stores a list of vertices and a faces list where
    each face is a list of indices into the vertices list.

    The :meth:`render_mesh` method, renders the mesh into a DXF MESH entity.
    The MESH entity supports ngons in AutoCAD, ngons are polygons with more
    than 4 vertices.

    Can only create new meshes.

    """

    def __init__(self) -> None:
        self.vertices: list[Vec3] = []
        # face storage, each face is a tuple of vertex indices (v0, v1, v2, v3, ....)
        self.faces: list[Sequence[int]] = []

    def bbox(self) -> BoundingBox:
        """Returns the :class:`~ezdxf.math.BoundingBox` of the mesh."""
        return BoundingBox(self.vertices)

    def copy(self):
        """Returns a copy of mesh."""
        return self.from_builder(self)

    def diagnose(self) -> MeshDiagnose:
        """Returns the :class:`MeshDiagnose` object for this mesh."""
        return MeshDiagnose(self)

    def get_face_vertices(self, index: int) -> Sequence[Vec3]:
        """Returns the face `index` as sequence of :class:`~ezdxf.math.Vec3`
        objects.
        """
        vertices = self.vertices
        return tuple(vertices[vi] for vi in self.faces[index])

    def get_face_normal(self, index: int) -> Vec3:
        """Returns the normal vector of the face `index` as :class:`~ezdxf.math.Vec3`,
        returns the ``NULLVEC`` instance for degenerated  faces.
        """
        face = self.get_face_vertices(index)
        try:
            return safe_normal_vector(face)
        except (ValueError, ZeroDivisionError):
            return NULLVEC

    def face_normals(self) -> Iterator[Vec3]:
        """Yields all face normals, yields the ``NULLVEC`` instance for degenerated
        faces.
        """
        for face in self.faces_as_vertices():
            try:
                yield safe_normal_vector(face)
            except (ValueError, ZeroDivisionError):
                yield NULLVEC

    def faces_as_vertices(self) -> Iterator[list[Vec3]]:
        """Yields all faces as list of vertices."""
        v = self.vertices
        for face in self.faces:
            yield [v[index] for index in face]

    def open_faces(self) -> Iterator[Face]:
        """Yields all faces as sequence of integers where the first vertex
        is not coincident with the last vertex.
        """
        yield from open_faces(self.faces)

    def add_face(self, vertices: Iterable[UVec]) -> None:
        """Add a face as vertices list to the mesh. A face requires at least 3
        vertices, each vertex is a ``(x, y, z)`` tuple or
        :class:`~ezdxf.math.Vec3` object. The new vertex indices are stored as
        face in the :attr:`faces` list.

        Args:
            vertices: list of at least 3 vertices ``[(x1, y1, z1), (x2, y2, z2),
                (x3, y3, y3), ...]``

        """
        self.faces.append(self.add_vertices(vertices))

    def add_vertices(self, vertices: Iterable[UVec]) -> Face:
        """Add new vertices to the mesh, each vertex is a ``(x, y, z)`` tuple
        or a :class:`~ezdxf.math.Vec3` object, returns the indices of the
        `vertices` added to the :attr:`vertices` list.

        e.g. adding 4 vertices to an empty mesh, returns the indices
        ``(0, 1, 2, 3)``, adding additional 4 vertices returns the indices
        ``(4, 5, 6, 7)``.

        Args:
            vertices: list of vertices, vertex as ``(x, y, z)`` tuple or
                :class:`~ezdxf.math.Vec3` objects

        Returns:
            tuple: indices of the `vertices` added to the :attr:`vertices` list

        """
        start_index = len(self.vertices)
        self.vertices.extend(Vec3.generate(vertices))
        return tuple(range(start_index, len(self.vertices)))

    def add_mesh(
        self,
        vertices: Optional[list[Vec3]] = None,
        faces: Optional[list[Face]] = None,
        mesh=None,
    ) -> None:
        """Add another mesh to this mesh.

        A `mesh` can be a :class:`MeshBuilder`, :class:`MeshVertexMerger` or
        :class:`~ezdxf.entities.Mesh` object or requires the attributes
        :attr:`vertices` and :attr:`faces`.

        Args:
            vertices: list of vertices, a vertex is a ``(x, y, z)`` tuple or
                :class:`~ezdxf.math.Vec3` object
            faces: list of faces, a face is a list of vertex indices
            mesh: another mesh entity

        """
        if mesh is not None:
            vertices = Vec3.list(mesh.vertices)
            faces = mesh.faces

        if vertices is None:
            raise ValueError("Requires vertices or another mesh.")
        faces = faces or []
        indices = self.add_vertices(vertices)

        for face_vertices in open_faces(faces):
            self.faces.append(tuple(indices[vi] for vi in face_vertices))

    def render_mesh(
        self,
        layout: GenericLayoutType,
        dxfattribs=None,
        matrix: Optional[Matrix44] = None,
        ucs: Optional[UCS] = None,
    ):
        """Render mesh as :class:`~ezdxf.entities.Mesh` entity into `layout`.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            dxfattribs: dict of DXF attributes e.g. ``{'layer': 'mesh', 'color': 7}``
            matrix: transformation matrix of type :class:`~ezdxf.math.Matrix44`
            ucs: transform vertices by :class:`~ezdxf.math.UCS` to :ref:`WCS`

        """
        dxfattribs = dict(dxfattribs) if dxfattribs else {}
        vertices = self.vertices
        if matrix is not None:
            vertices = matrix.transform_vertices(vertices)
        if ucs is not None:
            vertices = ucs.points_to_wcs(vertices)  # type: ignore
        mesh = layout.add_mesh(dxfattribs=dxfattribs)
        with mesh.edit_data() as data:
            # data will be copied at setting in edit_data()
            # ignore edges and creases!
            data.vertices = list(vertices)
            data.faces = list(self.faces)
        return mesh

    def render_normals(
        self,
        layout: GenericLayoutType,
        length: float = 1,
        relative=True,
        dxfattribs=None,
    ):
        """Render face normals as :class:`~ezdxf.entities.Line` entities into
        `layout`, useful to check orientation of mesh faces.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            length: visual length of normal, use length < 0 to point normals in
                opposite direction
            relative: scale length relative to face size if ``True``
            dxfattribs: dict of DXF attributes e.g. ``{'layer': 'normals', 'color': 6}``

        """
        dxfattribs = dict(dxfattribs) if dxfattribs else {}
        for face in self.faces_as_vertices():
            count = len(face)
            if count < 3:
                continue
            center = Vec3.sum(face) / count
            try:
                n = safe_normal_vector(face)
            except ZeroDivisionError:
                continue
            if relative:
                _length = (face[0] - center).magnitude * length
            else:
                _length = length
            layout.add_line(center, center + n * _length, dxfattribs=dxfattribs)

    @classmethod
    def from_mesh(cls: Type[T], other: Union[MeshBuilder, Mesh]) -> T:
        """Create new mesh from other mesh as class method.

        Args:
            other: `mesh` of type :class:`MeshBuilder` and inherited or DXF
                :class:`~ezdxf.entities.Mesh` entity or any object providing
                attributes :attr:`vertices`, :attr:`edges` and :attr:`faces`.

        """
        # just copy properties
        mesh = cls()
        assert isinstance(mesh, MeshBuilder)
        mesh.add_mesh(mesh=other)
        return mesh  # type: ignore

    @classmethod
    def from_polyface(cls: Type[T], other: Union[Polymesh, Polyface]) -> T:
        """Create new mesh from a  :class:`~ezdxf.entities.Polyface` or
        :class:`~ezdxf.entities.Polymesh` object.

        """
        if other.dxftype() != "POLYLINE":
            raise TypeError(f"Unsupported DXF type: {other.dxftype()}")

        mesh = cls()
        assert isinstance(mesh, MeshBuilder)
        if other.is_poly_face_mesh:
            _, faces = other.indexed_faces()  # type: ignore
            for face in faces:
                mesh.add_face(face.points())
        elif other.is_polygon_mesh:
            vertices = other.get_mesh_vertex_cache()  # type: ignore
            for m in range(other.dxf.m_count - 1):
                for n in range(other.dxf.n_count - 1):
                    mesh.add_face(
                        (
                            vertices[m, n],
                            vertices[m, n + 1],
                            vertices[m + 1, n + 1],
                            vertices[m + 1, n],
                        )
                    )
        else:
            raise TypeError("Not a polymesh or polyface.")
        return mesh  # type: ignore

    def render_polyface(
        self,
        layout: GenericLayoutType,
        dxfattribs=None,
        matrix: Optional[Matrix44] = None,
        ucs: Optional[UCS] = None,
    ):
        """Render mesh as :class:`~ezdxf.entities.Polyface` entity into
        `layout`.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            dxfattribs: dict of DXF attributes e.g. ``{'layer': 'mesh', 'color': 7}``
            matrix: transformation matrix of type :class:`~ezdxf.math.Matrix44`
            ucs: transform vertices by :class:`~ezdxf.math.UCS` to :ref:`WCS`

        """
        dxfattribs = dict(dxfattribs) if dxfattribs else {}
        polyface = layout.add_polyface(dxfattribs=dxfattribs)
        t = MeshTransformer.from_builder(self)
        if matrix is not None:
            t.transform(matrix)
        if ucs is not None:
            t.transform(ucs.matrix)
        polyface.append_faces(
            t.tessellation(max_vertex_count=4),
            dxfattribs=dxfattribs,
        )
        return polyface

    def render_3dsolid(self, layout: GenericLayoutType, dxfattribs=None) -> Solid3d:
        """Render mesh as :class:`~ezdxf.entities.Solid3d` entity into `layout`.

        This is an **experimental** feature to create simple 3DSOLID entities from 
        polyhedrons.
        
        The method supports closed and open shells.  A 3DSOLID entity can contain 
        multiple shells.  Separate the meshes beforehand by the method 
        :meth:`separate_meshes` if required.  The normals vectors of all faces should 
        point outwards. Faces can have more than 3 vertices (ngons) but non-planar 
        faces and concave faces will cause problems in some CAD applications.  The 
        method :meth:`mesh_tesselation` can help to break down the faces into triangles.

        Requires a valid DXF document for `layout` and DXF version R2000 or newer.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            dxfattribs: dict of DXF attributes e.g. ``{'layer': 'mesh', 'color': 7}``
        
        Raises:
            DXFValueError: valid DXF document required, if :attr:`layout.doc` is ``None``
            DXFVersionError: invalid DXF version

        .. versionadded:: 1.2.0

        """
        from ezdxf.acis import api as acis

        dxfattribs = dict(dxfattribs) if dxfattribs else {}
        solid3d = layout.add_3dsolid(dxfattribs=dxfattribs)
        body = acis.body_from_mesh(self)
        acis.export_dxf(solid3d, [body])
        return solid3d

    def render_3dfaces(
        self,
        layout: GenericLayoutType,
        dxfattribs=None,
        matrix: Optional[Matrix44] = None,
        ucs: Optional[UCS] = None,
    ):
        """Render mesh as :class:`~ezdxf.entities.Face3d` entities into
        `layout`.

        Args:
            layout: :class:`~ezdxf.layouts.BaseLayout` object
            dxfattribs: dict of DXF attributes e.g. ``{'layer': 'mesh', 'color': 7}``
            matrix: transformation matrix of type :class:`~ezdxf.math.Matrix44`
            ucs: transform vertices by :class:`~ezdxf.math.UCS` to :ref:`WCS`

        """
        dxfattribs = dict(dxfattribs) if dxfattribs else {}
        t = MeshTransformer.from_builder(self)
        if matrix is not None:
            t.transform(matrix)
        if ucs is not None:
            t.transform(ucs.matrix)
        for face in t.tessellation(max_vertex_count=4):
            layout.add_3dface(face, dxfattribs=dxfattribs)

    @classmethod
    def from_builder(cls: Type[T], other: MeshBuilder) -> T:
        """Create new mesh from other mesh builder, faster than
        :meth:`from_mesh` but supports only :class:`MeshBuilder` and inherited
        classes.

        """
        # just copy properties
        mesh = cls()
        assert isinstance(mesh, MeshBuilder)

        # DO NOT COPY CACHES!
        mesh.vertices = list(other.vertices)
        mesh.faces = list(other.faces)
        return mesh  # type: ignore

    def merge_coplanar_faces(self, passes: int = 1) -> MeshTransformer:
        """Returns a new :class:`MeshBuilder` object with merged adjacent
        coplanar faces.

        The faces have to share at least two vertices and have to have the
        same clockwise or counter-clockwise vertex order.

        The current implementation is not very capable!

        """
        mesh = self
        for _ in range(passes):
            mesh = _merge_adjacent_coplanar_faces(mesh.vertices, mesh.faces)
        return MeshTransformer.from_builder(mesh)

    def subdivide(self, level: int = 1, quads=True) -> MeshTransformer:
        """Returns a new :class:`MeshTransformer` object with all faces subdivided.

        Args:
             level: subdivide levels from 1 to max of 5
             quads: create quad faces if ``True`` else create triangles
        """
        mesh = self
        level = min(int(level), 5)
        while level > 0:
            mesh = _subdivide(mesh, quads)
            level -= 1
        return MeshTransformer.from_builder(mesh)

    def optimize_vertices(self, precision: int = 6) -> MeshTransformer:
        """Returns a new mesh with optimized vertices. Coincident vertices are
        merged together and all faces are open faces (first vertex != last
        vertex). Uses internally the :class:`MeshVertexMerger` class to merge
        vertices.
        """
        mesh = MeshVertexMerger(precision=precision)
        mesh.add_mesh(mesh=self)
        return MeshTransformer.from_builder(mesh)

    def subdivide_ngons(self, max_vertex_count=4) -> Iterator[Sequence[Vec3]]:
        """Yields all faces as sequence of :class:`~ezdxf.math.Vec3` instances,
        where all ngons which have more than `max_vertex_count` vertices gets
        subdivided.
        In contrast to the :meth:`tessellation` method, creates this method a
        new vertex in the centroid of the face. This can create a more regular
        tessellation but only works reliable for convex faces!
        """
        yield from subdivide_ngons(self.faces_as_vertices(), max_vertex_count)

    def tessellation(self, max_vertex_count: int = 4) -> Iterator[Sequence[Vec3]]:
        """Yields all faces as sequence of :class:`~ezdxf.math.Vec3` instances,
        each face has no more vertices than the given `max_vertex_count`. This
        method uses the "ear clipping" algorithm which works with concave faces
        too and does not create any additional vertices.
        """
        from ezdxf.math.triangulation import mapbox_earcut_3d

        for face in self.faces_as_vertices():
            if len(face) <= max_vertex_count:
                yield face
            else:
                yield from mapbox_earcut_3d(face)

    def mesh_tessellation(self, max_vertex_count: int = 4) -> MeshTransformer:
        """Returns a new :class:`MeshTransformer` instance, where each face has
        no more vertices than the given `max_vertex_count`.

        The `fast` mode uses a shortcut for faces with less than 6 vertices
        which may not work for concave faces!
        """
        mesh = MeshVertexMerger()
        for face in self.tessellation(max_vertex_count=max_vertex_count):
            mesh.add_face(face)
        return MeshTransformer.from_builder(mesh)

    def flip_normals(self) -> None:
        """Flips the normals of all faces by reversing the vertex order inplace."""
        self.faces = list(flip_face_normals(self.faces))

    def separate_meshes(self) -> list[MeshTransformer]:
        """A single :class:`MeshBuilder` instance can store multiple separated
        meshes. This function returns this separated meshes as multiple
        :class:`MeshTransformer` instances.
        """
        return list(separate_meshes(self))

    def normalize_faces(self) -> None:
        """Removes duplicated vertex indices from faces and stores all faces as
        open faces, where the last vertex is not coincident with the first
        vertex.
        """
        self.faces = list(normalize_faces(self.faces, close=False))

    def face_orientation_detector(self, reference: int = 0) -> FaceOrientationDetector:
        """Returns a :class:`FaceOrientationDetector` or short `fod` instance.
        The forward orientation is defined by the `reference` face which is
        0 by default.

        The `fod` can check if all faces are reachable from the reference face
        and if all faces have the same orientation. The `fod` can be reused to
        unify the face orientation of the mesh.

        """
        return FaceOrientationDetector(self, reference=reference)

    def unify_face_normals(
        self, *, fod: Optional[FaceOrientationDetector] = None
    ) -> MeshTransformer:
        """Returns a new :class:`MeshTransformer` object with unified
        face normal vectors of all faces.
        The forward direction (not necessarily outwards) is defined by the
        face-normals of the majority of the faces.
        This function can not process non-manifold meshes (more than two faces
        are connected by a single edge) or multiple disconnected meshes in a
        single :class:`MeshBuilder` object.

        It is possible to pass in an existing :class:`FaceOrientationDetector`
        instance as argument `fod`.

        Raises:
            NonManifoldError: non-manifold mesh
            MultipleMeshesError: the :class:`MeshBuilder` object contains multiple disconnected meshes

        """
        return unify_face_normals_by_majority(self, fod=fod)

    def unify_face_normals_by_reference(
        self,
        reference: int = 0,
        *,
        force_outwards=False,
        fod: Optional[FaceOrientationDetector] = None,
    ) -> MeshTransformer:
        """Returns a new :class:`MeshTransformer` object with unified
        face normal vectors of all faces.
        The forward direction (not necessarily outwards) is defined by the
        reference face, which is the first face of the `mesh` by default.
        This function can not process non-manifold
        meshes (more than two faces are connected by a single edge) or multiple
        disconnected meshes in a single :class:`MeshBuilder` object.

        The outward direction of all face normals can be forced by stetting
        the argument `force_outwards` to ``True`` but this works only for closed
        surfaces, and it's time-consuming!

        It is not possible to check for a closed surface as long the face normal
        vectors are not unified. But it can be done afterward by the attribute
        :meth:`MeshDiagnose.is_closed_surface` to see if the result is
        trustworthy.

        It is possible to pass in an existing :class:`FaceOrientationDetector`
        instance as argument `fod`.

        Args:
            reference: index of the reference face
            force_outwards: forces face-normals to point outwards, this works
                only for closed surfaces, and it's time-consuming!
            fod: :class:`FaceOrientationDetector` instance

        Raises:
            ValueError: non-manifold mesh or the :class:`MeshBuilder` object
                contains multiple disconnected meshes

        """
        mesh = unify_face_normals_by_reference(self, reference=reference, fod=fod)
        if force_outwards:
            _force_face_normals_pointing_outwards(mesh, reference)
        return mesh


class MeshTransformer(MeshBuilder):
    """A mesh builder with inplace transformation support."""

    def transform(self, matrix: Matrix44):
        """Transform mesh inplace by applying the transformation `matrix`.

        Args:
            matrix: 4x4 transformation matrix as :class:`~ezdxf.math.Matrix44`
                object

        """
        self.vertices = list(matrix.transform_vertices(self.vertices))
        return self

    def translate(self, dx: Union[float, UVec] = 0, dy: float = 0, dz: float = 0):
        """Translate mesh inplace.

        Args:
            dx: translation in x-axis or translation vector
            dy: translation in y-axis
            dz: translation in z-axis

        """
        if isinstance(dx, (float, int)):
            t = Vec3(dx, dy, dz)
        else:
            t = Vec3(dx)
        self.vertices = [t + v for v in self.vertices]
        return self

    def scale(self, sx: float = 1, sy: float = 1, sz: float = 1):
        """Scale mesh inplace.

        Args:
            sx: scale factor for x-axis
            sy: scale factor for y-axis
            sz: scale factor for z-axis

        """
        self.vertices = [Vec3(x * sx, y * sy, z * sz) for x, y, z in self.vertices]
        return self

    def scale_uniform(self, s: float):
        """Scale mesh uniform inplace.

        Args:
            s: scale factor for x-, y- and z-axis

        """
        self.vertices = [v * s for v in self.vertices]
        return self

    def rotate_x(self, angle: float):
        """Rotate mesh around x-axis about `angle` inplace.

        Args:
            angle: rotation angle in radians

        """
        self.vertices = list(Matrix44.x_rotate(angle).transform_vertices(self.vertices))
        return self

    def rotate_y(self, angle: float):
        """Rotate mesh around y-axis about `angle` inplace.

        Args:
            angle: rotation angle in radians

        """
        self.vertices = list(Matrix44.y_rotate(angle).transform_vertices(self.vertices))
        return self

    def rotate_z(self, angle: float):
        """Rotate mesh around z-axis about `angle` inplace.

        Args:
            angle: rotation angle in radians

        """
        self.vertices = list(Matrix44.z_rotate(angle).transform_vertices(self.vertices))
        return self

    def rotate_axis(self, axis: UVec, angle: float):
        """Rotate mesh around an arbitrary axis located in the origin (0, 0, 0)
        about `angle`.

        Args:
            axis: rotation axis as Vec3
            angle: rotation angle in radians

        """
        self.vertices = list(
            Matrix44.axis_rotate(axis, angle).transform_vertices(self.vertices)
        )
        return self


def _subdivide(mesh, quads=True) -> MeshVertexMerger:
    """Returns a new :class:`MeshVertexMerger` object with subdivided faces
    and edges.

    Args:
         quads: create quad faces if ``True`` else create triangles

    """
    new_mesh = MeshVertexMerger()
    for vertices in mesh.faces_as_vertices():
        if len(vertices) < 3:
            continue
        for face in subdivide_face(vertices, quads):
            new_mesh.add_face(face)
    return new_mesh


class MeshVertexMerger(MeshBuilder):
    """Subclass of :class:`MeshBuilder`

    Mesh with unique vertices and no doublets, but needs extra memory for
    bookkeeping.

    :class:`MeshVertexMerger` creates a key for every vertex by rounding its
    components by the Python :func:`round` function and a given `precision`
    value. Each vertex with the same key gets the same vertex index, which is
    the index of first vertex with this key, so all vertices with the same key
    will be located at the location of this first vertex. If you want an average
    location of all vertices with the same key use the
    :class:`MeshAverageVertexMerger` class.

    Args:
        precision: floating point precision for vertex rounding

    """

    # can not support vertex transformation
    def __init__(self, precision: int = 6):
        """
        Args:
            precision: floating point precision for vertex rounding

        """
        super().__init__()
        self.ledger: dict[Vec3, int] = {}
        self.precision: int = precision

    def add_vertices(self, vertices: Iterable[UVec]) -> Face:
        """Add new `vertices` only, if no vertex with identical (x, y, z)
        coordinates already exist, else the index of the existing vertex is
        returned as index of the added vertices.

        Args:
            vertices: list of vertices, vertex as (x, y, z) tuple or
                :class:`~ezdxf.math.Vec3` objects

        Returns:
            indices of the added `vertices`

        """
        indices = []
        precision = self.precision
        for vertex in Vec3.generate(vertices):
            key = vertex.round(precision)
            try:
                indices.append(self.ledger[key])
            except KeyError:
                index = len(self.vertices)
                self.vertices.append(vertex)
                self.ledger[key] = index
                indices.append(index)
        return tuple(indices)

    def index(self, vertex: UVec) -> int:
        """Get index of `vertex`, raises :class:`IndexError` if not found.

        Args:
            vertex: ``(x, y, z)`` tuple or :class:`~ezdxf.math.Vec3` object

        (internal API)
        """
        try:
            return self.ledger[Vec3(vertex).round(self.precision)]
        except KeyError:
            raise IndexError(f"Vertex {str(vertex)} not found.")

    @classmethod
    def from_builder(cls, other: MeshBuilder) -> MeshVertexMerger:
        """Create new mesh from other mesh builder."""
        # rebuild from scratch to create a valid ledger
        return cls.from_mesh(other)


class MeshAverageVertexMerger(MeshBuilder):
    """Subclass of :class:`MeshBuilder`

    Mesh with unique vertices and no doublets, but needs extra memory for
    bookkeeping and runtime for calculation of average vertex location.

    :class:`MeshAverageVertexMerger` creates a key for every vertex by rounding
    its components by the Python :func:`round` function and a given `precision`
    value. Each vertex with the same key gets the same vertex index, which is the
    index of first vertex with this key, the difference to the
    :class:`MeshVertexMerger` class is the calculation of the average location
    for all vertices with the same key, this needs extra memory to keep track of
    the count of vertices for each key and extra runtime for updating the vertex
    location each time a vertex with an existing key is added.

    Args:
        precision: floating point precision for vertex rounding

    """

    # can not support vertex transformation
    def __init__(self, precision: int = 6):
        super().__init__()
        self.ledger: dict[
            Vec3, tuple[int, int]
        ] = {}  # each key points to a tuple (vertex index, vertex count)
        self.precision: int = precision

    def add_vertices(self, vertices: Iterable[UVec]) -> Face:
        """Add new `vertices` only, if no vertex with identical ``(x, y, z)``
        coordinates already exist, else the index of the existing vertex is
        returned as index of the added vertices.

        Args:
            vertices: list of vertices, vertex as ``(x, y, z)`` tuple or
            :class:`~ezdxf.math.Vec3` objects

        Returns:
            tuple: indices of the `vertices` added to the
            :attr:`~MeshBuilder.vertices` list

        """
        indices = []
        precision = self.precision
        for vertex in Vec3.generate(vertices):
            key = vertex.round(precision)
            try:
                index, count = self.ledger[key]
            except KeyError:  # new key
                index = len(self.vertices)
                self.vertices.append(vertex)
                self.ledger[key] = (index, 1)
            else:  # update key entry
                # calculate new average location
                average = (self.vertices[index] * count) + vertex
                count += 1
                # update vertex location
                self.vertices[index] = average / count
                # update ledger
                self.ledger[key] = (index, count)
            indices.append(index)
        return tuple(indices)

    def index(self, vertex: UVec) -> int:
        """Get index of `vertex`, raises :class:`IndexError` if not found.

        Args:
            vertex: ``(x, y, z)`` tuple or :class:`~ezdxf.math.Vec3` object

        (internal API)
        """
        try:
            return self.ledger[Vec3(vertex).round(self.precision)][0]
        except KeyError:
            raise IndexError(f"Vertex {str(vertex)} not found.")

    @classmethod
    def from_builder(cls, other: MeshBuilder) -> MeshAverageVertexMerger:
        """Create new mesh from other mesh builder."""
        # rebuild from scratch to create a valid ledger
        return cls.from_mesh(other)


class _XFace:
    __slots__ = ("fingerprint", "indices", "_orientation")

    def __init__(self, indices: Face):
        self.fingerprint: int = hash(indices)
        self.indices: Face = indices
        self._orientation: Vec3 = VEC3_SENTINEL

    def orientation(self, vertices: Sequence[Vec3], precision: int = 4) -> Vec3:
        if self._orientation is VEC3_SENTINEL:
            orientation = NULLVEC
            v0, v1, *v = [vertices[i] for i in self.indices]
            for v2 in v:
                try:
                    orientation = normal_vector_3p(v0, v1, v2).round(precision)
                    break
                except ZeroDivisionError:
                    continue
            self._orientation = orientation
        return self._orientation


def _merge_adjacent_coplanar_faces(
    vertices: list[Vec3], faces: list[Face], precision: int = 4
) -> MeshVertexMerger:
    oriented_faces: dict[Vec3, list[_XFace]] = {}
    extended_faces: list[_XFace] = []
    for face in faces:
        if len(face) < 3:
            raise ValueError("found invalid face count < 3")
        xface = _XFace(face)
        extended_faces.append(xface)
        oriented_faces.setdefault(xface.orientation(vertices, precision), []).append(
            xface
        )

    mesh = MeshVertexMerger()
    done = set()
    for xface in extended_faces:
        if xface.fingerprint in done:
            continue
        done.add(xface.fingerprint)
        face = xface.indices
        orientation = xface.orientation(vertices, precision)
        parallel_faces = oriented_faces[orientation]
        face_set = set(face)
        for parallel_face in parallel_faces:
            if parallel_face.fingerprint in done:
                continue
            common_vertices = face_set.intersection(set(parallel_face.indices))
            # connection by at least 2 vertices required:
            if len(common_vertices) > 1:
                if len(common_vertices) == len(parallel_face.indices):
                    face = merge_full_patch(face, parallel_face.indices)
                else:
                    try:
                        face = merge_connected_paths(face, parallel_face.indices)
                    except (NodeMergingError, DegeneratedPathError):
                        continue
                done.add(parallel_face.fingerprint)
                face_set = set(face)
        v0 = list(remove_colinear_face_vertices([vertices[i] for i in face]))
        mesh.add_face(v0)
    return mesh


VEC3_SENTINEL = Vec3(0, 0, 0)


def remove_colinear_face_vertices(vertices: Sequence[Vec3]) -> Iterator[Vec3]:
    def get_direction(v1: Vec3, v2: Vec3):
        return (v2 - v1).normalize()

    if len(vertices) < 3:
        yield from vertices
        return

    # remove duplicated vertices
    _vertices: list[Vec3] = [vertices[0]]
    for v in vertices[1:]:
        if not v.isclose(_vertices[-1]):
            _vertices.append(v)

    if len(_vertices) < 3:
        if len(_vertices) == 1:
            _vertices.append(_vertices[0])
        yield from _vertices
        return

    start = _vertices[0]
    prev_vertex = VEC3_SENTINEL
    current_direction = VEC3_SENTINEL
    start_index = 0

    # find start direction
    yield start
    while current_direction is VEC3_SENTINEL:
        start_index += 1
        try:
            prev_vertex = vertices[start_index]
        except IndexError:
            yield prev_vertex
            return
        current_direction = get_direction(start, prev_vertex)

    yielded_anything = False
    _vertices.append(start)
    for vertex in _vertices[start_index:]:
        try:
            if get_direction(start, vertex).isclose(current_direction):
                prev_vertex = vertex
                continue
        except ZeroDivisionError:
            continue
        yield prev_vertex
        yielded_anything = True
        start = prev_vertex
        current_direction = get_direction(start, vertex)
        prev_vertex = vertex

    if not yielded_anything:
        yield _vertices[-2]  # last vertex


def merge_connected_paths(p1: Sequence[int], p2: Sequence[int]) -> Sequence[int]:
    def build_nodes(p: Sequence[int]):
        nodes = {e1: e2 for e1, e2 in zip(p, p[1:])}
        nodes[p[-1]] = p[0]
        return nodes

    current_path = build_nodes(p1)
    other_path = build_nodes(p2)
    current_node = p1[0]
    finish = p1[0]
    connected_path = [current_node]
    while True:
        try:
            next_node = current_path[current_node]
        except KeyError:
            raise NodeMergingError
        if next_node in other_path:
            current_path, other_path = other_path, current_path
        if next_node == finish:
            break
        current_node = next_node
        if current_node in connected_path:
            # node duplication is an error, e.g. two path are only connected
            # by one node:
            raise NodeMergingError
        connected_path.append(current_node)

    if len(connected_path) < 3:
        raise DegeneratedPathError
    return connected_path


def merge_full_patch(path: Sequence[int], patch: Sequence[int]):
    count = len(path)
    new_path = []
    for pos, node in enumerate(path):
        prev = path[pos - 1]
        succ = path[(pos + 1) % count]
        if prev in patch and succ in patch:
            continue
        new_path.append(node)
    return new_path


class Lump:
    def __init__(self, face: Face):
        self.edges: set[Edge] = set()
        self.faces: list[Face] = [face]
        for a, b in face_edges(face):
            # sort vertex indices to guarantee: edge a,b == edge b,a
            self.edges.add((a, b) if a <= b else (b, a))

    def is_connected(self, other: Lump) -> bool:
        return not self.edges.isdisjoint(other.edges)

    def merge(self, other: Lump):
        self.faces.extend(other.faces)
        self.edges.update(other.edges)


def merge_lumps(lumps: Iterable[Lump]) -> list[Lump]:
    merged_lumps = _merge_lumps(lumps)
    prior_len = 0
    while 1 < len(merged_lumps) != prior_len:
        prior_len = len(merged_lumps)
        merged_lumps = _merge_lumps(merged_lumps)
    return merged_lumps


def _merge_lumps(lumps: Iterable[Lump]) -> list[Lump]:
    merged_lumps: list[Lump] = []
    for lump in lumps:
        for base in merged_lumps:
            if lump.is_connected(base):
                base.merge(lump)
                break
        else:
            merged_lumps.append(lump)
    return merged_lumps


def separate_meshes(m: MeshBuilder) -> Iterator[MeshTransformer]:
    """Returns the separated meshes in a single :class:`MeshBuilder` instance
    as multiple :class:`MeshTransformer` instances.
    """
    # At the beginning each face is a separated lump and all connected faces
    # should be merged:
    disconnected_lumps = list(merge_lumps(Lump(face) for face in open_faces(m.faces)))
    if len(disconnected_lumps) > 1:
        vertices = m.vertices
        # create new separated meshes:
        for lump in disconnected_lumps:
            mesh = MeshVertexMerger()
            for face in lump.faces:
                mesh.add_face(vertices[i] for i in face)
            yield MeshTransformer.from_builder(mesh)
    else:
        yield MeshTransformer.from_builder(m)


def have_away_pointing_normals(f0: Sequence[Vec3], f1: Sequence[Vec3]) -> bool:
    """Returns ``True`` if the normals of the two faces are pointing
    away from the center of the two faces.

    .. warning::

        This does not work for any arbitrary face pair!

    """
    c0 = Vec3.sum(f0) / len(f0)
    c1 = Vec3.sum(f1) / len(f1)
    n0 = normal_vector_3p(f0[0], f0[1], f0[2]) * 0.5
    n1 = normal_vector_3p(f1[0], f1[1], f1[2]) * 0.5
    e0 = c0 + n0
    e1 = c1 + n1
    # distance of centroids
    d0 = c0.distance(c1)
    # distance of normal end points
    d1 = e0.distance(e1)
    return d1 > d0


def face_normals_after_transformation(m: Matrix44) -> bool:
    """Returns the state of face-normals of the unit-cube after the
    transformation `m` was applied.

        - ``True``: face-normals pointing outwards
        - ``False``: face-normals pointing inwards

    The state before the transformation is outward-pointing face-normals.

    """
    from .forms import cube

    unit_cube = cube(True).transform(m)
    bottom, _, _, _, _, top = unit_cube.faces_as_vertices()
    # it is not necessary to check all 3 axis!
    return have_away_pointing_normals(bottom, top)


def _make_edge_mapping(faces: Iterable[Face]) -> dict[Edge, list[Face]]:
    mapping: dict[Edge, list[Face]] = {}
    for face in faces:
        for edge in face_edges(face):
            mapping.setdefault(edge, []).append(face)
    return mapping


class FaceOrientationDetector:
    """
    Helper class for face orientation and face normal vector detection. Use the
    method :meth:`MeshBuilder.face_orientation_detector` to create an instance.

    The face orientation detector classifies the faces of a mesh by their
    forward or backward orientation.
    The forward orientation is defined by a reference face, which is the first
    face of the mesh by default and this orientation is not necessarily outwards.

    This class has some overlapping features with :class:`MeshDiagnose` but it
    has a longer setup time and needs more memory than :class:`MeshDiagnose`.

    Args:
        mesh: source mesh as :class:`MeshBuilder` object
        reference: index of the reference face

    """

    def __init__(self, mesh: MeshBuilder, reference: int = 0):
        self._mesh = mesh
        self.edge_mapping: dict[Edge, list[Face]] = _make_edge_mapping(mesh.faces)
        self.reference = reference
        self.is_manifold = True  # 2-manifold is meant
        self.forward: dict[int, Face] = dict()
        self.backward: dict[int, Face] = dict()
        self.classify_faces(reference)

    @property
    def has_uniform_face_normals(self) -> bool:
        """Returns ``True`` if all reachable faces are forward oriented
        according to the reference face.
        """
        return len(self.backward) == 0

    @property
    def all_reachable(self) -> bool:
        """Returns ``True`` if all faces are reachable from the reference face
        same as property :attr:`is_single_mesh`.
        """
        return len(self._mesh.faces) == sum(self.count)

    @property
    def is_single_mesh(self) -> bool:
        """Returns ``True`` if only a single mesh is present same as property
        :attr:`all_reachable`.
        """
        return self.all_reachable

    @property
    def count(self) -> tuple[int, int]:
        """Returns the count of forward and backward oriented faces."""
        return len(self.forward), len(self.backward)

    @property
    def forward_faces(self) -> Iterator[Face]:
        """Yields all forward oriented faces."""
        return iter(self.forward.values())

    @property
    def backward_faces(self) -> Iterator[Face]:
        """Yields all backward oriented faces."""
        return iter(self.backward.values())

    def classify_faces(self, reference: int = 0) -> None:
        """Detect the forward and backward oriented faces.

        The forward and backward orientation has to be defined by a `reference`
        face.
        """

        def add_forward(f: Face):
            forward[id(f)] = f

        def add_backward(f: Face):
            backward[id(f)] = f

        def add_face_to_process(f: Face, orientation: bool):
            key = id(f)
            if key not in forward and key not in backward:  # unprocessed face!
                process_faces.append((f, orientation))

        def add_adjacent_faces_to_process_queue(f: Face, orientation: bool):
            for edge in face_edges(f):
                # find adjacent faces at this edge with same edge orientation:
                linked_faces = edge_mapping[edge]
                if len(linked_faces) > 1:
                    # these faces are backward oriented faces:
                    for linked_face in linked_faces:
                        if linked_face is f:  # skip legit current face
                            continue
                        add_face_to_process(linked_face, not orientation)

                # find all adjacent faces at this edge with reversed edges:
                try:
                    linked_faces = edge_mapping[edge[1], edge[0]]
                except KeyError:
                    # open surface or backward oriented faces present
                    continue
                if len(linked_faces) == 1:
                    add_face_to_process(linked_faces[0], orientation)
                else:  # non-manifold mesh
                    # none of the linked face is processed!
                    self.is_manifold = False

        self.reference = int(reference)
        self.is_manifold = True
        edge_mapping = self.edge_mapping
        forward: dict[int, Face] = dict()
        backward: dict[int, Face] = dict()
        # the reference face defines the forward orientation
        process_faces: list[tuple[Face, bool]] = [(self._mesh.faces[reference], True)]
        while len(process_faces):
            # current face and orientation, True = forward, False = backward
            face, face_orientation = process_faces.pop(0)
            if face_orientation:
                add_forward(face)
            else:
                add_backward(face)
            add_adjacent_faces_to_process_queue(face, face_orientation)

        self.forward = forward
        self.backward = backward

    @property
    def is_closed_surface(self) -> bool:
        """Returns ``True`` if the mesh has a closed surface.
        This method does not require a unified face orientation.
        If multiple separated meshes are present the state is only ``True`` if
        **all** meshes have a closed surface.

        Returns ``False`` for non-manifold meshes.

        """
        if not self.is_manifold:
            return False
        empty: list[Face] = []
        edge_mapping = self.edge_mapping
        # For a closed surface all edges have to connect exact 2 faces.
        for edge in edge_mapping.keys():
            forward_connected_faces = edge_mapping.get(edge, empty)
            reversed_edge = edge[1], edge[0]
            backward_connected_faces = edge_mapping.get(reversed_edge, empty)
            if len(forward_connected_faces) + len(backward_connected_faces) != 2:
                return False
        return True

    def is_reference_face_pointing_outwards(self) -> bool:
        """Returns ``True`` if the normal vector of the reference face is
        pointing outwards. This works only for meshes with unified faces which
        represent a closed surfaces, and it's a time-consuming calculation!

        """
        from ezdxf.math import is_face_normal_pointing_outwards

        faces = list(self._mesh.faces_as_vertices())
        reference_face = faces[self.reference]
        return is_face_normal_pointing_outwards(faces, reference_face)


def unify_face_normals_by_reference(
    mesh: MeshBuilder,
    *,
    fod: Optional[FaceOrientationDetector] = None,
    reference: int = 0,
) -> MeshTransformer:
    """Unifies the orientation of all faces of a :class:`MeshBuilder` object.
    The forward orientation is defined by a reference face, which is the first
    face of the `mesh` by default. This function can not process non-manifold
    meshes (more than two faces are connected by a single edge) or multiple
    disconnected meshes in a single :class:`MeshBuilder` object.
    Returns always a copy of the source `mesh` as :class:`MeshTransformer`
    object.

    Args:
        mesh: source :class:`MeshBuilder` object
        fod: an already created :class:`FaceOrientationDetector`
            instance or ``None`` to create one internally.
        reference: index of the reference face for the internally created
            :class:`FaceOrientationDetector` instance

    Raises:
        NonManifoldError: non-manifold mesh
        MultipleMeshesError: :class:`MeshBuilder` object contains multiple
            disconnected meshes

    """
    if fod is None:
        fod = FaceOrientationDetector(mesh, reference)

    def backward_selector(detector: FaceOrientationDetector):
        return detector.backward

    return _unify_face_normals(mesh, fod, backward_selector)


def unify_face_normals_by_majority(
    mesh: MeshBuilder,
    *,
    fod: Optional[FaceOrientationDetector] = None,
) -> MeshTransformer:
    """Unifies the orientation of all faces of a :class:`MeshBuilder` object.
    The forward orientation is defined by the orientation of the majority of
    the faces.
    This function can not process non-manifold meshes (more than two faces are
    connected by a single edge) or multiple disconnected meshes in a single
    :class:`MeshBuilder` object.
    Returns always a copy of the source `mesh` as :class:`MeshTransformer`
    object.

    Args:
        mesh: source :class:`MeshBuilder` object
        fod: an already created :class:`FaceOrientationDetector`
            instance or ``None`` to create one internally.

    Raises:
        NonManifoldError: non-manifold mesh
        MultipleMeshesError: :class:`MeshBuilder` object contains multiple
            disconnected meshes

    """
    if fod is None:
        fod = FaceOrientationDetector(mesh)

    def backward_selector(detector: FaceOrientationDetector):
        count0, count1 = detector.count
        return detector.backward if count0 >= count1 else detector.forward

    return _unify_face_normals(mesh, fod, backward_selector)


def _unify_face_normals(
    mesh: MeshBuilder,
    fod: FaceOrientationDetector,
    backward_selector,
) -> MeshTransformer:
    """Unifies all faces of a MeshBuilder. The backward_selector() function
    returns the face ledger which represents the backward oriented faces.
    """
    if not fod.is_manifold:
        raise NonManifoldMeshError("non-manifold mesh")
    if not fod.all_reachable:
        raise MultipleMeshesError(
            f"not all faces are reachable from reference face #{fod.reference}"
        )

    new_mesh = MeshTransformer()
    new_mesh.vertices = list(mesh.vertices)
    if not fod.has_uniform_face_normals:
        backward = backward_selector(fod)
        faces = []
        for face in mesh.faces:
            if id(face) in backward:
                faces.append(tuple(reversed(face)))
            else:
                faces.append(tuple(face))
        new_mesh.faces = faces  # type: ignore
    else:
        new_mesh.faces = list(mesh.faces)
    return new_mesh


def _force_face_normals_pointing_outwards(
    mesh: MeshBuilder, reference: int = 0
) -> None:
    """Detect the orientation of the reference face and flip all face normals
    if required.

    Requirements: all face normals have to be unified and the mesh has a closed
    surface

    """
    from ezdxf.math import is_face_normal_pointing_outwards

    faces = list(mesh.faces_as_vertices())
    reference_face = faces[reference]
    if not is_face_normal_pointing_outwards(faces, reference_face):
        mesh.flip_normals()


def normalize_mesh(mesh: MeshBuilder) -> MeshTransformer:
    """Returns a new mesh with these properties:

        - one mesh: removes all meshes except the first mesh starting at the
          first face
        - optimized vertices: merges shared vertices
        - normalized faces: removes duplicated face vertices and faces with less
          than 3 vertices
        - open faces: all faces are open faces where first vertex != last vertex
        - unified face-orientation: all faces have the same orientation (ccw or cw)
        - ccw face-orientation: all face-normals are pointing outwards if the
          mesh has a closed surface

    Raises:
        NonManifoldMeshError: non-manifold mesh

    """
    new_mesh = mesh.optimize_vertices()
    new_mesh.normalize_faces()
    fod = new_mesh.face_orientation_detector(reference=0)

    if not fod.is_manifold:
        raise NonManifoldMeshError("non-manifold mesh")

    if not fod.all_reachable:  # extract first mesh
        new_mesh = list(separate_meshes(new_mesh))[0]
        fod = new_mesh.face_orientation_detector(reference=0)

    if not fod.has_uniform_face_normals:
        new_mesh = unify_face_normals_by_reference(new_mesh, fod=fod)
        fod = new_mesh.face_orientation_detector()

    if fod.is_closed_surface and not fod.is_reference_face_pointing_outwards():
        new_mesh.flip_normals()
    return new_mesh
