#  Copyright (c) 2022-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterator, Sequence, Optional, Iterable
from ezdxf.render import MeshVertexMerger, MeshTransformer, MeshBuilder
from ezdxf.math import Matrix44, Vec3, NULLVEC, BoundingBox
from . import entities
from .entities import Body, Lump, NONE_REF, Face, Shell


def mesh_from_body(body: Body, merge_lumps=True) -> list[MeshTransformer]:
    """Returns a list of :class:`~ezdxf.render.MeshTransformer` instances from
    the given ACIS :class:`Body` entity.
    The list contains multiple meshes if `merge_lumps` is ``False`` or just a
    single mesh if `merge_lumps` is ``True``.

    The ACIS format stores the faces in counter-clockwise orientation where the
    face-normal points outwards (away) from the solid body (material).

    .. note::

        This function returns meshes build up only from flat polygonal
        :class:`Face` entities, for a tessellation of more complex ACIS
        entities (spline surfaces, tori, cones, ...) is an ACIS kernel
        required which `ezdxf` does not provide.

    Args:
        body: ACIS entity of type :class:`Body`
        merge_lumps: returns all :class:`Lump` entities
            from a body as a single mesh if ``True`` otherwise each :class:`Lump`
            entity is a separated mesh

    Raises:
        TypeError: given `body` entity has invalid type

    """
    if not isinstance(body, Body):
        raise TypeError(f"expected a body entity, got: {type(body)}")

    meshes: list[MeshTransformer] = []
    builder = MeshVertexMerger()
    for faces in flat_polygon_faces_from_body(body):
        for face in faces:
            builder.add_face(face)
        if not merge_lumps:
            meshes.append(MeshTransformer.from_builder(builder))
            builder = MeshVertexMerger()
    if merge_lumps:
        meshes.append(MeshTransformer.from_builder(builder))
    return meshes


def flat_polygon_faces_from_body(
    body: Body,
) -> Iterator[list[Sequence[Vec3]]]:
    """Yields all flat polygon faces from all lumps in the given
    :class:`Body` entity.
    Yields a separated list of faces for each linked :class:`Lump` entity.

    Args:
        body: ACIS entity of type :class:`Body`

    Raises:
        TypeError: given `body` entity has invalid type

    """

    if not isinstance(body, Body):
        raise TypeError(f"expected a body entity, got: {type(body)}")
    lump = body.lump
    transform = body.transform

    m: Optional[Matrix44] = None
    if not transform.is_none:
        m = transform.matrix
    while not lump.is_none:
        yield list(flat_polygon_faces_from_lump(lump, m))
        lump = lump.next_lump


def flat_polygon_faces_from_lump(
    lump: Lump, m: Matrix44 | None = None
) -> Iterator[Sequence[Vec3]]:
    """Yields all flat polygon faces from the given :class:`Lump` entity as
    sequence of :class:`~ezdxf.math.Vec3` instances. Applies the transformation
    :class:`~ezdxf.math.Matrix44` `m` to all vertices if not ``None``.

    Args:
        lump: :class:`Lump` entity
        m: optional transformation matrix

    Raises:
        TypeError: `lump` has invalid ACIS type

    """
    if not isinstance(lump, Lump):
        raise TypeError(f"expected a lump entity, got: {type(lump)}")

    shell = lump.shell
    if shell.is_none:
        return  # not a shell
    vertices: list[Vec3] = []
    face = shell.face
    while not face.is_none:
        first_coedge = NONE_REF
        vertices.clear()
        if face.surface.type == "plane-surface":
            try:
                first_coedge = face.loop.coedge
            except AttributeError:  # loop is a none-entity
                pass
        coedge = first_coedge
        while not coedge.is_none:  # invalid coedge or face is not closed
            # the edge entity contains the vertices and the curve type
            edge = coedge.edge
            try:
                # only straight lines as face edges supported:
                if edge.curve.type != "straight-curve":
                    break
                # add the first edge vertex to the face vertices
                if coedge.sense:  # reversed sense of the underlying edge
                    vertices.append(edge.end_vertex.point.location)
                else:  # same sense as the underlying edge
                    vertices.append(edge.start_vertex.point.location)
            except AttributeError:
                # one of the involved entities is a none-entity or an
                # incompatible entity type -> ignore this face!
                break
            coedge = coedge.next_coedge
            if coedge is first_coedge:  # a valid closed face
                if m is not None:
                    yield tuple(m.transform_vertices(vertices))
                else:
                    yield tuple(vertices)
                break
        face = face.next_face


def body_from_mesh(mesh: MeshBuilder, precision: int = 6) -> Body:
    """Returns a :term:`ACIS` :class:`~ezdxf.acis.entities.Body` entity from a
    :class:`~ezdxf.render.MeshBuilder` instance.

    This entity can be assigned to a :class:`~ezdxf.entities.Solid3d` DXF entity
    as :term:`SAT` or :term:`SAB` data according to the version your DXF
    document uses (SAT for DXF R2000 to R2010 and SAB for DXF R2013 and later).

    If the `mesh` contains multiple separated meshes, each mesh will be a
    separated :class:`~ezdxf.acis.entities.Lump` node.
    If each mesh should get its own :class:`~ezdxf.acis.entities.Body` entity,
    separate the meshes beforehand by the method
    :meth:`~ezdxf.render.MeshBuilder.separate_meshes`.

    A closed mesh creates a solid body and an open mesh creates an open (hollow)
    shell. The detection if the mesh is open or closed is based on the edges
    of the mesh: if **all** edges of mesh have two adjacent faces the mesh is
    closed.

    The current implementation applies automatically a vertex optimization,
    which merges coincident vertices into a single vertex.

    """
    mesh = mesh.optimize_vertices(precision)
    body = Body()
    bbox = BoundingBox(mesh.vertices)
    if not bbox.center.is_null:
        mesh.translate(-bbox.center)
        transform = entities.Transform()
        transform.matrix = Matrix44.translate(*bbox.center)
        body.transform = transform

    for mesh in mesh.separate_meshes():
        lump = lump_from_mesh(mesh)
        body.append_lump(lump)
    return body


def lump_from_mesh(mesh: MeshBuilder) -> Lump:
    """Returns a :class:`~ezdxf.acis.entities.Lump` entity from a
    :class:`~ezdxf.render.MeshBuilder` instance. The `mesh` has to be a single
    body or shell!
    """
    lump = Lump()
    shell = Shell()
    lump.append_shell(shell)
    face_builder = PolyhedronFaceBuilder(mesh)
    for face in face_builder.acis_faces():
        shell.append_face(face)
    return lump


class PolyhedronFaceBuilder:
    def __init__(self, mesh: MeshBuilder):
        mesh_copy = mesh.copy()
        mesh_copy.normalize_faces()  # open faces without duplicates!
        self.vertices: list[Vec3] = mesh_copy.vertices
        self.faces: list[Sequence[int]] = mesh_copy.faces
        self.normals = list(mesh_copy.face_normals())
        self.acis_vertices: list[entities.Vertex] = []

        # double_sided:
        # If every edge belongs to two faces the body is for sure a closed
        # surface. But the "is_edge_balance_broken" property can not detect
        # non-manifold meshes!
        # - True: the body is an open shell, each side of the face is outside
        #   (environment side)
        # - False: the body is a closed solid body, one side points outwards of
        #   the body (environment side) and one side points inwards (material
        #   side)
        self.double_sided = mesh_copy.diagnose().is_edge_balance_broken

        # coedges and edges ledger, where index1 <= index2
        self.partner_coedges: dict[tuple[int, int], entities.Coedge] = dict()
        self.edges: dict[tuple[int, int], entities.Edge] = dict()

    def reset(self):
        self.acis_vertices = list(make_vertices(self.vertices))
        self.partner_coedges.clear()
        self.edges.clear()

    def acis_faces(self) -> list[Face]:
        self.reset()
        faces: list[Face] = []
        for face, face_normal in zip(self.faces, self.normals):
            if face_normal.is_null:
                continue
            acis_face = Face()
            plane = self.make_plane(face)
            if plane is None:
                continue
            plane.normal = face_normal
            loop = self.make_loop(face)
            if loop is None:
                continue
            acis_face.append_loop(loop)
            acis_face.surface = plane
            acis_face.sense = False  # face normal is plane normal
            acis_face.double_sided = self.double_sided
            faces.append(acis_face)
        # The link structure of all entities is only completed at the end of
        # the building process. Do not yield faces from the body of the loop!
        return faces

    def make_plane(self, face: Sequence[int]) -> Optional[entities.Plane]:
        assert len(face) > 1, "face requires least 2 vertices"
        plane = entities.Plane()
        # normal is always calculated by the right-hand rule:
        plane.reverse_v = False
        plane.origin = self.vertices[face[0]]
        try:
            plane.u_dir = (self.vertices[face[1]] - plane.origin).normalize()
        except ZeroDivisionError:
            return None  # vertices are too close together
        return plane

    def make_loop(self, face: Sequence[int]) -> Optional[entities.Loop]:
        coedges: list[entities.Coedge] = []
        face2 = list(face[1:])
        if face[0] != face[-1]:
            face2.append(face[0])

        for i1, i2 in zip(face, face2):
            coedge = self.make_coedge(i1, i2)
            coedge.edge, coedge.sense = self.make_edge(i1, i2, coedge)
            coedges.append(coedge)
        loop = entities.Loop()
        loop.set_coedges(coedges, close=True)
        return loop

    def make_coedge(self, index1: int, index2: int) -> entities.Coedge:
        if index1 > index2:
            key = index2, index1
        else:
            key = index1, index2
        coedge = entities.Coedge()
        try:
            partner_coedge = self.partner_coedges[key]
        except KeyError:
            self.partner_coedges[key] = coedge
        else:
            partner_coedge.add_partner_coedge(coedge)
        return coedge

    def make_edge(
        self, index1: int, index2: int, parent: entities.Coedge
    ) -> tuple[entities.Edge, bool]:
        def make_vertex(index: int):
            vertex = self.acis_vertices[index]
            vertex.ref_count += 1
            # assign first edge which references the vertex as parent edge (?):
            if vertex.edge.is_none:
                vertex.edge = edge
            return vertex

        sense = False
        ex1 = index1  # vertex index of unified edges
        ex2 = index2  # vertex index of unified edges
        if ex1 > ex2:
            sense = True
            ex1, ex2 = ex2, ex1
        try:
            return self.edges[ex1, ex2], sense
        except KeyError:
            pass
        # The edge has always the same direction as the underlying
        # straight curve:
        edge = entities.Edge()
        edge.coedge = parent  # first coedge which references this edge
        edge.sense = False
        edge.start_vertex = make_vertex(ex1)
        edge.start_param = 0.0
        edge.end_vertex = make_vertex(ex2)
        edge.end_param = self.vertices[ex1].distance(self.vertices[ex2])
        edge.curve = self.make_ray(ex1, ex2)
        self.edges[ex1, ex2] = edge
        return edge, sense

    def make_ray(self, index1: int, index2: int) -> entities.StraightCurve:
        v1 = self.vertices[index1]
        v2 = self.vertices[index2]
        ray = entities.StraightCurve()
        ray.origin = v1
        try:
            ray.direction = (v2 - v1).normalize()
        except ZeroDivisionError:  # avoided by normalize_faces()
            ray.direction = NULLVEC
        return ray


def make_vertices(vertices: Iterable[Vec3]) -> Iterator[entities.Vertex]:
    for v in vertices:
        point = entities.Point()
        point.location = v
        vertex = entities.Vertex()
        vertex.point = point
        yield vertex


def vertices_from_body(body: Body) -> list[Vec3]:
    """Returns all stored vertices in the given :class:`Body` entity.
    The result is not optimized, meaning the vertices are in no particular order and
    there are duplicates.

    This function can be useful to determining the approximate bounding box of an 
    :term:`ACIS` entity.  The result is exact for polyhedra with flat faces with 
    straight edges, but not for bodies with curved edges and faces.

    Args:
        body: ACIS entity of type :class:`Body`

    Raises:
        TypeError: given `body` entity has invalid type

    """

    if not isinstance(body, Body):
        raise TypeError(f"expected a body entity, got: {type(body)}")
    lump = body.lump
    transform = body.transform
    vertices: list[Vec3] = []

    m: Optional[Matrix44] = None
    if not transform.is_none:
        m = transform.matrix
    while not lump.is_none:
        vertices.extend(vertices_from_lump(lump, m))
        lump = lump.next_lump
    return vertices


def vertices_from_lump(lump: Lump, m: Matrix44 | None = None) -> list[Vec3]:
    """Returns all stored vertices from a given :class:`Lump` entity. 
    Applies the transformation :class:`~ezdxf.math.Matrix44` `m` to all vertices if not 
    ``None``.

    Args:
        lump: :class:`Lump` entity
        m: optional transformation matrix

    Raises:
        TypeError: `lump` has invalid ACIS type

    """
    if not isinstance(lump, Lump):
        raise TypeError(f"expected a lump entity, got: {type(lump)}")

    vertices: list[Vec3] = []
    shell = lump.shell
    if shell.is_none:
        return vertices  # not a shell

    face = shell.face
    while not face.is_none:
        first_coedge = NONE_REF
        try:
            first_coedge = face.loop.coedge
        except AttributeError:  # loop is a none-entity
            pass
        coedge = first_coedge
        while not coedge.is_none:  # invalid coedge or face is not closed
            # the edge entity contains the vertices and the curve type
            edge = coedge.edge
            try:
                vertices.append(edge.start_vertex.point.location)
                vertices.append(edge.end_vertex.point.location)
            except AttributeError:
                # one of the involved entities is a none-entity or an
                # incompatible entity type -> ignore this face!
                break
            coedge = coedge.next_coedge
            if coedge is first_coedge:  # a valid closed face
                break
        face = face.next_face
    if m is not None:
        return list(m.transform_vertices(vertices))
    return vertices
