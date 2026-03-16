# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    Union,
    cast,
    Sequence,
    Optional,
)
from itertools import chain
from ezdxf.lldxf import validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
    merge_group_code_mappings,
)
from ezdxf.lldxf.const import DXF12, SUBCLASS_MARKER, VERTEXNAMES
from ezdxf.lldxf import const
from ezdxf.math import Vec3, Matrix44, NULLVEC, Z_AXIS, UVec
from ezdxf.math.transformtools import OCSTransform, NonUniformScalingError
from ezdxf.render.polyline import virtual_polyline_entities
from ezdxf.explode import explode_entity
from ezdxf.query import EntityQuery
from ezdxf.entities import factory
from ezdxf.audit import AuditError
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import DXFGraphic, acdb_entity, acdb_entity_group_codes
from .lwpolyline import FORMAT_CODES
from .subentity import LinkedEntities

if TYPE_CHECKING:
    from ezdxf.audit import Auditor
    from ezdxf.entities import DXFNamespace, Line, Arc, Face3d
    from ezdxf.layouts import BaseLayout
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.eztypes import FaceType

__all__ = ["Polyline", "Polyface", "Polymesh"]

acdb_polyline = DefSubclass(
    "AcDbPolylineDummy",
    {
        # AcDbPolylineDummy is a temporary solution while loading
        # Group code 66 is obsolete - Vertices follow flag
        # Elevation is a "dummy" point. The x and y values are always 0,
        # and the Z value is the polyline elevation:
        "elevation": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        # Polyline flags (bit-coded):
        # 1 = closed POLYLINE or a POLYMESH closed in the M direction
        # 2 = Curve-fit vertices have been added
        # 4 = Spline-fit vertices have been added
        # 8 = 3D POLYLINE
        # 16 = POLYMESH
        # 32 = POLYMESH is closed in the N direction
        # 64 = POLYFACE
        # 128 = linetype pattern is generated continuously around the vertices
        "flags": DXFAttr(70, default=0),
        "default_start_width": DXFAttr(40, default=0, optional=True),
        "default_end_width": DXFAttr(41, default=0, optional=True),
        "m_count": DXFAttr(
            71,
            default=0,
            optional=True,
            validator=validator.is_greater_or_equal_zero,
            fixer=RETURN_DEFAULT,
        ),
        "n_count": DXFAttr(
            72,
            default=0,
            optional=True,
            validator=validator.is_greater_or_equal_zero,
            fixer=RETURN_DEFAULT,
        ),
        "m_smooth_density": DXFAttr(73, default=0, optional=True),
        "n_smooth_density": DXFAttr(74, default=0, optional=True),
        # Curves and smooth surface type:
        # 0 = No smooth surface fitted
        # 5 = Quadratic B-spline surface
        # 6 = Cubic B-spline surface
        # 8 = Bezier surface
        "smooth_type": DXFAttr(
            75,
            default=0,
            optional=True,
            validator=validator.is_one_of({0, 5, 6, 8}),
            fixer=RETURN_DEFAULT,
        ),
        "thickness": DXFAttr(39, default=0, optional=True),
        "extrusion": DXFAttr(
            210,
            xtype=XType.point3d,
            default=Z_AXIS,
            optional=True,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
    },
)
acdb_polyline_group_codes = group_code_mapping(acdb_polyline, ignore=(66,))
merged_polyline_group_codes = merge_group_code_mappings(
    acdb_entity_group_codes, acdb_polyline_group_codes  # type: ignore
)


# Notes to SEQEND:
# todo: A loaded entity should have a valid SEQEND, a POLYLINE without vertices
#  makes no sense - has to be tested
#
# A virtual POLYLINE does not need a SEQEND, because it can not be exported,
# therefore the SEQEND entity should not be created in the
# DXFEntity.post_new_hook() method.
#
# A bounded POLYLINE needs a SEQEND to valid at export, therefore the
# LinkedEntities.post_bind_hook() method creates a new SEQEND after binding
# the entity to a document if needed.


@factory.register_entity
class Polyline(LinkedEntities):
    """DXF POLYLINE entity

    The POLYLINE entity is hard owner of its VERTEX entities and the SEQEND entity:

       VERTEX.dxf.owner == POLYLINE.dxf.handle
       SEQEND.dxf.owner == POLYLINE.dxf.handle

    """

    DXFTYPE = "POLYLINE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_polyline)
    # polyline flags (70)
    CLOSED = 1
    MESH_CLOSED_M_DIRECTION = CLOSED
    CURVE_FIT_VERTICES_ADDED = 2
    SPLINE_FIT_VERTICES_ADDED = 4
    POLYLINE_3D = 8
    POLYMESH = 16
    MESH_CLOSED_N_DIRECTION = 32
    POLYFACE = 64
    GENERATE_LINETYPE_PATTERN = 128
    # polymesh smooth type (75)
    NO_SMOOTH = 0
    QUADRATIC_BSPLINE = 5
    CUBIC_BSPLINE = 6
    BEZIER_SURFACE = 8
    ANY3D = POLYLINE_3D | POLYMESH | POLYFACE

    @property
    def vertices(self) -> list[DXFVertex]:
        return self._sub_entities  # type: ignore

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Loading interface. (internal API)"""
        # bypass DXFGraphic, loading proxy graphic is skipped!
        dxf = super(DXFGraphic, self).load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, merged_polyline_group_codes)
        return dxf

    def export_dxf(self, tagwriter: AbstractTagWriter):
        """Export POLYLINE entity and all linked entities: VERTEX, SEQEND."""
        super().export_dxf(tagwriter)
        # export sub-entities
        self.process_sub_entities(lambda e: e.export_dxf(tagwriter))

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export POLYLINE specific data as DXF tags."""
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            tagwriter.write_tag2(SUBCLASS_MARKER, self.get_mode())

        tagwriter.write_tag2(66, 1)  # Vertices follow
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "elevation",
                "flags",
                "default_start_width",
                "default_end_width",
                "m_count",
                "n_count",
                "m_smooth_density",
                "n_smooth_density",
                "smooth_type",
                "thickness",
                "extrusion",
            ],
        )

    def on_layer_change(self, layer: str):
        """Event handler for layer change. Changes also the layer of all vertices.

        Args:
            layer: new layer as string

        """
        for v in self.vertices:
            v.dxf.layer = layer

    def on_linetype_change(self, linetype: str):
        """Event handler for linetype change. Changes also the linetype of all
        vertices.

        Args:
            linetype: new linetype as string

        """
        for v in self.vertices:
            v.dxf.linetype = linetype

    def get_vertex_flags(self) -> int:
        return const.VERTEX_FLAGS[self.get_mode()]

    def get_mode(self) -> str:
        """Returns POLYLINE type as string:

        - "AcDb2dPolyline"
        - "AcDb3dPolyline"
        - "AcDbPolygonMesh"
        - "AcDbPolyFaceMesh"

        """
        if self.is_3d_polyline:
            return "AcDb3dPolyline"
        elif self.is_polygon_mesh:
            return "AcDbPolygonMesh"
        elif self.is_poly_face_mesh:
            return "AcDbPolyFaceMesh"
        else:
            return "AcDb2dPolyline"

    @property
    def is_2d_polyline(self) -> bool:
        """``True`` if POLYLINE is a 2D polyline."""
        return self.dxf.flags & self.ANY3D == 0

    @property
    def is_3d_polyline(self) -> bool:
        """``True`` if POLYLINE is a 3D polyline."""
        return bool(self.dxf.flags & self.POLYLINE_3D)

    @property
    def is_polygon_mesh(self) -> bool:
        """``True`` if POLYLINE is a polygon mesh, see :class:`Polymesh`"""
        return bool(self.dxf.flags & self.POLYMESH)

    @property
    def is_poly_face_mesh(self) -> bool:
        """``True`` if POLYLINE is a poly face mesh, see :class:`Polyface`"""
        return bool(self.dxf.flags & self.POLYFACE)

    @property
    def is_closed(self) -> bool:
        """``True`` if POLYLINE is closed."""
        return bool(self.dxf.flags & self.CLOSED)

    @property
    def is_m_closed(self) -> bool:
        """``True`` if POLYLINE (as :class:`Polymesh`) is closed in m
        direction.
        """
        return bool(self.dxf.flags & self.MESH_CLOSED_M_DIRECTION)

    @property
    def is_n_closed(self) -> bool:
        """``True`` if POLYLINE (as :class:`Polymesh`) is closed in n
        direction.
        """
        return bool(self.dxf.flags & self.MESH_CLOSED_N_DIRECTION)

    @property
    def has_arc(self) -> bool:
        """Returns ``True`` if 2D POLYLINE has an arc segment."""
        if self.is_2d_polyline:
            return any(
                v.dxf.hasattr("bulge") and bool(v.dxf.bulge) for v in self.vertices
            )
        else:
            return False

    @property
    def has_width(self) -> bool:
        """Returns ``True`` if 2D POLYLINE has default width values or any
        segment with width attributes.
        """
        if self.is_2d_polyline:
            if self.dxf.hasattr("default_start_width") and bool(
                self.dxf.default_start_width
            ):
                return True
            if self.dxf.hasattr("default_end_width") and bool(
                self.dxf.default_end_width
            ):
                return True
            for v in self.vertices:
                if v.dxf.hasattr("start_width") and bool(v.dxf.start_width):
                    return True
                if v.dxf.hasattr("end_width") and bool(v.dxf.end_width):
                    return True
        return False

    def m_close(self, status=True) -> None:
        """Close POLYMESH in m direction if `status` is ``True`` (also closes
        POLYLINE), clears closed state if `status` is ``False``.
        """
        self.set_flag_state(self.MESH_CLOSED_M_DIRECTION, status, name="flags")

    def n_close(self, status=True) -> None:
        """Close POLYMESH in n direction if `status` is ``True``, clears closed
        state if `status` is ``False``.
        """
        self.set_flag_state(self.MESH_CLOSED_N_DIRECTION, status, name="flags")

    def close(self, m_close=True, n_close=False) -> None:
        """Set closed state of POLYMESH and POLYLINE in m direction and n
        direction. ``True`` set closed flag, ``False`` clears closed flag.
        """
        self.m_close(m_close)
        self.n_close(n_close)

    def __len__(self) -> int:
        """Returns count of :class:`Vertex` entities."""
        return len(self.vertices)

    def __getitem__(self, pos) -> DXFVertex:
        """Get :class:`Vertex` entity at position `pos`, supports list-like slicing."""
        return self.vertices[pos]

    def points(self) -> Iterator[Vec3]:
        """Returns all polyline points in :ref:`OCS` or :ref:`WCS` coordinates as
        :class:`~ezdxf.math.Vec3`.

        These are the raw location coordinates stored in the :class:`Vertex` entities.
        A separately stored elevation value will not be applied. The points of
        2D polylines are :ref:`OCS` coordinates other polyline types return :ref:`WCS`
        coordinates.
        """
        return (vertex.dxf.location for vertex in self.vertices)

    def points_in_wcs(self) -> Iterator[Vec3]:
        """Returns all polyline points in :ref:`WCS` coordinates as
        :class:`~ezdxf.math.Vec3`.

        .. versionadded:: 1.4

        """
        points = self.points()
        if not self.is_2d_polyline:
            return points

        elevation: float = self.dxf.elevation.z
        if elevation:
            points = (p.replace(z=elevation) for p in points)

        if Z_AXIS.isclose(self.dxf.extrusion):  # OCS == WCS
            return points

        ocs = self.ocs()
        return ocs.points_to_wcs(points)

    def _append_vertex(self, vertex: DXFVertex) -> None:
        self.vertices.append(vertex)

    def append_vertices(self, points: Iterable[UVec], dxfattribs=None) -> None:
        """Append multiple :class:`Vertex` entities at location `points`.

        Args:
            points: iterable of (x, y[, z]) tuples
            dxfattribs: dict of DXF attributes for the VERTEX objects

        """
        dxfattribs = dict(dxfattribs or {})
        for vertex in self._build_dxf_vertices(points, dxfattribs):
            self._append_vertex(vertex)

    def append_formatted_vertices(
        self,
        points: Iterable[Sequence],
        format: str = "xy",
        dxfattribs=None,
    ) -> None:
        """Append multiple :class:`Vertex` entities at location `points`.

        Args:
            points: iterable of (x, y, [start_width, [end_width, [bulge]]])
                    tuple
            format: format string, default is "xy", see: :ref:`format codes`
            dxfattribs: dict of DXF attributes for the VERTEX objects

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["flags"] = dxfattribs.get("flags", 0) | self.get_vertex_flags()

        # same DXF attributes for VERTEX entities as for POLYLINE
        dxfattribs["owner"] = self.dxf.owner
        dxfattribs["layer"] = self.dxf.layer
        if self.dxf.hasattr("linetype"):
            dxfattribs["linetype"] = self.dxf.linetype

        for point in points:
            attribs = vertex_attribs(point, format)
            attribs.update(dxfattribs)
            vertex = cast(DXFVertex, self._new_compound_entity("VERTEX", attribs))
            self._append_vertex(vertex)

    def append_vertex(self, point: UVec, dxfattribs=None) -> None:
        """Append a single :class:`Vertex` entity at location `point`.

        Args:
            point: as (x, y[, z]) tuple
            dxfattribs: dict of DXF attributes for :class:`Vertex` class

        """
        dxfattribs = dict(dxfattribs or {})
        for vertex in self._build_dxf_vertices([point], dxfattribs):
            self._append_vertex(vertex)

    def insert_vertices(
        self, pos: int, points: Iterable[UVec], dxfattribs=None
    ) -> None:
        """Insert vertices `points` into :attr:`Polyline.vertices` list
        at insertion location `pos` .

        Args:
            pos: insertion position of list :attr:`Polyline.vertices`
            points: list of (x, y[, z]) tuples
            dxfattribs: dict of DXF attributes for :class:`Vertex` class

        """
        dxfattribs = dict(dxfattribs or {})
        self.vertices[pos:pos] = list(self._build_dxf_vertices(points, dxfattribs))

    def _build_dxf_vertices(
        self, points: Iterable[UVec], dxfattribs: dict
    ) -> Iterator[DXFVertex]:
        """Converts point (x, y, z) tuples into DXFVertex objects.

        Args:
            points: list of (x, y, z)-tuples
            dxfattribs: dict of DXF attributes
        """
        dxfattribs["flags"] = dxfattribs.get("flags", 0) | self.get_vertex_flags()

        # same DXF attributes for VERTEX entities as for POLYLINE
        dxfattribs["owner"] = self.dxf.handle
        dxfattribs["layer"] = self.dxf.layer
        if self.dxf.hasattr("linetype"):
            dxfattribs["linetype"] = self.dxf.linetype
        for point in points:
            dxfattribs["location"] = Vec3(point)
            yield cast(DXFVertex, self._new_compound_entity("VERTEX", dxfattribs))

    def cast(self) -> Union[Polyline, Polymesh, Polyface]:
        mode = self.get_mode()
        if mode == "AcDbPolyFaceMesh":
            return Polyface.from_polyline(self)
        elif mode == "AcDbPolygonMesh":
            return Polymesh.from_polyline(self)
        else:
            return self

    def transform(self, m: Matrix44) -> Polyline:
        """Transform the POLYLINE entity by transformation matrix `m` inplace.

        A non-uniform scaling is not supported if a 2D POLYLINE contains
        circular arc segments (bulges).

        Args:
            m: transformation :class:`~ezdxf.math.Matrix44`

        Raises:
            NonUniformScalingError: for non-uniform scaling of 2D POLYLINE
                containing circular arc segments (bulges)

        """

        def _ocs_locations(elevation):
            for vertex in self.vertices:
                location = vertex.dxf.location
                if elevation is not None:
                    # Older DXF versions may not have written the z-axis, so
                    # replace existing z-axis by the elevation value.
                    location = location.replace(z=elevation)
                yield location

        if self.is_2d_polyline:
            dxf = self.dxf
            ocs = OCSTransform(self.dxf.extrusion, m)
            if not ocs.scale_uniform and self.has_arc:
                raise NonUniformScalingError(
                    "2D POLYLINE containing arcs (bulges) does not support non uniform scaling"
                )
                # The caller function has to catch this exception and explode the
                # 2D POLYLINE into LINE and ELLIPSE entities.
            if dxf.hasattr("elevation"):
                z_axis = dxf.elevation.z
            else:
                z_axis = None
            vertices = [
                ocs.transform_vertex(vertex) for vertex in _ocs_locations(z_axis)
            ]

            # All vertices of a 2D polyline must have the same z-axis, which is
            # the elevation of the polyline:
            if vertices:
                dxf.elevation = vertices[0].replace(x=0.0, y=0.0)

            for vertex, location in zip(self.vertices, vertices):
                vdxf = vertex.dxf
                vdxf.location = location
                if vdxf.hasattr("start_width"):
                    vdxf.start_width = ocs.transform_width(vdxf.start_width)
                if vdxf.hasattr("end_width"):
                    vdxf.end_width = ocs.transform_width(vdxf.end_width)

            if dxf.hasattr("default_start_width"):
                dxf.default_start_width = ocs.transform_width(dxf.default_start_width)
            if dxf.hasattr("default_end_width"):
                dxf.default_end_width = ocs.transform_width(dxf.default_end_width)
            if dxf.hasattr("thickness"):
                dxf.thickness = ocs.transform_thickness(dxf.thickness)

            dxf.extrusion = ocs.new_extrusion
        else:
            for vertex in self.vertices:
                vertex.transform(m)
        self.post_transform(m)
        return self

    def explode(self, target_layout: Optional[BaseLayout] = None) -> EntityQuery:
        """Explode the POLYLINE entity as DXF primitives (LINE, ARC or 3DFACE)
        into the target layout, if the target layout is ``None``, the target
        layout is the layout of the POLYLINE entity.

        Returns an :class:`~ezdxf.query.EntityQuery` container referencing all
        DXF primitives.

        Args:
            target_layout: target layout for DXF primitives, ``None`` for same
                layout as source entity.

        """
        return explode_entity(self, target_layout)

    def virtual_entities(self) -> Iterator[Union[Line, Arc, Face3d]]:
        """Yields the graphical representation of POLYLINE as virtual DXF
        primitives (LINE, ARC or 3DFACE).

        These virtual entities are located at the original location, but are not
        stored in the entity database, have no handle and are not assigned to
        any layout.

        """
        for e in virtual_polyline_entities(self):
            e.set_source_of_copy(self)
            yield e

    def audit(self, auditor: Auditor) -> None:
        """Audit and repair the POLYLINE entity."""

        def audit_sub_entity(entity):
            entity.doc = doc  # grant same document
            dxf = entity.dxf
            if dxf.owner != owner:
                dxf.owner = owner
            if dxf.layer != layer:
                dxf.layer = layer

        doc = self.doc
        owner = self.dxf.handle
        layer = self.dxf.layer
        for vertex in self.vertices:
            audit_sub_entity(vertex)

        seqend = self.seqend
        if seqend:
            audit_sub_entity(seqend)
        elif doc:
            self.new_seqend()
            auditor.fixed_error(
                code=AuditError.MISSING_REQUIRED_SEQEND,
                message=f"Created required SEQEND entity for {str(self)}.",
                dxf_entity=self,
            )


class Polyface(Polyline):
    """
    PolyFace structure:

    POLYLINE
      AcDbEntity
      AcDbPolyFaceMesh
    VERTEX - Vertex
      AcDbEntity
      AcDbVertex
      AcDbPolyFaceMeshVertex
    VERTEX - Face
      AcDbEntity
      AcDbFaceRecord
    SEQEND

    Order of mesh_vertices and face_records is important (DXF R2010):

        1. mesh_vertices: the polyface mesh vertex locations
        2. face_records: indices of the face forming vertices

    """

    @classmethod
    def from_polyline(cls, polyline: Polyline) -> Polyface:
        polyface = cls.shallow_copy(polyline)
        polyface._sub_entities = polyline._sub_entities
        polyface.seqend = polyline.seqend
        # do not destroy polyline - all data would be lost
        return polyface

    def append_face(self, face: FaceType, dxfattribs=None) -> None:
        """Append a single face. A `face` is a sequence of (x, y, z) tuples.

        Args:
            face: sequence of (x, y, z) tuples
            dxfattribs: dict of DXF attributes for the VERTEX objects

        """
        self.append_faces([face], dxfattribs)

    def _points_to_dxf_vertices(
        self, points: Iterable[UVec], dxfattribs
    ) -> list[DXFVertex]:
        """Convert (x, y, z) tuples into DXFVertex objects.

        Args:
            points: sequence of (x, y, z) tuples
            dxfattribs: dict of DXF attributes for the VERTEX entity

        """
        dxfattribs["flags"] = dxfattribs.get("flags", 0) | self.get_vertex_flags()

        # All vertices have to be on the same layer as the POLYLINE entity:
        dxfattribs["layer"] = self.get_dxf_attrib("layer", "0")
        vertices: list[DXFVertex] = []
        for point in points:
            dxfattribs["location"] = point
            vertices.append(
                cast("DXFVertex", self._new_compound_entity("VERTEX", dxfattribs))
            )
        return vertices

    def append_faces(self, faces: Iterable[FaceType], dxfattribs=None) -> None:
        """Append multiple `faces`. `faces` is a list of single faces and a
        single face is a sequence of (x, y, z) tuples.

        Args:
            faces: iterable of sequences of (x, y, z) tuples
            dxfattribs: dict of DXF attributes for the VERTEX entity

        """

        def new_face_record():
            dxfattribs["flags"] = const.VTX_3D_POLYFACE_MESH_VERTEX
            # location of face record vertex is always (0, 0, 0)
            dxfattribs["location"] = Vec3()
            return cast(DXFVertex, self._new_compound_entity("VERTEX", dxfattribs))

        dxfattribs = dict(dxfattribs or {})

        existing_vertices, existing_faces = self.indexed_faces()
        new_faces: list[FaceProxy] = []
        for face in faces:
            face_mesh_vertices = self._points_to_dxf_vertices(face, {})
            # Index of first new vertex
            index = len(existing_vertices)
            existing_vertices.extend(face_mesh_vertices)
            face_record = FaceProxy(new_face_record(), existing_vertices)

            # Set VERTEX indices:
            face_record.indices = tuple(range(index, index + len(face_mesh_vertices)))
            new_faces.append(face_record)
        self._rebuild(chain(existing_faces, new_faces))

    def _rebuild(self, faces: Iterable[FaceProxy], precision: int = 6) -> None:
        """Build a valid POLYFACE structure from `faces`.

        Args:
            faces: iterable of FaceProxy objects.

        """
        polyface_builder = PolyfaceBuilder(faces, precision=precision)
        # Why is list[DXFGraphic] incompatible to list[DXFVertex] when DXFVertex
        # is a subclass of DXFGraphic?
        self._sub_entities = polyface_builder.get_vertices()  # type: ignore
        self.update_count(polyface_builder.nvertices, polyface_builder.nfaces)

    def update_count(self, nvertices: int, nfaces: int) -> None:
        self.dxf.m_count = nvertices
        self.dxf.n_count = nfaces

    def optimize(self, precision: int = 6) -> None:
        """Rebuilds the :class:`Polyface` by merging vertices with nearly same vertex
        locations.

        Args:
            precision: floating point precision for determining identical
                vertex locations

        """
        vertices, faces = self.indexed_faces()
        self._rebuild(faces, precision)

    def faces(self) -> Iterator[list[DXFVertex]]:
        """Iterable of all faces, a face is a tuple of vertices.

        Returns:
             list of [vertex, vertex, vertex, [vertex,] face_record]

        """
        _, faces = self.indexed_faces()
        for face in faces:
            face_vertices = list(face)
            face_vertices.append(face.face_record)
            yield face_vertices

    def indexed_faces(self) -> tuple[list[DXFVertex], Iterator[FaceProxy]]:
        """Returns a list of all vertices and a generator of FaceProxy()
        objects.

        (internal API)
        """
        vertices: list[DXFVertex] = []
        face_records: list[DXFVertex] = []
        for vertex in self.vertices:
            (vertices if vertex.is_poly_face_mesh_vertex else face_records).append(
                vertex
            )

        faces = (FaceProxy(face_record, vertices) for face_record in face_records)
        return vertices, faces


class FaceProxy:
    """Represents a single face of a polyface structure. (internal class)

    vertices:

        List of all polyface vertices.

    face_record:

        The face forming vertex of type ``AcDbFaceRecord``, contains the indices
        to the face building vertices. Indices of the DXF structure are 1-based
        and a negative index indicates the beginning of an invisible edge.
        Face.face_record.dxf.color determines the color of the face.

    indices:

        Indices to the face building vertices as tuple. This indices are 0-base
        and are used to get vertices from the list `Face.vertices`.

    """

    __slots__ = ("vertices", "face_record", "indices")

    def __init__(self, face_record: DXFVertex, vertices: Sequence[DXFVertex]):
        """Returns iterable of all face vertices as :class:`Vertex` entities."""
        self.vertices: Sequence[DXFVertex] = vertices
        self.face_record: DXFVertex = face_record
        self.indices: Sequence[int] = self._indices()

    def __len__(self) -> int:
        """Returns count of face vertices (without face_record)."""
        return len(self.indices)

    def __getitem__(self, pos: int) -> DXFVertex:
        """Returns :class:`Vertex` at position `pos`.

        Args:
            pos: vertex position 0-based

        """
        return self.vertices[self.indices[pos]]

    def __iter__(self) -> Iterator["DXFVertex"]:
        return (self.vertices[index] for index in self.indices)

    def points(self) -> Iterator[UVec]:
        """Returns iterable of all face vertex locations as (x, y, z)-tuples."""
        return (vertex.dxf.location for vertex in self)

    def _raw_indices(self) -> Iterable[int]:
        return (self.face_record.get_dxf_attrib(name, 0) for name in const.VERTEXNAMES)

    def _indices(self) -> Sequence[int]:
        return tuple(abs(index) - 1 for index in self._raw_indices() if index != 0)

    def is_edge_visible(self, pos: int) -> bool:
        """Returns ``True`` if edge starting at vertex `pos` is visible.

        Args:
            pos: vertex position 0-based

        """
        name = const.VERTEXNAMES[pos]
        return self.face_record.get_dxf_attrib(name) > 0


class PolyfaceBuilder:
    """Optimized POLYFACE builder. (internal class)"""

    def __init__(self, faces: Iterable[FaceProxy], precision: int = 6):
        self.precision: int = precision
        self.faces: list[DXFVertex] = []
        self.vertices: list[DXFVertex] = []
        self.index_mapping: dict[tuple[float, ...], int] = {}
        self.build(faces)

    @property
    def nvertices(self) -> int:
        return len(self.vertices)

    @property
    def nfaces(self) -> int:
        return len(self.faces)

    def get_vertices(self) -> list[DXFVertex]:
        vertices = self.vertices[:]
        vertices.extend(self.faces)
        return vertices

    def build(self, faces: Iterable[FaceProxy]) -> None:
        for face in faces:
            face_record = face.face_record
            for vertex, name in zip(face, VERTEXNAMES):
                index = self.add(vertex)
                # preserve sign of old index value
                sign = -1 if face_record.dxf.get(name, 0) < 0 else +1
                face_record.dxf.set(name, (index + 1) * sign)
            self.faces.append(face_record)

    def add(self, vertex: DXFVertex) -> int:
        def key(point):
            return tuple((round(coord, self.precision) for coord in point))

        location = key(vertex.dxf.location)
        try:
            return self.index_mapping[location]
        except KeyError:
            index = len(self.vertices)
            self.index_mapping[location] = index
            self.vertices.append(vertex)
            return index


class Polymesh(Polyline):
    """
    PolyMesh structure:

    POLYLINE
      AcDbEntity
      AcDbPolygonMesh
    VERTEX
      AcDbEntity
      AcDbVertex
      AcDbPolygonMeshVertex
    """

    @classmethod
    def from_polyline(cls, polyline: Polyline) -> Polymesh:
        polymesh = cls.shallow_copy(polyline)
        polymesh._sub_entities = polyline._sub_entities
        polymesh.seqend = polyline.seqend
        return polymesh

    def set_mesh_vertex(self, pos: tuple[int, int], point: UVec, dxfattribs=None):
        """Set location and DXF attributes of a single mesh vertex.

        Args:
            pos: 0-based (row, col) tuple, position of mesh vertex
            point: (x, y, z) tuple, new 3D coordinates of the mesh vertex
            dxfattribs: dict of DXF attributes

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["location"] = point
        vertex = self.get_mesh_vertex(pos)
        vertex.update_dxf_attribs(dxfattribs)

    def get_mesh_vertex(self, pos: tuple[int, int]) -> DXFVertex:
        """Get location of a single mesh vertex.

        Args:
            pos: 0-based (row, col) tuple, position of mesh vertex

        """
        m_count = self.dxf.m_count
        n_count = self.dxf.n_count
        m, n = pos
        if 0 <= m < m_count and 0 <= n < n_count:
            return self.vertices[m * n_count + n]
        else:
            raise const.DXFIndexError(repr(pos))

    def get_mesh_vertex_cache(self) -> MeshVertexCache:
        """Get a :class:`MeshVertexCache` object for this POLYMESH.
        The caching object provides fast access to the :attr:`location`
        attribute of mesh vertices.

        """
        return MeshVertexCache(self)


class MeshVertexCache:
    """Cache mesh vertices in a dict, keys are 0-based (row, col)-tuples.

    vertices:
        Dict of mesh vertices, keys are 0-based (row, col)-tuples. Writing to
        this dict doesn't change the DXF entity.

    """

    __slots__ = ("vertices",)

    def __init__(self, mesh: Polyline):
        self.vertices: dict[tuple[int, int], DXFVertex] = self._setup(
            mesh, mesh.dxf.m_count, mesh.dxf.n_count
        )

    def _setup(self, mesh: Polyline, m_count: int, n_count: int) -> dict:
        cache: dict[tuple[int, int], DXFVertex] = {}
        vertices = iter(mesh.vertices)
        for m in range(m_count):
            for n in range(n_count):
                cache[(m, n)] = next(vertices)
        return cache

    def __getitem__(self, pos: tuple[int, int]) -> UVec:
        """Get mesh vertex location as (x, y, z)-tuple.

        Args:
            pos: 0-based (row, col)-tuple.

        """
        try:
            return self.vertices[pos].dxf.location
        except KeyError:
            raise const.DXFIndexError(repr(pos))

    def __setitem__(self, pos: tuple[int, int], location: UVec) -> None:
        """Get mesh vertex location as (x, y, z)-tuple.

        Args:
            pos: 0-based (row, col)-tuple.
            location: (x, y, z)-tuple

        """
        try:
            self.vertices[pos].dxf.location = location
        except KeyError:
            raise const.DXFIndexError(repr(pos))


acdb_vertex = DefSubclass(
    "AcDbVertex",
    {  # last subclass index -1
        # Location point in OCS if 2D, and WCS if 3D
        "location": DXFAttr(10, xtype=XType.point3d),
        "start_width": DXFAttr(40, default=0, optional=True),
        "end_width": DXFAttr(41, default=0, optional=True),
        # Bulge (optional; default is 0). The bulge is the tangent of one fourth
        # the included angle for an arc segment, made negative if the arc goes
        # clockwise from the start point to the endpoint. A bulge of 0 indicates
        # a straight segment, and a bulge of 1 is a semicircle.
        "bulge": DXFAttr(42, default=0, optional=True),
        "flags": DXFAttr(70, default=0),
        # Curve fit tangent direction (in degrees)
        "tangent": DXFAttr(50, optional=True),
        "vtx0": DXFAttr(71, optional=True),
        "vtx1": DXFAttr(72, optional=True),
        "vtx2": DXFAttr(73, optional=True),
        "vtx3": DXFAttr(74, optional=True),
        "vertex_identifier": DXFAttr(91, optional=True),
    },
)
acdb_vertex_group_codes = group_code_mapping(acdb_vertex)
merged_vertex_group_codes = merge_group_code_mappings(
    acdb_entity_group_codes, acdb_vertex_group_codes  # type: ignore
)


@factory.register_entity
class DXFVertex(DXFGraphic):
    """DXF VERTEX entity"""

    DXFTYPE = "VERTEX"

    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_vertex)
    # Extra vertex created by curve-fitting:
    EXTRA_VERTEX_CREATED = 1

    # Curve-fit tangent defined for this vertex. A curve-fit tangent direction
    # of 0 may be omitted from the DXF output, but is significant if this bit
    # is set:
    CURVE_FIT_TANGENT = 2

    # 4 = unused, never set in dxf files
    # Spline vertex created by spline-fitting
    SPLINE_VERTEX_CREATED = 8
    SPLINE_FRAME_CONTROL_POINT = 16
    POLYLINE_3D_VERTEX = 32
    POLYGON_MESH_VERTEX = 64
    POLYFACE_MESH_VERTEX = 128
    FACE_FLAGS = POLYGON_MESH_VERTEX + POLYFACE_MESH_VERTEX
    VTX3D = POLYLINE_3D_VERTEX + POLYGON_MESH_VERTEX + POLYFACE_MESH_VERTEX

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Loading interface. (internal API)"""
        # bypass DXFGraphic, loading proxy graphic is skipped!
        dxf = super(DXFGraphic, self).load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, merged_vertex_group_codes)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        # VERTEX can have 3 subclasses if representing a `face record` or
        # 4 subclasses if representing a vertex location, just the last
        # subclass contains data
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            if self.is_face_record:
                tagwriter.write_tag2(SUBCLASS_MARKER, "AcDbFaceRecord")
            else:
                tagwriter.write_tag2(SUBCLASS_MARKER, "AcDbVertex")
                if self.is_3d_polyline_vertex:
                    tagwriter.write_tag2(SUBCLASS_MARKER, "AcDb3dPolylineVertex")
                elif self.is_poly_face_mesh_vertex:
                    tagwriter.write_tag2(SUBCLASS_MARKER, "AcDbPolyFaceMeshVertex")
                elif self.is_polygon_mesh_vertex:
                    tagwriter.write_tag2(SUBCLASS_MARKER, "AcDbPolygonMeshVertex")
                else:
                    tagwriter.write_tag2(SUBCLASS_MARKER, "AcDb2dVertex")

        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "location",
                "start_width",
                "end_width",
                "bulge",
                "flags",
                "tangent",
                "vtx0",
                "vtx1",
                "vtx2",
                "vtx3",
                "vertex_identifier",
            ],
        )

    @property
    def is_2d_polyline_vertex(self) -> bool:
        return self.dxf.flags & self.VTX3D == 0

    @property
    def is_3d_polyline_vertex(self) -> bool:
        return self.dxf.flags & self.POLYLINE_3D_VERTEX

    @property
    def is_polygon_mesh_vertex(self) -> bool:
        return self.dxf.flags & self.POLYGON_MESH_VERTEX

    @property
    def is_poly_face_mesh_vertex(self) -> bool:
        return self.dxf.flags & self.FACE_FLAGS == self.FACE_FLAGS

    @property
    def is_face_record(self) -> bool:
        return (self.dxf.flags & self.FACE_FLAGS) == self.POLYFACE_MESH_VERTEX

    def transform(self, m: Matrix44) -> DXFVertex:
        """Transform the VERTEX entity by transformation matrix `m` inplace."""
        if self.is_face_record:
            return self
        self.dxf.location = m.transform(self.dxf.location)
        return self

    def format(self, format="xyz") -> Sequence:
        """Return formatted vertex components as tuple.

        Format codes:

            - ``x`` = x-coordinate
            - ``y`` = y-coordinate
            - ``z`` = z-coordinate
            - ``s`` = start width
            - ``e`` = end width
            - ``b`` = bulge value
            - ``v`` = (x, y, z) as tuple

        Args:
            format: format string, default is "xyz"

        """
        dxf = self.dxf
        v = Vec3(dxf.location)
        x, y, z = v.xyz
        b = dxf.bulge
        s = dxf.start_width
        e = dxf.end_width
        vars = locals()
        return tuple(vars[code] for code in format.lower())


def vertex_attribs(data: Sequence, format="xyseb") -> dict:
    """Create VERTEX attributes from input data.

    Format codes:

        - ``x`` = x-coordinate
        - ``y`` = y-coordinate
        - ``s`` = start width
        - ``e`` = end width
        - ``b`` = bulge value
        - ``v`` = (x, y [,z]) tuple (z-axis is ignored)

    Args:
        data: list or tuple of point components
        format: format string, default is 'xyseb'

    Returns:
       dict with keys: 'location', 'bulge', 'start_width', 'end_width'

    """
    attribs = dict()
    format = [code for code in format.lower() if code in FORMAT_CODES]
    location = Vec3()
    for code, value in zip(format, data):
        if code not in FORMAT_CODES:
            continue
        if code == "v":
            location = Vec3(value)
        elif code == "b":
            attribs["bulge"] = float(value)
        elif code == "s":
            attribs["start_width"] = float(value)
        elif code == "e":
            attribs["end_width"] = float(value)
        elif code == "x":
            location = location.replace(x=float(value))
        elif code == "y":
            location = location.replace(y=float(value))
    attribs["location"] = location
    return attribs
