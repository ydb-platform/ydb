# Copyright (c) 2019-2022 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
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
from ezdxf.math import Matrix44, Z_AXIS, NULLVEC, Vec3
from ezdxf.math.transformtools import OCSTransform
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import (
    DXFGraphic,
    acdb_entity,
    elevation_to_z_axis,
    acdb_entity_group_codes,
)
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.entities import DXFNamespace

__all__ = ["Solid", "Trace", "Face3d"]

acdb_trace = DefSubclass(
    "AcDbTrace",
    {
        # IMPORTANT: all 4 vertices have to be present in the DXF file,
        # otherwise AutoCAD shows a DXF structure error and does not load the
        # file! (SOLID, TRACE and 3DFACE)
        # 1. corner Solid WCS; Trace OCS
        "vtx0": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        # 2. corner Solid WCS; Trace OCS
        "vtx1": DXFAttr(11, xtype=XType.point3d, default=NULLVEC),
        # 3. corner Solid WCS; Trace OCS
        "vtx2": DXFAttr(12, xtype=XType.point3d, default=NULLVEC),
        # 4. corner Solid WCS; Trace OCS:
        # If only three corners are entered to define the SOLID, then the fourth
        # corner coordinate is the same as the third.
        "vtx3": DXFAttr(13, xtype=XType.point3d, default=NULLVEC),
        # IMPORTANT: for TRACE and SOLID the last two vertices are in reversed
        # order: a square has the vertex order 0-1-3-2
        # Elevation is a legacy feature from R11 and prior, do not use this
        # attribute, store the entity elevation in the z-axis of the vertices.
        # ezdxf does not export the elevation attribute!
        "elevation": DXFAttr(38, default=0, optional=True),
        # Thickness could be negative:
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
acdb_trace_group_codes = group_code_mapping(acdb_trace)
merged_trace_group_codes = merge_group_code_mappings(
    acdb_entity_group_codes, acdb_trace_group_codes  # type: ignore
)


class _Base(DXFGraphic):
    def __getitem__(self, num):
        return self.dxf.get(VERTEXNAMES[num])

    def __setitem__(self, num, value):
        return self.dxf.set(VERTEXNAMES[num], value)


@register_entity
class Solid(_Base):
    """DXF SHAPE entity"""

    DXFTYPE = "SOLID"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_trace)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Loading interface. (internal API)"""
        # bypass DXFGraphic, loading proxy graphic is skipped!
        dxf = super(DXFGraphic, self).load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, merged_trace_group_codes)
            if processor.r12:
                # Transform elevation attribute from R11 to z-axis values:
                elevation_to_z_axis(dxf, VERTEXNAMES)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags. (internal API)"""
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_trace.name)
        if not self.dxf.hasattr("vtx3"):
            self.dxf.vtx3 = self.dxf.vtx2
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "vtx0",
                "vtx1",
                "vtx2",
                "vtx3",
                "thickness",
                "extrusion",
            ],
        )

    def transform(self, m: Matrix44) -> Solid:
        """Transform the SOLID/TRACE entity by transformation matrix `m` inplace."""
        # SOLID and TRACE are OCS entities.
        dxf = self.dxf
        ocs = OCSTransform(self.dxf.extrusion, m)
        for name in VERTEXNAMES:
            if dxf.hasattr(name):
                dxf.set(name, ocs.transform_vertex(dxf.get(name)))
        if dxf.hasattr("thickness"):
            dxf.thickness = ocs.transform_thickness(dxf.thickness)
        dxf.extrusion = ocs.new_extrusion
        self.post_transform(m)
        return self

    def wcs_vertices(self, close: bool = False) -> list[Vec3]:
        """Returns WCS vertices in correct order,
        if argument `close` is ``True``, last vertex == first vertex.
        Does **not** return the duplicated last vertex if the entity represents
        a triangle.

        """
        ocs = self.ocs()
        return list(ocs.points_to_wcs(self.vertices(close)))

    def vertices(self, close: bool = False) -> list[Vec3]:
        """Returns OCS vertices in correct order,
        if argument `close` is ``True``, last vertex == first vertex.
        Does **not** return the duplicated last vertex if the entity represents
        a triangle.

        """
        dxf = self.dxf
        vertices: list[Vec3] = [dxf.vtx0, dxf.vtx1, dxf.vtx2]
        if dxf.vtx3 != dxf.vtx2:  # face is not a triangle
            vertices.append(dxf.vtx3)

        # adjust weird vertex order of SOLID and TRACE:
        # 0, 1, 2, 3 -> 0, 1, 3, 2
        if len(vertices) > 3:
            vertices[2], vertices[3] = vertices[3], vertices[2]

        if close and not vertices[0].isclose(vertices[-1]):
            vertices.append(vertices[0])
        return vertices


@register_entity
class Trace(Solid):
    """DXF TRACE entity"""

    DXFTYPE = "TRACE"


acdb_face = DefSubclass(
    "AcDbFace",
    {
        # 1. corner WCS:
        "vtx0": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        # 2. corner WCS:
        "vtx1": DXFAttr(11, xtype=XType.point3d, default=NULLVEC),
        # 3. corner WCS:
        "vtx2": DXFAttr(12, xtype=XType.point3d, default=NULLVEC),
        # 4. corner WCS:
        # If only three corners are entered to define the SOLID, then the fourth
        # corner coordinate is the same as the third.
        "vtx3": DXFAttr(13, xtype=XType.point3d, default=NULLVEC),
        # invisible:
        # 1 = First edge is invisible
        # 2 = Second edge is invisible
        # 4 = Third edge is invisible
        # 8 = Fourth edge is invisible
        "invisible_edges": DXFAttr(70, default=0, optional=True),
    },
)
acdb_face_group_codes = group_code_mapping(acdb_face)
merged_face_group_codes = merge_group_code_mappings(
    acdb_entity_group_codes, acdb_face_group_codes  # type: ignore
)


@register_entity
class Face3d(_Base):
    """DXF 3DFACE entity"""

    # IMPORTANT: for 3DFACE the last two vertices are in regular order:
    # a square has the vertex order 0-1-2-3

    DXFTYPE = "3DFACE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_face)

    def is_invisible_edge(self, num: int) -> bool:
        """Returns True if edge `num` is an invisible edge."""
        if num < 0 or num > 4:
            raise ValueError(f"invalid edge: {num}")
        return bool(self.dxf.invisible_edges & (1 << num))

    def set_edge_visibility(self, num: int, visible: bool = False) -> None:
        """Set visibility of edge `num`, status `True` for visible, status
        `False` for invisible.
        """
        if num < 0 or num >= 4:
            raise ValueError(f"invalid edge: {num}")
        if not visible:
            self.dxf.invisible_edges = self.dxf.invisible_edges | (1 << num)
        else:
            self.dxf.invisible_edges = self.dxf.invisible_edges & ~(1 << num)

    def get_edges_visibility(self) -> list[bool]:
        # if the face is a triangle, a fourth visibility flag
        # may be present but is ignored
        return [not self.is_invisible_edge(i) for i in range(4)]

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Loading interface. (internal API)"""
        # bypass DXFGraphic, loading proxy graphic is skipped!
        dxf = super(DXFGraphic, self).load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, merged_face_group_codes)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_face.name)
        if not self.dxf.hasattr("vtx3"):
            self.dxf.vtx3 = self.dxf.vtx2
        self.dxf.export_dxf_attribs(
            tagwriter, ["vtx0", "vtx1", "vtx2", "vtx3", "invisible_edges"]
        )

    def transform(self, m: Matrix44) -> Face3d:
        """Transform the 3DFACE  entity by transformation matrix `m` inplace."""
        dxf = self.dxf
        # 3DFACE is a real 3d entity
        dxf.vtx0, dxf.vtx1, dxf.vtx2, dxf.vtx3 = m.transform_vertices(
            (dxf.vtx0, dxf.vtx1, dxf.vtx2, dxf.vtx3)
        )
        self.post_transform(m)
        return self

    def wcs_vertices(self, close: bool = False) -> list[Vec3]:
        """Returns WCS vertices, if argument `close` is ``True``,
        the first vertex is also returned as closing last vertex.

        Returns 4 vertices when `close` is ``False`` and 5 vertices when `close` is
        ``True``.  Some edges may have zero-length.  This is a compatibility interface
        to SOLID and TRACE. The 3DFACE entity is already defined by WCS vertices.
        """
        dxf = self.dxf
        vertices: list[Vec3] = [dxf.vtx0, dxf.vtx1, dxf.vtx2]
        vtx3 = dxf.get("vtx3")
        if (
            isinstance(vtx3, Vec3) and vtx3 != dxf.vtx2
        ):  # face is not a triangle
            vertices.append(vtx3)
        if close:
            vertices.append(vertices[0])
        return vertices
