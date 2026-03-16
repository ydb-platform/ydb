# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Union, Optional, Sequence, Any
from typing_extensions import Self, override
import logging

from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    group_code_mapping,
)
from ezdxf.lldxf import const
from ezdxf.lldxf.tags import Tags, DXFTag
from ezdxf.math import Matrix44
from ezdxf.tools import crypt, guid
from ezdxf import msgtypes

from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import DXFGraphic, acdb_entity
from .factory import register_entity
from .copy import default_copy
from .temporary_transform import TransformByBlockReference

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf import xref


__all__ = [
    "Body",
    "Solid3d",
    "Region",
    "Surface",
    "ExtrudedSurface",
    "LoftedSurface",
    "RevolvedSurface",
    "SweptSurface",
]

logger = logging.getLogger("ezdxf")
acdb_modeler_geometry = DefSubclass(
    "AcDbModelerGeometry",
    {
        "version": DXFAttr(70, default=1),
        "flags": DXFAttr(290, dxfversion=const.DXF2013),
        "uid": DXFAttr(2, dxfversion=const.DXF2013),
    },
)
acdb_modeler_geometry_group_codes = group_code_mapping(acdb_modeler_geometry)

# with R2013/AC1027 Modeler Geometry of ACIS data is stored in the ACDSDATA
# section as binary encoded information detection:
# group code 70, 1, 3 is missing
# group code 290, 2 present
#
#   0
# ACDSRECORD
#  90
# 1
#   2
# AcDbDs::ID
# 280
# 10
# 320
# 19B   <<< handle of associated 3DSOLID entity in model space
#   2
# ASM_Data
# 280
# 15
#  94
# 7197  <<< size in bytes ???
# 310
# 414349532042696E61727946696C6...


@register_entity
class Body(DXFGraphic):
    """DXF BODY entity - container entity for embedded ACIS data."""

    DXFTYPE = "BODY"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_modeler_geometry)
    MIN_DXF_VERSION_FOR_EXPORT = const.DXF2000

    def __init__(self) -> None:
        super().__init__()
        # Store SAT data as immutable sequence of strings, so the data can be shared
        # across multiple copies of an ACIS entity.
        self._sat: Sequence[str] = tuple()
        self._sab: bytes = b""
        self._update = False
        self._temporary_transformation = TransformByBlockReference()

    @property
    def acis_data(self) -> Union[bytes, Sequence[str]]:
        """Returns :term:`SAT` data  for DXF R2000 up to R2010 and :term:`SAB`
        data for DXF R2013 and later
        """
        if self.has_binary_data:
            return self.sab
        return self.sat

    @property
    def sat(self) -> Sequence[str]:
        """Get/Set :term:`SAT` data as sequence of strings."""
        return self._sat

    @sat.setter
    def sat(self, data: Sequence[str]) -> None:
        """Set :term:`SAT` data as sequence of strings."""
        self._sat = tuple(data)

    @property
    def sab(self) -> bytes:
        """Get/Set :term:`SAB` data as bytes."""
        if (  # load SAB data on demand
            self.doc is not None and self.has_binary_data and len(self._sab) == 0
        ):
            self._sab = self.doc.acdsdata.get_acis_data(self.dxf.handle)
        return self._sab

    @sab.setter
    def sab(self, data: bytes) -> None:
        """Set :term:`SAB` data as bytes."""
        self._update = True
        self._sab = data

    @property
    def has_binary_data(self):
        """Returns ``True`` if the entity contains :term:`SAB` data and
        ``False`` if the entity contains :term:`SAT` data.
        """
        if self.doc:
            return self.doc.dxfversion >= const.DXF2013
        else:
            return False

    @override
    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, Body)
        entity.sat = self.sat
        entity.sab = self.sab  # load SAB on demand
        entity.dxf.uid = guid()
        entity._temporary_transformation = self._temporary_transformation

    @override
    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        super().map_resources(clone, mapping)
        clone.convert_acis_data()

    def convert_acis_data(self) -> None:
        if self.doc is None:
            return
        msg = ""
        dxfversion = self.doc.dxfversion
        if dxfversion < const.DXF2013:
            if self._sab:
                self._sab = b""
                msg = "DXF version mismatch, can't convert ACIS data from SAB to SAT, SAB data removed."
        else:
            if self._sat:
                self._sat = tuple()
                msg = "DXF version mismatch, can't convert ACIS data from SAT to SAB, SAT data removed."
        if msg:
            logger.info(msg)

    @override
    def notify(self, message_type: int, data: Any = None) -> None:
        if message_type == msgtypes.COMMIT_PENDING_CHANGES:
            self._temporary_transformation.apply_transformation(self)

    @override
    def preprocess_export(self, tagwriter: AbstractTagWriter) -> bool:
        msg = ""
        if tagwriter.dxfversion < const.DXF2013:
            valid = len(self.sat) > 0
            if not valid:
                msg = f"{str(self)} doesn't have SAT data, skipping DXF export"
        else:
            valid = len(self.sab) > 0
            if not valid:
                msg = f"{str(self)} doesn't have SAB data, skipping DXF export"
        if not valid:
            logger.info(msg)
        if valid and self._temporary_transformation.get_matrix() is not None:
            logger.warning(f"{str(self)} has unapplied temporary transformations.")
        return valid

    @override
    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Loading interface. (internal API)"""
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(
                dxf, acdb_modeler_geometry_group_codes, 2, log=False
            )
            if not self.has_binary_data:
                self.load_sat_data(processor.subclasses[2])
        return dxf

    def load_sat_data(self, tags: Tags):
        """Loading interface. (internal API)"""
        text_lines = tags2textlines(tag for tag in tags if tag.code in (1, 3))
        self._sat = tuple(crypt.decode(text_lines))

    @override
    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags. (internal API)"""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_modeler_geometry.name)
        if tagwriter.dxfversion >= const.DXF2013:
            # ACIS data is stored in the ACDSDATA section as SAB
            if self.doc and self._update:
                # write back changed SAB data into AcDsDataSection or create
                # a new ACIS record:
                self.doc.acdsdata.set_acis_data(self.dxf.handle, self.sab)
            if self.dxf.hasattr("version"):
                tagwriter.write_tag2(70, self.dxf.version)
            self.dxf.export_dxf_attribs(tagwriter, ["flags", "uid"])
        else:
            # DXF R2000 - R2010 stores the ACIS data as SAT in the entity
            self.dxf.export_dxf_attribs(tagwriter, "version")
            self.export_sat_data(tagwriter)

    def export_sat_data(self, tagwriter: AbstractTagWriter) -> None:
        """Export ACIS data as DXF tags. (internal API)"""

        def cleanup(lines):
            for line in lines:
                yield line.rstrip().replace("\n", "")

        tags = Tags(textlines2tags(crypt.encode(cleanup(self.sat))))
        tagwriter.write_tags(tags)

    def tostring(self) -> str:
        """Returns ACIS :term:`SAT` data as a single string if the entity has
        SAT data.
        """
        if self.has_binary_data:
            return ""
        else:
            return "\n".join(self.sat)

    @override
    def destroy(self) -> None:
        if self.has_binary_data:
            self.doc.acdsdata.del_acis_data(self.dxf.handle)  # type: ignore
        super().destroy()

    @override
    def transform(self, m: Matrix44) -> Self:
        self._temporary_transformation.add_matrix(m)
        return self

    def temporary_transformation(self) -> TransformByBlockReference:
        return self._temporary_transformation


def tags2textlines(tags: Iterable) -> Iterable[str]:
    """Yields text lines from code 1 and 3 tags, code 1 starts a line following
    code 3 tags are appended to the line.
    """
    line = None
    for code, value in tags:
        if code == 1:
            if line is not None:
                yield line
            line = value
        elif code == 3:
            line += value
    if line is not None:
        yield line


def textlines2tags(lines: Iterable[str]) -> Iterable[DXFTag]:
    """Yields text lines as DXFTags, splitting long lines (>255) int code 1
    and code 3 tags.
    """
    for line in lines:
        text = line[:255]
        tail = line[255:]
        yield DXFTag(1, text)
        while len(tail):
            text = tail[:255]
            tail = tail[255:]
            yield DXFTag(3, text)


@register_entity
class Region(Body):
    """DXF REGION entity - container entity for embedded ACIS data."""

    DXFTYPE = "REGION"


acdb_3dsolid = DefSubclass(
    "AcDb3dSolid",
    {
        "history_handle": DXFAttr(350, default="0"),
    },
)
acdb_3dsolid_group_codes = group_code_mapping(acdb_3dsolid)


@register_entity
class Solid3d(Body):
    """DXF 3DSOLID entity - container entity for embedded ACIS data."""

    DXFTYPE = "3DSOLID"
    DXFATTRIBS = DXFAttributes(
        base_class, acdb_entity, acdb_modeler_geometry, acdb_3dsolid
    )

    @override
    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_3dsolid_group_codes, 3)
        return dxf

    @override
    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        # base class export is done by parent class
        super().export_entity(tagwriter)
        # AcDbEntity export is done by parent class
        # AcDbModelerGeometry export is done by parent class
        if tagwriter.dxfversion > const.DXF2004:
            tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_3dsolid.name)
            self.dxf.export_dxf_attribs(tagwriter, "history_handle")


def load_matrix(subclass: Tags, code: int) -> Matrix44:
    values = [tag.value for tag in subclass.find_all(code)]
    if len(values) != 16:
        raise const.DXFStructureError("Invalid transformation matrix.")
    return Matrix44(values)


def export_matrix(tagwriter: AbstractTagWriter, code: int, matrix: Matrix44) -> None:
    for value in list(matrix):
        tagwriter.write_tag2(code, value)


acdb_surface = DefSubclass(
    "AcDbSurface",
    {
        "u_count": DXFAttr(71),
        "v_count": DXFAttr(72),
    },
)
acdb_surface_group_codes = group_code_mapping(acdb_surface)


@register_entity
class Surface(Body):
    """DXF SURFACE entity - container entity for embedded ACIS data."""

    DXFTYPE = "SURFACE"
    DXFATTRIBS = DXFAttributes(
        base_class, acdb_entity, acdb_modeler_geometry, acdb_surface
    )

    @override
    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_surface_group_codes, 3)
        return dxf

    @override
    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        # base class export is done by parent class
        super().export_entity(tagwriter)
        # AcDbEntity export is done by parent class
        # AcDbModelerGeometry export is done by parent class
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_surface.name)
        self.dxf.export_dxf_attribs(tagwriter, ["u_count", "v_count"])


acdb_extruded_surface = DefSubclass(
    "AcDbExtrudedSurface",
    {
        "class_id": DXFAttr(90),
        "sweep_vector": DXFAttr(10, xtype=XType.point3d),
        # 16x group code 40: Transform matrix of extruded entity (16 floats;
        # row major format; default = identity matrix)
        "draft_angle": DXFAttr(42, default=0.0),  # in radians
        "draft_start_distance": DXFAttr(43, default=0.0),
        "draft_end_distance": DXFAttr(44, default=0.0),
        "twist_angle": DXFAttr(45, default=0.0),  # in radians?
        "scale_factor": DXFAttr(48, default=0.0),
        "align_angle": DXFAttr(49, default=0.0),  # in radians
        # 16x group code 46: Transform matrix of sweep entity (16 floats;
        # row major format; default = identity matrix)
        # 16x group code 47: Transform matrix of path entity (16 floats;
        # row major format; default = identity matrix)
        "solid": DXFAttr(290, default=0),  # bool
        # 0=No alignment; 1=Align sweep entity to path:
        "sweep_alignment_flags": DXFAttr(70, default=0),
        "unknown1": DXFAttr(71, default=0),
        # 2=Translate sweep entity to path; 3=Translate path to sweep entity:
        "align_start": DXFAttr(292, default=0),  # bool
        "bank": DXFAttr(293, default=0),  # bool
        "base_point_set": DXFAttr(294, default=0),  # bool
        "sweep_entity_transform_computed": DXFAttr(295, default=0),  # bool
        "path_entity_transform_computed": DXFAttr(296, default=0),  # bool
        "reference_vector_for_controlling_twist": DXFAttr(11, xtype=XType.point3d),
    },
)
acdb_extruded_surface_group_codes = group_code_mapping(acdb_extruded_surface)


@register_entity
class ExtrudedSurface(Surface):
    """DXF EXTRUDEDSURFACE entity - container entity for embedded ACIS data."""

    DXFTYPE = "EXTRUDEDSURFACE"
    DXFATTRIBS = DXFAttributes(
        base_class,
        acdb_entity,
        acdb_modeler_geometry,
        acdb_surface,
        acdb_extruded_surface,
    )

    def __init__(self):
        super().__init__()
        self.transformation_matrix_extruded_entity = Matrix44()
        self.sweep_entity_transformation_matrix = Matrix44()
        self.path_entity_transformation_matrix = Matrix44()

    @override
    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, ExtrudedSurface)
        super().copy_data(entity, copy_strategy)
        entity.transformation_matrix_extruded_entity = (
            self.transformation_matrix_extruded_entity.copy()
        )
        entity.sweep_entity_transformation_matrix = (
            self.sweep_entity_transformation_matrix.copy()
        )
        entity.path_entity_transformation_matrix = (
            self.path_entity_transformation_matrix.copy()
        )

    @override
    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(
                dxf, acdb_extruded_surface_group_codes, 4, log=False
            )
            self.load_matrices(processor.subclasses[4])
        return dxf

    def load_matrices(self, tags: Tags):
        self.transformation_matrix_extruded_entity = load_matrix(tags, code=40)
        self.sweep_entity_transformation_matrix = load_matrix(tags, code=46)
        self.path_entity_transformation_matrix = load_matrix(tags, code=47)

    @override
    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        # base class export is done by parent class
        super().export_entity(tagwriter)
        # AcDbEntity export is done by parent class
        # AcDbModelerGeometry export is done by parent class
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_extruded_surface.name)
        self.dxf.export_dxf_attribs(tagwriter, ["class_id", "sweep_vector"])
        export_matrix(
            tagwriter,
            code=40,
            matrix=self.transformation_matrix_extruded_entity,
        )
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "draft_angle",
                "draft_start_distance",
                "draft_end_distance",
                "twist_angle",
                "scale_factor",
                "align_angle",
            ],
        )
        export_matrix(
            tagwriter, code=46, matrix=self.sweep_entity_transformation_matrix
        )
        export_matrix(tagwriter, code=47, matrix=self.path_entity_transformation_matrix)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "solid",
                "sweep_alignment_flags",
                "unknown1",
                "align_start",
                "bank",
                "base_point_set",
                "sweep_entity_transform_computed",
                "path_entity_transform_computed",
                "reference_vector_for_controlling_twist",
            ],
        )


acdb_lofted_surface = DefSubclass(
    "AcDbLoftedSurface",
    {
        # 16x group code 40: Transform matrix of loft entity (16 floats;
        # row major format; default = identity matrix)
        "plane_normal_lofting_type": DXFAttr(70),
        "start_draft_angle": DXFAttr(41, default=0.0),  # in radians
        "end_draft_angle": DXFAttr(42, default=0.0),  # in radians
        "start_draft_magnitude": DXFAttr(43, default=0.0),
        "end_draft_magnitude": DXFAttr(44, default=0.0),
        "arc_length_parameterization": DXFAttr(290, default=0),  # bool
        "no_twist": DXFAttr(291, default=1),  # true/false
        "align_direction": DXFAttr(292, default=1),  # bool
        "simple_surfaces": DXFAttr(293, default=1),  # bool
        "closed_surfaces": DXFAttr(294, default=0),  # bool
        "solid": DXFAttr(295, default=0),  # true/false
        "ruled_surface": DXFAttr(296, default=0),  # bool
        "virtual_guide": DXFAttr(297, default=0),  # bool
    },
)
acdb_lofted_surface_group_codes = group_code_mapping(acdb_lofted_surface)


@register_entity
class LoftedSurface(Surface):
    """DXF LOFTEDSURFACE entity - container entity for embedded ACIS data."""

    DXFTYPE = "LOFTEDSURFACE"
    DXFATTRIBS = DXFAttributes(
        base_class,
        acdb_entity,
        acdb_modeler_geometry,
        acdb_surface,
        acdb_lofted_surface,
    )

    def __init__(self):
        super().__init__()
        self.transformation_matrix_lofted_entity = Matrix44()

    @override
    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, LoftedSurface)
        super().copy_data(entity, copy_strategy)
        entity.transformation_matrix_lofted_entity = (
            self.transformation_matrix_lofted_entity.copy()
        )

    @override
    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(
                dxf, acdb_lofted_surface_group_codes, 4, log=False
            )
            self.load_matrices(processor.subclasses[4])
        return dxf

    def load_matrices(self, tags: Tags):
        self.transformation_matrix_lofted_entity = load_matrix(tags, code=40)

    @override
    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        # base class export is done by parent class
        super().export_entity(tagwriter)
        # AcDbEntity export is done by parent class
        # AcDbModelerGeometry export is done by parent class
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_lofted_surface.name)
        export_matrix(
            tagwriter, code=40, matrix=self.transformation_matrix_lofted_entity
        )
        self.dxf.export_dxf_attribs(tagwriter, acdb_lofted_surface.attribs.keys())


acdb_revolved_surface = DefSubclass(
    "AcDbRevolvedSurface",
    {
        "class_id": DXFAttr(90, default=0.0),
        "axis_point": DXFAttr(10, xtype=XType.point3d),
        "axis_vector": DXFAttr(11, xtype=XType.point3d),
        "revolve_angle": DXFAttr(40),  # in radians
        "start_angle": DXFAttr(41),  # in radians
        # 16x group code 42: Transform matrix of revolved entity (16 floats;
        # row major format; default = identity matrix)
        "draft_angle": DXFAttr(43),  # in radians
        "start_draft_distance": DXFAttr(44, default=0),
        "end_draft_distance": DXFAttr(45, default=0),
        "twist_angle": DXFAttr(46, default=0),  # in radians
        "solid": DXFAttr(290, default=0),  # bool
        "close_to_axis": DXFAttr(291, default=0),  # bool
    },
)
acdb_revolved_surface_group_codes = group_code_mapping(acdb_revolved_surface)


@register_entity
class RevolvedSurface(Surface):
    """DXF REVOLVEDSURFACE entity - container entity for embedded ACIS data."""

    DXFTYPE = "REVOLVEDSURFACE"
    DXFATTRIBS = DXFAttributes(
        base_class,
        acdb_entity,
        acdb_modeler_geometry,
        acdb_surface,
        acdb_revolved_surface,
    )

    def __init__(self):
        super().__init__()
        self.transformation_matrix_revolved_entity = Matrix44()

    @override
    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, RevolvedSurface)
        super().copy_data(entity, copy_strategy)
        entity.transformation_matrix_revolved_entity = (
            self.transformation_matrix_revolved_entity.copy()
        )

    @override
    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(
                dxf, acdb_revolved_surface_group_codes, 4, log=False
            )
            self.load_matrices(processor.subclasses[4])
        return dxf

    def load_matrices(self, tags: Tags):
        self.transformation_matrix_revolved_entity = load_matrix(tags, code=42)

    @override
    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        # base class export is done by parent class
        super().export_entity(tagwriter)
        # AcDbEntity export is done by parent class
        # AcDbModelerGeometry export is done by parent class
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_revolved_surface.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "class_id",
                "axis_point",
                "axis_vector",
                "revolve_angle",
                "start_angle",
            ],
        )
        export_matrix(
            tagwriter,
            code=42,
            matrix=self.transformation_matrix_revolved_entity,
        )
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "draft_angle",
                "start_draft_distance",
                "end_draft_distance",
                "twist_angle",
                "solid",
                "close_to_axis",
            ],
        )


acdb_swept_surface = DefSubclass(
    "AcDbSweptSurface",
    {
        "swept_entity_id": DXFAttr(90),
        # 90: size of binary data (lost on saving)
        # 310: binary data  (lost on saving)
        "path_entity_id": DXFAttr(91),
        # 90: size of binary data  (lost on saving)
        # 310: binary data  (lost on saving)
        # 16x group code 40: Transform matrix of sweep entity (16 floats;
        # row major format; default = identity matrix)
        # 16x group code 41: Transform matrix of path entity (16 floats;
        # row major format; default = identity matrix)
        "draft_angle": DXFAttr(42),  # in radians
        "draft_start_distance": DXFAttr(43, default=0),
        "draft_end_distance": DXFAttr(44, default=0),
        "twist_angle": DXFAttr(45, default=0),  # in radians
        "scale_factor": DXFAttr(48, default=1),
        "align_angle": DXFAttr(49, default=0),  # in radians
        # don't know the meaning of this matrices
        # 16x group code 46: Transform matrix of sweep entity (16 floats;
        # row major format; default = identity matrix)
        # 16x group code 47: Transform matrix of path entity (16 floats;
        # row major format; default = identity matrix)
        "solid": DXFAttr(290, default=0),  # in radians
        # 0=No alignment; 1= align sweep entity to path:
        "sweep_alignment": DXFAttr(70, default=0),
        "unknown1": DXFAttr(71, default=0),
        # 2=Translate sweep entity to path; 3=Translate path to sweep entity:
        "align_start": DXFAttr(292, default=0),  # bool
        "bank": DXFAttr(293, default=0),  # bool
        "base_point_set": DXFAttr(294, default=0),  # bool
        "sweep_entity_transform_computed": DXFAttr(295, default=0),  # bool
        "path_entity_transform_computed": DXFAttr(296, default=0),  # bool
        "reference_vector_for_controlling_twist": DXFAttr(11, xtype=XType.point3d),
    },
)
acdb_swept_surface_group_codes = group_code_mapping(acdb_swept_surface)


@register_entity
class SweptSurface(Surface):
    """DXF SWEPTSURFACE entity - container entity for embedded ACIS data."""

    DXFTYPE = "SWEPTSURFACE"
    DXFATTRIBS = DXFAttributes(
        base_class,
        acdb_entity,
        acdb_modeler_geometry,
        acdb_surface,
        acdb_swept_surface,
    )

    def __init__(self):
        super().__init__()
        self.transformation_matrix_sweep_entity = Matrix44()
        self.transformation_matrix_path_entity = Matrix44()
        self.sweep_entity_transformation_matrix = Matrix44()
        self.path_entity_transformation_matrix = Matrix44()

    @override
    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, SweptSurface)
        super().copy_data(entity, copy_strategy)
        entity.transformation_matrix_sweep_entity = (
            self.transformation_matrix_sweep_entity.copy()
        )
        entity.transformation_matrix_path_entity = (
            self.transformation_matrix_path_entity.copy()
        )
        entity.sweep_entity_transformation_matrix = (
            self.sweep_entity_transformation_matrix.copy()
        )
        entity.path_entity_transformation_matrix = (
            self.path_entity_transformation_matrix.copy()
        )

    @override
    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(
                dxf, acdb_swept_surface_group_codes, 4, log=False
            )
            self.load_matrices(processor.subclasses[4])
        return dxf

    def load_matrices(self, tags: Tags):
        self.transformation_matrix_sweep_entity = load_matrix(tags, code=40)
        self.transformation_matrix_path_entity = load_matrix(tags, code=41)
        self.sweep_entity_transformation_matrix = load_matrix(tags, code=46)
        self.path_entity_transformation_matrix = load_matrix(tags, code=47)

    @override
    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        # base class export is done by parent class
        super().export_entity(tagwriter)
        # AcDbEntity export is done by parent class
        # AcDbModelerGeometry export is done by parent class
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_swept_surface.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "swept_entity_id",
                "path_entity_id",
            ],
        )
        export_matrix(
            tagwriter, code=40, matrix=self.transformation_matrix_sweep_entity
        )
        export_matrix(tagwriter, code=41, matrix=self.transformation_matrix_path_entity)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "draft_angle",
                "draft_start_distance",
                "draft_end_distance",
                "twist_angle",
                "scale_factor",
                "align_angle",
            ],
        )

        export_matrix(
            tagwriter, code=46, matrix=self.sweep_entity_transformation_matrix
        )
        export_matrix(tagwriter, code=47, matrix=self.path_entity_transformation_matrix)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "solid",
                "sweep_alignment",
                "unknown1",
                "align_start",
                "bank",
                "base_point_set",
                "sweep_entity_transform_computed",
                "path_entity_transform_computed",
                "reference_vector_for_controlling_twist",
            ],
        )
