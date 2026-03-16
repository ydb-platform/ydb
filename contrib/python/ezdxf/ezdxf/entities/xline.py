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
)
from ezdxf.lldxf.const import SUBCLASS_MARKER, DXF2000
from ezdxf.math import Vec3, Matrix44, NULLVEC, Z_AXIS
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import DXFGraphic, acdb_entity
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["Ray", "XLine"]

acdb_xline = DefSubclass(
    "AcDbXline",
    {
        "start": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        "unit_vector": DXFAttr(
            11,
            xtype=XType.point3d,
            default=Z_AXIS,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
    },
)
acdb_xline_group_codes = group_code_mapping(acdb_xline)


@register_entity
class XLine(DXFGraphic):
    """DXF XLINE entity"""

    DXFTYPE = "XLINE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_xline)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000
    XLINE_SUBCLASS = "AcDbXline"

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(
                dxf, acdb_xline_group_codes, subclass=2, recover=True
            )
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, self.XLINE_SUBCLASS)
        self.dxf.export_dxf_attribs(tagwriter, ["start", "unit_vector"])

    def transform(self, m: Matrix44) -> XLine:
        """Transform the XLINE/RAY entity by transformation matrix `m` inplace."""
        self.dxf.start = m.transform(self.dxf.start)
        self.dxf.unit_vector = m.transform_direction(
            self.dxf.unit_vector
        ).normalize()
        self.post_transform(m)
        return self

    def translate(self, dx: float, dy: float, dz: float) -> XLine:
        """Optimized XLINE/RAY translation about `dx` in x-axis, `dy` in
        y-axis and `dz` in z-axis.

        """
        self.dxf.start = Vec3(dx, dy, dz) + self.dxf.start
        # Avoid Matrix44 instantiation if not required:
        if self.is_post_transform_required:
            self.post_transform(Matrix44.translate(dx, dy, dz))
        return self


@register_entity
class Ray(XLine):
    """DXF Ray entity"""

    DXFTYPE = "RAY"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_xline)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000
    XLINE_SUBCLASS = "AcDbRay"
