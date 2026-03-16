# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from typing_extensions import Self
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
from ezdxf.math import NULLVEC, Z_AXIS, X_AXIS
from ezdxf.math.transformtools import transform_extrusion
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import DXFGraphic, acdb_entity
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.math import Matrix44
    from ezdxf import xref


__all__ = ["Tolerance"]

acdb_tolerance = DefSubclass(
    "AcDbFcf",
    {
        "dimstyle": DXFAttr(
            3,
            default="Standard",
            validator=validator.is_valid_table_name,
        ),
        # Insertion point (in WCS):
        "insert": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        # String representing the visual representation of the tolerance:
        "content": DXFAttr(1, default=""),
        "extrusion": DXFAttr(
            210,
            xtype=XType.point3d,
            default=Z_AXIS,
            optional=True,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        # X-axis direction vector (in WCS):
        "x_axis_vector": DXFAttr(
            11,
            xtype=XType.point3d,
            default=X_AXIS,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
    },
)
acdb_tolerance_group_codes = group_code_mapping(acdb_tolerance)


@register_entity
class Tolerance(DXFGraphic):
    """DXF TOLERANCE entity"""

    DXFTYPE = "TOLERANCE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_tolerance)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(
                dxf, acdb_tolerance_group_codes, subclass=2, recover=True
            )
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_tolerance.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            ["dimstyle", "insert", "content", "extrusion", "x_axis_vector"],
        )

    def register_resources(self, registry: xref.Registry) -> None:
        super().register_resources(registry)
        registry.add_dim_style(self.dxf.dimstyle)

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        super().map_resources(clone, mapping)
        clone.dxf.dimstyle = mapping.get_dim_style(self.dxf.dimstyle)

    def transform(self, m: Matrix44) -> Tolerance:
        """Transform the TOLERANCE entity by transformation matrix `m` inplace."""
        self.dxf.insert = m.transform(self.dxf.insert)
        self.dxf.x_axis_vector = m.transform_direction(self.dxf.x_axis_vector)
        self.dxf.extrusion, _ = transform_extrusion(self.dxf.extrusion, m)
        self.post_transform(m)
        return self
