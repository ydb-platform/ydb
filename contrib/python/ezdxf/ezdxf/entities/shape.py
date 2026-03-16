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
from ezdxf.lldxf.const import DXF12, SUBCLASS_MARKER
from ezdxf.math import NULLVEC, Z_AXIS
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
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.math import Matrix44


__all__ = ["Shape"]

# Description of the "name" attribute from the DWG documentation: 20.4.37 SHAPE (33)
# In DXF the shape name is stored. When reading from DXF, the shape is found by
# iterating over all the text styles and when the text style contains a shape file,
# iterating over all the shapes until the one with the matching name is found.

acdb_shape = DefSubclass(
    "AcDbShape",
    {
        # Elevation is a legacy feature from R11 and prior, do not use this
        # attribute, store the entity elevation in the z-axis of the vertices.
        # ezdxf does not export the elevation attribute!
        "elevation": DXFAttr(38, default=0, optional=True),
        # Thickness could be negative:
        "thickness": DXFAttr(39, default=0, optional=True),
        # Insertion point (in WCS)
        "insert": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        # Shape size:
        "size": DXFAttr(40, default=1),
        # Shape name:
        "name": DXFAttr(2, default=""),
        # Rotation angle in degrees:
        "rotation": DXFAttr(50, default=0, optional=True),
        # Relative X scale factor
        "xscale": DXFAttr(
            41,
            default=1,
            optional=True,
            validator=validator.is_not_zero,
            fixer=RETURN_DEFAULT,
        ),
        # Oblique angle in degrees:
        "oblique": DXFAttr(51, default=0, optional=True),
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
acdb_shape_group_codes = group_code_mapping(acdb_shape)
merged_shape_group_codes = merge_group_code_mappings(
    acdb_entity_group_codes, acdb_shape_group_codes  # type: ignore
)


@register_entity
class Shape(DXFGraphic):
    """DXF SHAPE entity"""

    DXFTYPE = "SHAPE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_shape)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Loading interface. (internal API)"""
        # bypass DXFGraphic, loading proxy graphic is skipped!
        dxf = super(DXFGraphic, self).load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, merged_shape_group_codes)
            if processor.r12:
                # Transform elevation attribute from R11 to z-axis values:
                elevation_to_z_axis(dxf, ("center",))
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_shape.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "insert",
                "size",
                "name",
                "thickness",
                "rotation",
                "xscale",
                "oblique",
                "extrusion",
            ],
        )

    def transform(self, m: Matrix44) -> Shape:
        """Transform the SHAPE entity by transformation matrix `m` inplace."""
        dxf = self.dxf
        dxf.insert = m.transform(dxf.insert)  # DXF Reference: WCS?
        ocs = OCSTransform(self.dxf.extrusion, m)

        dxf.rotation = ocs.transform_deg_angle(dxf.rotation)
        dxf.size = ocs.transform_length((0, dxf.size, 0))
        dxf.x_scale = ocs.transform_length(
            (dxf.x_scale, 0, 0), reflection=dxf.x_scale
        )
        if dxf.hasattr("thickness"):
            dxf.thickness = ocs.transform_thickness(dxf.thickness)

        dxf.extrusion = ocs.new_extrusion
        self.post_transform(m)
        return self
