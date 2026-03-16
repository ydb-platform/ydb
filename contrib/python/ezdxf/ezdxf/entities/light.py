# Copyright (c) 2019-2022, Manfred Moitzi
# License: MIT-License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from ezdxf.lldxf import validator
from ezdxf.lldxf.const import SUBCLASS_MARKER, DXF2007
from ezdxf.lldxf.attributes import (
    DXFAttributes,
    DefSubclass,
    DXFAttr,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
)
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import acdb_entity, DXFGraphic
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.math import Matrix44

__all__ = ["Light"]

acdb_light = DefSubclass(
    "AcDbLight",
    {
        "version": DXFAttr(90, default=0),
        # Light name
        "name": DXFAttr(1, default=""),
        # Light type:
        # 1 = distant
        # 2 = point
        # 3 = spot
        "type": DXFAttr(
            70,
            default=1,
            validator=validator.is_in_integer_range(1, 4),
            fixer=RETURN_DEFAULT,
        ),
        "status": DXFAttr(
            290,
            default=1,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        "plot_glyph": DXFAttr(
            291,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        "intensity": DXFAttr(40, default=1),
        # Light position
        "location": DXFAttr(10, xtype=XType.point3d),
        # Target location
        "target": DXFAttr(11, xtype=XType.point3d),
        # Attenuation type:
        # 0 = None
        # 1 = Inverse Linear
        # 2 = Inverse Square
        "attenuation_type": DXFAttr(
            72,
            default=2,
            validator=validator.is_in_integer_range(0, 3),
            fixer=RETURN_DEFAULT,
        ),
        "use_attenuation_limits": DXFAttr(
            292,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        "attenuation_start_limits": DXFAttr(41),
        "attenuation_end_limits": DXFAttr(42),
        "hotspot_angle": DXFAttr(50),  # in degrees
        "falloff_angle": DXFAttr(51),  # in degrees
        "cast_shadows": DXFAttr(
            293,
            default=1,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # Shadow Type:
        # 0 = Ray traced shadows
        # 1 = Shadow maps
        "shadow_type": DXFAttr(
            73,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        "shadow_map_size": DXFAttr(91),
        "shadow_map_softness": DXFAttr(280),
    },
)
acdb_light_group_codes = group_code_mapping(acdb_light)


@register_entity
class Light(DXFGraphic):
    """DXF LIGHT entity"""

    DXFTYPE = "LIGHT"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_light)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2007

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(
                dxf, acdb_light_group_codes, 2, recover=True
            )
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_light.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "version",
                "name",
                "type",
                "status",
                "plot_glyph",
                "intensity",
                "location",
                "target",
                "attenuation_type",
                "use_attenuation_limits",
                "attenuation_start_limits",
                "attenuation_end_limits",
                "hotspot_angle",
                "falloff_angle",
                "cast_shadows",
                "shadow_type",
                "shadow_map_size",
                "shadow_map_softness",
            ],
        )

    def transform(self, m: Matrix44) -> Light:
        """Transform the LIGHT entity by transformation matrix `m` inplace."""
        self.dxf.location = m.transform(self.dxf.location)
        self.dxf.target = m.transform(self.dxf.target)
        self.post_transform(m)
        return self
