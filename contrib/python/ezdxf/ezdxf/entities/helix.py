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
from ezdxf.lldxf.const import SUBCLASS_MARKER
from ezdxf.math import NULLVEC, X_AXIS, Z_AXIS
from .spline import Spline, acdb_spline
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import acdb_entity
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.math import Matrix44

__all__ = ["Helix"]

acdb_helix = DefSubclass(
    "AcDbHelix",
    {
        "major_release_number": DXFAttr(90, default=29),
        "maintenance_release_number": DXFAttr(91, default=63),
        "axis_base_point": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        "start_point": DXFAttr(
            11,
            xtype=XType.point3d,
            default=X_AXIS,
            fixer=RETURN_DEFAULT,
        ),
        "axis_vector": DXFAttr(
            12,
            xtype=XType.point3d,
            default=Z_AXIS,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        # base radius = (start-point - axis_base_point).magnitude
        "radius": DXFAttr(40, default=1),  # top radius
        "turns": DXFAttr(41, default=1),
        "turn_height": DXFAttr(42, default=1),
        # Handedness:
        # 0 = left
        # 1 = right
        "handedness": DXFAttr(
            290,
            default=1,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # Constrain type:
        # 0 = Constrain turn height
        # 1 = Constrain turns
        # 2 = Constrain height
        "constrain": DXFAttr(
            280,
            default=1,
            validator=validator.is_in_integer_range(0, 3),
            fixer=RETURN_DEFAULT,
        ),
    },
)
acdb_helix_group_codes = group_code_mapping(acdb_helix)


@register_entity
class Helix(Spline):
    """DXF HELIX entity"""

    DXFTYPE = "HELIX"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_spline, acdb_helix)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(
                dxf, acdb_helix_group_codes, 3, recover=True
            )
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_helix.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "major_release_number",
                "maintenance_release_number",
                "axis_base_point",
                "start_point",
                "axis_vector",
                "radius",
                "turns",
                "turn_height",
                "handedness",
                "constrain",
            ],
        )

    def transform(self, m: Matrix44) -> Helix:
        """Transform the HELIX entity by transformation matrix `m` inplace."""
        super().transform(m)
        self.dxf.axis_base_point = m.transform(self.dxf.axis_base_point)
        self.dxf.axis_vector = m.transform_direction(self.dxf.axis_vector)
        self.dxf.start_point = m.transform(self.dxf.start_point)
        self.dxf.radius = m.transform_direction(
            (self.dxf.radius, 0, 0)
        ).magnitude
        self.post_transform(m)
        return self
