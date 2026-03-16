# Copyright (c) 2019-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
import logging

from ezdxf.lldxf import validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.lldxf.const import DXF12, SUBCLASS_MARKER, DXF2000, DXF2007, DXF2010
from ezdxf.math import Vec3, NULLVEC
from ezdxf.entities.dxfentity import base_class, SubclassProcessor, DXFEntity
from ezdxf.entities.layer import acdb_symbol_table_record
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["View"]
logger = logging.getLogger("ezdxf")

acdb_view = DefSubclass(
    "AcDbViewTableRecord",
    {
        "name": DXFAttr(2, validator=validator.is_valid_table_name),
        "flags": DXFAttr(70, default=0),
        "height": DXFAttr(40, default=1),
        "width": DXFAttr(41, default=1),
        "center": DXFAttr(10, xtype=XType.point2d, default=NULLVEC),
        "direction": DXFAttr(
            11,
            xtype=XType.point3d,
            default=Vec3(1, 1, 1),
            validator=validator.is_not_null_vector,
        ),
        "target": DXFAttr(12, xtype=XType.point3d, default=NULLVEC),
        "focal_length": DXFAttr(42, default=50),
        "front_clipping": DXFAttr(43, default=0),
        "back_clipping": DXFAttr(44, default=0),
        "view_twist": DXFAttr(50, default=0),
        "view_mode": DXFAttr(71, default=0),
        # Render mode:
        # 0 = 2D Optimized (classic 2D)
        # 1 = Wireframe
        # 2 = Hidden line
        # 3 = Flat shaded
        # 4 = Gouraud shaded
        # 5 = Flat shaded with wireframe
        # 6 = Gouraud shaded with wireframe
        "render_mode": DXFAttr(
            281,
            default=0,
            dxfversion=DXF2000,
            validator=validator.is_in_integer_range(0, 7),
            fixer=RETURN_DEFAULT,
        ),
        # 1 if there is an UCS associated to this view, 0 otherwise.
        "ucs": DXFAttr(
            72,
            default=0,
            dxfversion=DXF2000,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        "ucs_origin": DXFAttr(110, xtype=XType.point3d, dxfversion=DXF2000),
        "ucs_xaxis": DXFAttr(
            111,
            xtype=XType.point3d,
            dxfversion=DXF2000,
            validator=validator.is_not_null_vector,
        ),
        "ucs_yaxis": DXFAttr(
            112,
            xtype=XType.point3d,
            dxfversion=DXF2000,
            validator=validator.is_not_null_vector,
        ),
        # 0 = UCS is not orthographic
        # 1 = Top
        # 2 = Bottom
        # 3 = Front
        # 4 = Back
        # 5 = Left
        # 6 = Right
        "ucs_ortho_type": DXFAttr(
            79,
            dxfversion=DXF2000,
            validator=validator.is_in_integer_range(0, 7),
            fixer=lambda x: 0,
        ),
        "elevation": DXFAttr(146, dxfversion=DXF2000, default=0),
        # handle of AcDbUCSTableRecord if UCS is a named UCS. If not present,
        # then UCS is unnamed:
        "ucs_handle": DXFAttr(345, dxfversion=DXF2000),
        # handle of AcDbUCSTableRecord of base UCS if UCS is orthographic (79 code
        # is non-zero). If not present and 79 code is non-zero, then base UCS is
        # taken to be WORLD
        "base_ucs_handle": DXFAttr(346, dxfversion=DXF2000),
        # 1 if the camera is plottable
        "camera_plottable": DXFAttr(
            73,
            default=0,
            dxfversion=DXF2007,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        "background_handle": DXFAttr(332, optional=True, dxfversion=DXF2007),
        "live_selection_handle": DXFAttr(
            334, optional=True, dxfversion=DXF2007
        ),
        "visual_style_handle": DXFAttr(348, optional=True, dxfversion=DXF2007),
        "sun_handle": DXFAttr(361, optional=True, dxfversion=DXF2010),
    },
)
acdb_view_group_codes = group_code_mapping(acdb_view)


@register_entity
class View(DXFEntity):
    """DXF VIEW entity"""

    DXFTYPE = "VIEW"
    DXFATTRIBS = DXFAttributes(base_class, acdb_symbol_table_record, acdb_view)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, acdb_view_group_codes)  # type: ignore
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_symbol_table_record.name)
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_view.name)

        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "name",
                "flags",
                "height",
                "width",
                "center",
                "direction",
                "target",
                "focal_length",
                "front_clipping",
                "back_clipping",
                "view_twist",
                "view_mode",
                "render_mode",
                "ucs",
                "ucs_origin",
                "ucs_xaxis",
                "ucs_yaxis",
                "ucs_ortho_type",
                "elevation",
                "ucs_handle",
                "base_ucs_handle",
                "camera_plottable",
                "background_handle",
                "live_selection_handle",
                "visual_style_handle",
                "sun_handle",
            ],
        )
