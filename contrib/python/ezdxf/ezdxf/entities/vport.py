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
from ezdxf.lldxf.const import DXF12, SUBCLASS_MARKER, DXF2000, DXF2007
from ezdxf.lldxf.validator import is_valid_vport_name
from ezdxf.math import Vec2, NULLVEC, Z_AXIS
from ezdxf.entities.dxfentity import base_class, SubclassProcessor, DXFEntity
from ezdxf.entities.layer import acdb_symbol_table_record
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["VPort"]
logger = logging.getLogger("ezdxf")

acdb_vport = DefSubclass(
    "AcDbViewportTableRecord",
    {
        "name": DXFAttr(2, validator=is_valid_vport_name),
        "flags": DXFAttr(70, default=0),
        "lower_left": DXFAttr(10, xtype=XType.point2d, default=Vec2(0, 0)),
        "upper_right": DXFAttr(11, xtype=XType.point2d, default=Vec2(1, 1)),
        "center": DXFAttr(12, xtype=XType.point2d, default=Vec2(0, 0)),
        "snap_base": DXFAttr(13, xtype=XType.point2d, default=Vec2(0, 0)),
        "snap_spacing": DXFAttr(
            14, xtype=XType.point2d, default=Vec2(0.5, 0.5)
        ),
        "grid_spacing": DXFAttr(
            15, xtype=XType.point2d, default=Vec2(0.5, 0.5)
        ),
        "direction": DXFAttr(
            16,
            xtype=XType.point3d,
            default=Z_AXIS,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        "target": DXFAttr(17, xtype=XType.point3d, default=NULLVEC),
        # height: DXF reference error: listed as group code 45
        "height": DXFAttr(40, default=1000),
        "aspect_ratio": DXFAttr(41, default=1.34),
        "focal_length": DXFAttr(42, default=50),
        "front_clipping": DXFAttr(43, default=0),
        "back_clipping": DXFAttr(44, default=0),
        "snap_rotation": DXFAttr(50, default=0),
        "view_twist": DXFAttr(51, default=0),
        "view_mode": DXFAttr(71, default=0),
        "circle_sides": DXFAttr(72, default=1000),
        "fast_zoom": DXFAttr(73, default=1),  # removed in R2007
        # ucs_icon:
        # bit 0: 0=hide, 1=show
        # bit 1: 0=display in lower left corner, 1=display at origin
        "ucs_icon": DXFAttr(74, default=3),  # show at origin
        "snap_on": DXFAttr(75, default=0),  # removed in R2007
        "grid_on": DXFAttr(76, default=0),  # removed in R2007
        "snap_style": DXFAttr(77, default=0),  # removed in R2007
        "snap_isopair": DXFAttr(78, default=0),  # removed in R2007
        # R2000: 331 or 441 (optional) - ignored by ezdxf
        # Soft or hard-pointer ID/handle to frozen layer objects;
        # repeats for each frozen layers
        # 70: Bit flags and perspective mode
        # CTB-File?
        "plot_style_sheet": DXFAttr(1, dxfversion=DXF2007),
        # Render mode:
        # 0 = 2D Optimized (classic 2D)
        # 1 = Wireframe
        # 2 = Hidden line
        # 3 = Flat shaded
        # 4 = Gouraud shaded
        # 5 = Flat shaded with wireframe
        # 6 = Gouraud shaded with wireframe
        # All rendering modes other than 2D Optimized engage the new 3D graphics
        # pipeline. These values directly correspond to the SHADEMODE command and
        # the AcDbAbstractViewTableRecord::RenderMode enum
        "render_mode": DXFAttr(
            281,
            default=0,
            dxfversion=DXF2000,
            validator=validator.is_in_integer_range(0, 7),
            fixer=RETURN_DEFAULT,
        ),
        # Value of UCSVP for this viewport. If set to 1, then viewport stores its
        # own UCS which will become the current UCS whenever the viewport is
        # activated. If set to 0, UCS will not change when this viewport is
        # activated
        "ucs_vp": DXFAttr(
            65,
            dxfversion=DXF2000,
            default=0,
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
        # Handle of AcDbUCSTableRecord if UCS is a named UCS. If not present,
        # then UCS is unnamed:
        "ucs_handle": DXFAttr(345, dxfversion=DXF2000),
        # Handle of AcDbUCSTableRecord of base UCS if UCS is orthographic (79 code
        # is non-zero). If not present and 79 code is non-zero, then base UCS is
        # taken to be WORLD:
        "base_ucs_handle": DXFAttr(346, dxfversion=DXF2000),
        # UCS ortho type:
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
        ),
        "elevation": DXFAttr(146, dxfversion=DXF2000, default=0),
        "unknown1": DXFAttr(60, dxfversion=DXF2000),
        "shade_plot_setting": DXFAttr(170, dxfversion=DXF2007),
        "major_grid_lines": DXFAttr(61, dxfversion=DXF2007),
        # Handle to background object
        "background_handle": DXFAttr(332, dxfversion=DXF2007, optional=True),
        # Handle to shade plot object
        "shade_plot_handle": DXFAttr(333, dxfversion=DXF2007, optional=True),
        # Handle to visual style object
        "visual_style_handle": DXFAttr(348, dxfversion=DXF2007, optional=True),
        "default_lighting_on": DXFAttr(
            292,
            dxfversion=DXF2007,
            validator=validator.is_integer_bool,
        ),
        # Default lighting type:
        # 0 = One distant light
        # 1 = Two distant lights
        "default_lighting_type": DXFAttr(
            282,
            dxfversion=DXF2007,
            validator=validator.is_integer_bool,
        ),
        "brightness": DXFAttr(141, dxfversion=DXF2000),
        "contrast": DXFAttr(142, dxfversion=DXF2000),
        "ambient_color_aci": DXFAttr(63, dxfversion=DXF2000, optional=True),
        "ambient_true_color": DXFAttr(421, dxfversion=DXF2000, optional=True),
        "ambient_color_name": DXFAttr(431, dxfversion=DXF2000, optional=True),
        # Hard-pointer handle to sun object:
        "sun_handle": DXFAttr(361, dxfversion=DXF2007, optional=True),
    },
)
acdb_vport_group_codes = group_code_mapping(acdb_vport)


@register_entity
class VPort(DXFEntity):
    """DXF VIEW entity"""

    DXFTYPE = "VPORT"
    DXFATTRIBS = DXFAttributes(base_class, acdb_symbol_table_record, acdb_vport)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(
                dxf, acdb_vport_group_codes, subclass=2
            )
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        super().export_entity(tagwriter)
        dxfversion = tagwriter.dxfversion
        if dxfversion > DXF12:
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_symbol_table_record.name)
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_vport.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "name",
                "flags",
                "lower_left",
                "upper_right",
                "center",
                "snap_base",
                "snap_spacing",
                "grid_spacing",
                "direction",
                "target",
                "height",
                "aspect_ratio",
                "focal_length",
                "front_clipping",
                "back_clipping",
                "snap_rotation",
                "view_twist",
                "view_mode",
                "circle_sides",
                "fast_zoom",
                "ucs_icon",
                "snap_on",
                "grid_on",
                "snap_style",
                "snap_isopair",
                "plot_style_sheet",
                "render_mode",
                "ucs_vp",
                "ucs_origin",
                "ucs_xaxis",
                "ucs_yaxis",
                "ucs_handle",
                "base_ucs_handle",
                "ucs_ortho_type",
                "elevation",
                "unknown1",
                "shade_plot_setting",
                "major_grid_lines",
                "background_handle",
                "shade_plot_handle",
                "visual_style_handle",
                "default_lighting_on",
                "default_lighting_type",
                "brightness",
                "contrast",
                "ambient_color_aci",
                "ambient_true_color",
                "ambient_color_name",
            ],
        )

    def reset_wcs(self) -> None:
        """Reset coordinate system to the :ref:`WCS`."""
        self.dxf.ucs_vp = 1
        self.dxf.ucs_origin = (0, 0, 0)
        self.dxf.ucs_xaxis = (1, 0, 0)
        self.dxf.ucs_yaxis = (0, 1, 0)
        self.dxf.ucs_ortho_type = 0
        self.dxf.discard("ucs_handle")
        self.dxf.discard("base_ucs_handle")
