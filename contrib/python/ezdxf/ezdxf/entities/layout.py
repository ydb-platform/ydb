#  Copyright (c) 2020-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from typing_extensions import Self
from ezdxf.lldxf import validator
from ezdxf.lldxf.const import SUBCLASS_MARKER
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.math import Vec3, Vec2, NULLVEC, X_AXIS, Y_AXIS
from .dxfentity import base_class, SubclassProcessor, DXFEntity
from .dxfobj import DXFObject
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.entities.dxfns import DXFNamespace
    from ezdxf import xref

__all__ = ["PlotSettings", "DXFLayout"]

acdb_plot_settings = DefSubclass(
    "AcDbPlotSettings",
    {
        # acdb_plot_settings is also part of LAYOUT and LAYOUT has a 'name' attribute
        "page_setup_name": DXFAttr(1, default=""),

        # An optional empty string selects the default printer/plotter
        "plot_configuration_file": DXFAttr(2, default="", optional=True),
        "paper_size": DXFAttr(4, default="A3"),
        "plot_view_name": DXFAttr(6, default=""),
        "left_margin": DXFAttr(40, default=7.5),  # in mm
        "bottom_margin": DXFAttr(41, default=20),  # in mm
        "right_margin": DXFAttr(42, default=7.5),  # in mm
        "top_margin": DXFAttr(43, default=20),  # in mm
        "paper_width": DXFAttr(44, default=420),  # in mm
        "paper_height": DXFAttr(45, default=297),  # in mm
        "plot_origin_x_offset": DXFAttr(46, default=0.0),  # in mm
        "plot_origin_y_offset": DXFAttr(47, default=0.0),  # in mm
        "plot_window_x1": DXFAttr(48, default=0.0),
        "plot_window_y1": DXFAttr(49, default=0.0),
        "plot_window_x2": DXFAttr(140, default=0.0),
        "plot_window_y2": DXFAttr(141, default=0.0),
        # Numerator of custom print scale: real world (paper) units:
        "scale_numerator": DXFAttr(142, default=1.0),
        # Denominator of custom print scale: drawing units:
        "scale_denominator": DXFAttr(143, default=1.0),
        # Plot layout flags:
        # 1 = plot viewport borders
        # 2 = show plot-styles
        # 4 = plot centered
        # 8 = plot hidden == hide paperspace entities?
        # 16 = use standard scale
        # 32 = plot with plot-styles
        # 64 = scale lineweights
        # 128 = plot entity lineweights
        # 512 = draw viewports first
        # 1024 = model type
        # 2048 = update paper
        # 4096 = zoom to paper on update
        # 8192 = initializing
        # 16384 = prev plot-init
        # the "Plot transparencies" option is stored in the XDATA section
        "plot_layout_flags": DXFAttr(70, default=688),
        # Plot paper units:
        # 0 = Plot in inches
        # 1 = Plot in millimeters
        # 2 = Plot in pixels
        "plot_paper_units": DXFAttr(
            72,
            default=1,
            validator=validator.is_in_integer_range(0, 3),
            fixer=RETURN_DEFAULT,
        ),
        # Plot rotation:
        # 0 = No rotation
        # 1 = 90 degrees counterclockwise
        # 2 = Upside-down
        # 3 = 90 degrees clockwise
        "plot_rotation": DXFAttr(
            73,
            default=0,
            validator=validator.is_in_integer_range(0, 4),
            fixer=RETURN_DEFAULT,
        ),
        # Plot type:
        # 0 = Last screen display
        # 1 = Drawing extents
        # 2 = Drawing limits
        # 3 = View specified by code 6
        # 4 = Window specified by codes 48, 49, 140, and 141
        # 5 = Layout information
        "plot_type": DXFAttr(
            74,
            default=5,
            validator=validator.is_in_integer_range(0, 6),
            fixer=RETURN_DEFAULT,
        ),
        # Associated CTB-file
        "current_style_sheet": DXFAttr(7, default=""),
        # Standard scale type:
        # 0 = Scaled to Fit
        # 1 = 1/128"=1'
        # 2 = 1/64"=1'
        # 3 = 1/32"=1'
        # 4 = 1/16"=1'
        # 5 = 3/32"=1'
        # 6 = 1/8"=1'
        # 7 = 3/16"=1'
        # 8 = 1/4"=1'
        # 9 = 3/8"=1'
        # 10 = 1/2"=1'
        # 11 = 3/4"=1'
        # 12 = 1"=1'
        # 13 = 3"=1'
        # 14 = 6"=1'
        # 15 = 1'=1'
        # 16 = 1:1
        # 17 = 1:2
        # 18 = 1:4
        # 19 = 1:8
        # 20 = 1:10
        # 21 = 1:16
        # 22 = 1:20
        # 23 = 1:30
        # 24 = 1:40
        # 25 = 1:50
        # 26 = 1:100
        # 27 = 2:1
        # 28 = 4:1
        # 29 = 8:1
        # 30 = 10:1
        # 31 = 100:1
        # 32 = 1000:1
        "standard_scale_type": DXFAttr(
            75,
            default=16,
            validator=validator.is_in_integer_range(0, 33),
            fixer=RETURN_DEFAULT,
        ),
        # Shade plot mode:
        # 0 = As Displayed
        # 1 = Wireframe
        # 2 = Hidden
        # 3 = Rendered
        "shade_plot_mode": DXFAttr(
            76,
            default=0,
            validator=validator.is_in_integer_range(0, 4),
            fixer=RETURN_DEFAULT,
        ),
        # Shade plot resolution level:
        # 0 = Draft
        # 1 = Preview
        # 2 = Normal
        # 3 = Presentation
        # 4 = Maximum
        # 5 = Custom
        "shade_plot_resolution_level": DXFAttr(
            77,
            default=2,
            validator=validator.is_in_integer_range(0, 6),
            fixer=RETURN_DEFAULT,
        ),
        # Valid range: 100 to 32767, Only applied when the shade_plot_resolution
        # level is set to 5 (Custom)
        "shade_plot_custom_dpi": DXFAttr(
            78,
            default=300,
            validator=validator.is_in_integer_range(100, 32768),
            fixer=validator.fit_into_integer_range(100, 32768),
        ),
        # Factor for unit conversion (mm -> inches)
        # 147: DXF Reference error: 'A floating point scale factor that represents
        # the standard scale value specified in code 75'
        "unit_factor": DXFAttr(
            147,
            default=1.0,
            validator=validator.is_greater_zero,
            fixer=RETURN_DEFAULT,
        ),
        "paper_image_origin_x": DXFAttr(148, default=0),
        "paper_image_origin_y": DXFAttr(149, default=0),
        "shade_plot_handle": DXFAttr(333, optional=True),
    },
)
acdb_plot_settings_group_codes = group_code_mapping(acdb_plot_settings)

# The "Plot transparencies" option is stored in the XDATA section of the
# LAYOUT entity:
# 1001
# PLOTTRANSPARENCY
# 1071
# 1


@register_entity
class PlotSettings(DXFObject):
    DXFTYPE = "PLOTSETTINGS"
    DXFATTRIBS = DXFAttributes(base_class, acdb_plot_settings)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_plot_settings_group_codes, 1)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_plot_settings.name)

        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "page_setup_name",
                "plot_configuration_file",
                "paper_size",
                "plot_view_name",
                "left_margin",
                "bottom_margin",
                "right_margin",
                "top_margin",
                "paper_width",
                "paper_height",
                "plot_origin_x_offset",
                "plot_origin_y_offset",
                "plot_window_x1",
                "plot_window_y1",
                "plot_window_x2",
                "plot_window_y2",
                "scale_numerator",
                "scale_denominator",
                "plot_layout_flags",
                "plot_paper_units",
                "plot_rotation",
                "plot_type",
                "current_style_sheet",
                "standard_scale_type",
                "shade_plot_mode",
                "shade_plot_resolution_level",
                "shade_plot_custom_dpi",
                "unit_factor",
                "paper_image_origin_x",
                "paper_image_origin_y",
            ],
        )

    def register_resources(self, registry: xref.Registry) -> None:
        super().register_resources(registry)
        registry.add_handle(self.dxf.get("shade_plot_handle"))

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        super().map_resources(clone, mapping)
        shade_plot_handle = self.dxf.get("shade_plot_handle")
        if shade_plot_handle and shade_plot_handle != "0":
            clone.dxf.shade_plot_handle = mapping.get_handle(shade_plot_handle)
        else:
            clone.dxf.discard("shade_plot_handle")


acdb_layout = DefSubclass(
    "AcDbLayout",
    {
        # Layout name:
        "name": DXFAttr(1, default="Layoutname"),
        # Flag (bit-coded) to control the following:
        # 1 = Indicates the PSLTSCALE value for this layout when this layout is current
        # 2 = Indicates the LIMCHECK value for this layout when this layout is current
        "layout_flags": DXFAttr(70, default=1),
        # Tab order: This number is an ordinal indicating this layout's ordering in
        # the tab control that is attached to the AutoCAD drawing frame window.
        # Note that the "Model" tab always appears as the first tab regardless of
        # its tab order.
        "taborder": DXFAttr(71, default=1),
        # Minimum limits:
        "limmin": DXFAttr(10, xtype=XType.point2d, default=Vec2(0, 0)),
        # Maximum limits:
        "limmax": DXFAttr(11, xtype=XType.point2d, default=Vec2(420, 297)),
        # Insertion base point for this layout:
        "insert_base": DXFAttr(12, xtype=XType.point3d, default=NULLVEC),
        # Minimum extents for this layout:
        "extmin": DXFAttr(14, xtype=XType.point3d, default=Vec3(1e20, 1e20, 1e20)),
        # Maximum extents for this layout:
        "extmax": DXFAttr(15, xtype=XType.point3d, default=Vec3(-1e20, -1e20, -1e20)),
        "elevation": DXFAttr(146, default=0.0),
        "ucs_origin": DXFAttr(13, xtype=XType.point3d, default=NULLVEC),
        "ucs_xaxis": DXFAttr(
            16,
            xtype=XType.point3d,
            default=X_AXIS,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        "ucs_yaxis": DXFAttr(
            17,
            xtype=XType.point3d,
            default=Y_AXIS,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        # Orthographic type of UCS:
        # 0 = UCS is not orthographic
        # 1 = Top
        # 2 = Bottom
        # 3 = Front
        # 4 = Back
        # 5 = Left
        # 6 = Right
        "ucs_type": DXFAttr(
            76,
            default=1,
            validator=validator.is_in_integer_range(0, 7),
            fixer=RETURN_DEFAULT,
        ),
        # Handle of parent BLOCK_RECORD
        "block_record_handle": DXFAttr(330),
        # Handle to the viewport that was last active in this
        # layout when the layout was current:
        "viewport_handle": DXFAttr(331),
        # Handle of AcDbUCSTableRecord if UCS is a named
        # UCS. If not present, then UCS is unnamed
        "ucs_handle": DXFAttr(345),
        # Handle of AcDbUCSTableRecord of base UCS if UCS is
        # orthographic (76 code is non-zero). If not present and
        # 76 code is non-zero, then base UCS is taken to be WORLD
        "base_ucs_handle": DXFAttr(346),
    },
)
acdb_layout_group_codes = group_code_mapping(acdb_layout)


@register_entity
class DXFLayout(PlotSettings):
    DXFTYPE = "LAYOUT"
    DXFATTRIBS = DXFAttributes(base_class, acdb_plot_settings, acdb_layout)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_layout_group_codes, 2)
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        # Set correct model type flag
        self.set_flag_state(1024, self.dxf.name.upper() == "MODEL", "plot_layout_flags")
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_layout.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "name",
                "layout_flags",
                "taborder",
                "limmin",
                "limmax",
                "insert_base",
                "extmin",
                "extmax",
                "elevation",
                "ucs_origin",
                "ucs_xaxis",
                "ucs_yaxis",
                "ucs_type",
                "block_record_handle",
                "viewport_handle",
                "ucs_handle",
                "base_ucs_handle",
            ],
        )

    def register_resources(self, registry: xref.Registry) -> None:
        super().register_resources(registry)
        registry.add_handle(self.dxf.get("ucs_handle"))
        registry.add_handle(self.dxf.get("base_ucs_handle"))

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        super().map_resources(clone, mapping)

        # The content of paperspace layouts is not copied automatically and the
        # associated BLOCK_RECORD is created and assigned in a special method.
        mapping.map_existing_handle(self, clone, "ucs_handle", optional=True)
        mapping.map_existing_handle(self, clone, "base_ucs_handle", optional=True)
        mapping.map_existing_handle(self, clone, "viewport_handle", optional=True)
