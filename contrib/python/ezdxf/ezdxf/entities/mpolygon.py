#  Copyright (c) 2021-2022, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
import logging
from ezdxf.lldxf import validator, const
from ezdxf.lldxf.attributes import (
    DXFAttributes,
    DefSubclass,
    DXFAttr,
    RETURN_DEFAULT,
    XType,
    group_code_mapping,
)
from ezdxf.math import NULLVEC, Z_AXIS
from .dxfentity import base_class
from .dxfgfx import acdb_entity
from .factory import register_entity
from .polygon import DXFPolygon
from .gradient import Gradient, GradientType

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.colors import RGB

__all__ = ["MPolygon"]
logger = logging.getLogger("ezdxf")

acdb_mpolygon = DefSubclass(
    "AcDbMPolygon",
    {
        # MPolygon: version
        "version": DXFAttr(
            70,
            default=1,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # x- and y-axis always equal 0, z-axis represents the elevation:
        "elevation": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        "extrusion": DXFAttr(
            210,
            xtype=XType.point3d,
            default=Z_AXIS,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        # Hatch pattern name:
        "pattern_name": DXFAttr(2, default=""),
        # Solid fill color as ACI
        "fill_color": DXFAttr(
            63,
            default=const.BYLAYER,
            optional=True,
            validator=validator.is_valid_aci_color,
            fixer=RETURN_DEFAULT,
        ),
        # MPolygon: Solid-fill flag:
        # 0 = lacks solid fill
        # 1 = has solid fill
        "solid_fill": DXFAttr(
            71,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # Hatch style tag is not supported for MPOLYGON!?
        # I don't know in which order this tag has to be exported.
        # TrueView does not accept this tag in any place!
        # BricsCAD supports this tag!
        "hatch_style": DXFAttr(
            75,
            default=const.HATCH_STYLE_NESTED,
            validator=validator.is_in_integer_range(0, 3),
            fixer=RETURN_DEFAULT,
            optional=True,
        ),
        # Hatch pattern type ... see HATCH
        "pattern_type": DXFAttr(
            76,
            default=const.HATCH_TYPE_PREDEFINED,
            validator=validator.is_in_integer_range(0, 3),
            fixer=RETURN_DEFAULT,
        ),
        # Hatch pattern angle (pattern fill only) in degrees:
        "pattern_angle": DXFAttr(52, default=0),
        # Hatch pattern scale or spacing (pattern fill only):
        "pattern_scale": DXFAttr(
            41,
            default=1,
            validator=validator.is_not_zero,
            fixer=RETURN_DEFAULT,
        ),
        # MPolygon: Boundary annotation flag:
        # 0 = boundary is not an annotated boundary
        # 1 = boundary is an annotated boundary
        "annotated_boundary": DXFAttr(
            73,
            default=0,
            optional=True,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # Hatch pattern double flag (pattern fill only) .. see HATCH
        "pattern_double": DXFAttr(
            77,
            default=0,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # see ... HATCH
        "pixel_size": DXFAttr(47, optional=True),
        "n_seed_points": DXFAttr(
            98,
            default=0,
            validator=validator.is_greater_or_equal_zero,
            fixer=RETURN_DEFAULT,
        ),
        # MPolygon: offset vector in OCS ???
        "offset_vector": DXFAttr(11, xtype=XType.point2d, default=(0, 0)),
        # MPolygon: number of degenerate boundary paths (loops), where a
        # degenerate boundary path is a border that is ignored by the hatch:
        "degenerated_loops": DXFAttr(99, default=0),
    },
)
acdb_mpolygon_group_code = group_code_mapping(acdb_mpolygon)


@register_entity
class MPolygon(DXFPolygon):
    """DXF MPOLYGON entity

    The MPOLYGON is not a core DXF entity, and requires a CLASS definition.

    """

    DXFTYPE = "MPOLYGON"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_mpolygon)
    MIN_DXF_VERSION_FOR_EXPORT = const.DXF2000
    LOAD_GROUP_CODES = acdb_mpolygon_group_code

    def preprocess_export(self, tagwriter: AbstractTagWriter) -> bool:
        if self.paths.has_edge_paths:
            logger.warning(
                "MPOLYGON including edge paths are not exported by ezdxf!"
            )
            return False
        return True

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_mpolygon.name)
        dxf = self.dxf
        dxf.export_dxf_attribs(
            tagwriter,
            [  # tag order is important!
                "version",
                "elevation",
                "extrusion",
                "pattern_name",
                "solid_fill",
            ],
        )
        self.paths.export_dxf(tagwriter, self.dxftype())
        # hatch_style not supported?
        dxf.export_dxf_attribs(
            tagwriter,
            [
                # "hatch_style",  # not supported by MPolygon ???
                "pattern_type",
            ],
        )
        if dxf.solid_fill == 0:  # export pattern
            dxf.export_dxf_attribs(
                tagwriter, ["pattern_angle", "pattern_scale", "pattern_double"]
            )

        dxf.export_dxf_attribs(
            tagwriter,
            [
                "annotated_boundary",
                "pixel_size",
            ],
        )
        if dxf.solid_fill == 0:  # export pattern
            if self.pattern:
                self.pattern.export_dxf(tagwriter, force=True)
            else:  # required pattern length tag!
                tagwriter.write_tag2(78, 0)
        if tagwriter.dxfversion > const.DXF2000:
            dxf.export_dxf_attribs(tagwriter, "fill_color")
        dxf.export_dxf_attribs(tagwriter, "offset_vector")
        self.export_degenerated_loops(tagwriter)
        self.export_gradient(tagwriter)

    def export_degenerated_loops(self, tagwriter: AbstractTagWriter):
        self.dxf.export_dxf_attribs(tagwriter, "degenerated_loops")

    def export_gradient(self, tagwriter: AbstractTagWriter):
        if tagwriter.dxfversion < const.DXF2004:
            return
        if self.gradient is None:
            self.gradient = Gradient(kind=0, num=0, type=0)
        self.gradient.export_dxf(tagwriter)

    def set_solid_fill(
        self, color: int = 7, style: int = 1, rgb: Optional[RGB] = None
    ):
        """Set :class:`MPolygon` to solid fill mode and removes all gradient and
        pattern fill related data.

        Args:
            color: :ref:`ACI`, (0 = BYBLOCK; 256 = BYLAYER)
            style: hatch style is not supported by MPOLYGON, just for symmetry
                to HATCH
            rgb: true color value as (r, g, b)-tuple - has higher priority
                than `color`. True color support requires DXF R2004+

        """
        # remove existing gradient and pattern fill
        self.gradient = None
        self.pattern = None
        self.dxf.solid_fill = 1

        # if a gradient is present, the solid fill_color is ignored
        self.dxf.fill_color = color
        self.dxf.pattern_name = "SOLID"
        self.dxf.pattern_type = const.HATCH_TYPE_PREDEFINED
        if rgb is not None:
            self.set_solid_rgb_gradient(rgb)

    def set_solid_rgb_gradient(self, rgb: RGB):
        """Set solid fill color as gradient of a single RGB color.
        This disables pattern fill!

        (internal API)
        """
        self.gradient = Gradient(kind=1, num=2, type=GradientType.LINEAR)
        self.gradient.color1 = rgb
        self.gradient.color2 = rgb
