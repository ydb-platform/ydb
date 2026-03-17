# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from typing_extensions import Self
import math

from ezdxf.lldxf import validator
from ezdxf.lldxf import const
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
    merge_group_code_mappings,
)
from ezdxf.enums import (
    TextEntityAlignment,
    MAP_TEXT_ENUM_TO_ALIGN_FLAGS,
    MAP_TEXT_ALIGN_FLAGS_TO_ENUM,
)
from ezdxf.math import Vec3, UVec, Matrix44, NULLVEC, Z_AXIS
from ezdxf.math.transformtools import OCSTransform
from ezdxf.audit import Auditor
from ezdxf.tools.text import plain_text

from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import (
    DXFGraphic,
    acdb_entity,
    elevation_to_z_axis,
    acdb_entity_group_codes,
)
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFNamespace, DXFEntity
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf import xref

__all__ = ["Text", "acdb_text", "acdb_text_group_codes"]

acdb_text = DefSubclass(
    "AcDbText",
    {
        # First alignment point (in OCS):
        "insert": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        # Text height
        "height": DXFAttr(
            40,
            default=2.5,
            validator=validator.is_greater_zero,
            fixer=RETURN_DEFAULT,
        ),
        # Text content as string:
        "text": DXFAttr(
            1,
            default="",
            validator=validator.is_valid_one_line_text,
            fixer=validator.fix_one_line_text,
        ),
        # Text rotation in degrees (optional)
        "rotation": DXFAttr(50, default=0, optional=True),
        # Oblique angle in degrees, vertical = 0 deg (optional)
        "oblique": DXFAttr(51, default=0, optional=True),
        # Text style name (optional), given text style must have an entry in the
        # text-styles tables.
        "style": DXFAttr(7, default="Standard", optional=True),
        # Relative X scale factor—width (optional)
        # This value is also adjusted when fit-type text is used
        "width": DXFAttr(
            41,
            default=1,
            optional=True,
            validator=validator.is_greater_zero,
            fixer=RETURN_DEFAULT,
        ),
        # Text generation flags (optional)
        # 2 = backward (mirror-x),
        # 4 = upside down (mirror-y)
        "text_generation_flag": DXFAttr(
            71,
            default=0,
            optional=True,
            validator=validator.is_one_of({0, 2, 4, 6}),
            fixer=RETURN_DEFAULT,
        ),
        # Horizontal text justification type (optional) horizontal justification
        # 0 = Left
        # 1 = Center
        # 2 = Right
        # 3 = Aligned (if vertical alignment = 0)
        # 4 = Middle (if vertical alignment = 0)
        # 5 = Fit (if vertical alignment = 0)
        # This value is meaningful only if the value of a 72 or 73 group is nonzero
        # (if the justification is anything other than baseline/left)
        "halign": DXFAttr(
            72,
            default=0,
            optional=True,
            validator=validator.is_in_integer_range(0, 6),
            fixer=RETURN_DEFAULT,
        ),
        # Second alignment point (in OCS) (optional)
        "align_point": DXFAttr(11, xtype=XType.point3d, optional=True),
        # Elevation is a legacy feature from R11 and prior, do not use this
        # attribute, store the entity elevation in the z-axis of the vertices.
        # ezdxf does not export the elevation attribute!
        "elevation": DXFAttr(38, default=0, optional=True),
        # Thickness in extrusion direction, only supported for SHX font in
        # AutoCAD/BricsCAD (optional), can be negative
        "thickness": DXFAttr(39, default=0, optional=True),
        # Extrusion direction (optional)
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
acdb_text_group_codes = group_code_mapping(acdb_text)
acdb_text2 = DefSubclass(
    "AcDbText",
    {
        # Vertical text justification type (optional)
        # 0 = Baseline
        # 1 = Bottom
        # 2 = Middle
        # 3 = Top
        "valign": DXFAttr(
            73,
            default=0,
            optional=True,
            validator=validator.is_in_integer_range(0, 4),
            fixer=RETURN_DEFAULT,
        )
    },
)
acdb_text2_group_codes = group_code_mapping(acdb_text2)
merged_text_group_codes = merge_group_code_mappings(
    acdb_entity_group_codes,  # type: ignore
    acdb_text_group_codes,
    acdb_text2_group_codes,
)


# Formatting codes:
# %%d: '°'
# %%u in TEXT start underline formatting until next %%u or until end of line


@register_entity
class Text(DXFGraphic):
    """DXF TEXT entity"""

    DXFTYPE = "TEXT"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_text, acdb_text2)
    # horizontal align values
    LEFT = 0
    CENTER = 1
    RIGHT = 2
    # vertical align values
    BASELINE = 0
    BOTTOM = 1
    MIDDLE = 2
    TOP = 3
    # text generation flags
    MIRROR_X = 2
    MIRROR_Y = 4
    BACKWARD = MIRROR_X
    UPSIDE_DOWN = MIRROR_Y

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Loading interface. (internal API)"""
        dxf = super(DXFGraphic, self).load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, merged_text_group_codes)
            if processor.r12:
                # Transform elevation attribute from R11 to z-axis values:
                elevation_to_z_axis(dxf, ("insert", "align_point"))
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags. (internal API)"""
        super().export_entity(tagwriter)
        self.export_acdb_text(tagwriter)
        self.export_acdb_text2(tagwriter)

    def export_acdb_text(self, tagwriter: AbstractTagWriter) -> None:
        """Export TEXT data as DXF tags. (internal API)"""
        if tagwriter.dxfversion > const.DXF12:
            tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_text.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "insert",
                "height",
                "text",
                "thickness",
                "rotation",
                "oblique",
                "style",
                "width",
                "text_generation_flag",
                "halign",
                "align_point",
                "extrusion",
            ],
        )

    def export_acdb_text2(self, tagwriter: AbstractTagWriter) -> None:
        """Export TEXT data as DXF tags. (internal API)"""
        if tagwriter.dxfversion > const.DXF12:
            tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_text2.name)
        self.dxf.export_dxf_attribs(tagwriter, "valign")

    def set_placement(
        self,
        p1: UVec,
        p2: Optional[UVec] = None,
        align: Optional[TextEntityAlignment] = None,
    ) -> Text:
        """Set text alignment and location.

        The alignments :attr:`ALIGNED` and :attr:`FIT`
        are special, they require a second alignment point, the text is aligned
        on the virtual line between these two points and sits vertically at the
        baseline.

        - :attr:`ALIGNED`: Text is stretched or compressed
          to fit exactly between `p1` and `p2` and the text height is also
          adjusted to preserve height/width ratio.
        - :attr:`FIT`: Text is stretched or compressed to fit
          exactly between `p1` and `p2` but only the text width is adjusted,
          the text height is fixed by the :attr:`dxf.height` attribute.
        - :attr:`MIDDLE`: also a special adjustment, centered
          text like :attr:`MIDDLE_CENTER`, but vertically
          centred at the total height of the text.

        Args:
            p1: first alignment point as (x, y[, z])
            p2: second alignment point as (x, y[, z]), required for :attr:`ALIGNED`
                and :attr:`FIT` else ignored
            align: new alignment as enum :class:`~ezdxf.enums.TextEntityAlignment`,
                ``None`` to preserve the existing alignment.

        """
        if align is None:
            align = self.get_align_enum()
        else:
            assert isinstance(align, TextEntityAlignment)
            self.set_align_enum(align)
        self.dxf.insert = p1
        if align in (TextEntityAlignment.ALIGNED, TextEntityAlignment.FIT):
            if p2 is None:
                raise const.DXFValueError(
                    f"Alignment '{str(align)}' requires a second alignment point."
                )
        else:
            p2 = p1
        self.dxf.align_point = p2
        return self

    def get_placement(self) -> tuple[TextEntityAlignment, Vec3, Optional[Vec3]]:
        """Returns a tuple (`align`, `p1`, `p2`), `align` is the alignment
        enum :class:`~ezdxf.enum.TextEntityAlignment`, `p1` is the
        alignment point, `p2` is only relevant if `align` is :attr:`ALIGNED` or
        :attr:`FIT`, otherwise it is ``None``.

        """
        p1 = Vec3(self.dxf.insert)
        # Except for "LEFT" is the "align point" the real insert point:
        # If the required "align point" is not present use "insert"!
        p2 = Vec3(self.dxf.get("align_point", p1))
        align = self.get_align_enum()
        if align is TextEntityAlignment.LEFT:
            return align, p1, None
        if align in (TextEntityAlignment.FIT, TextEntityAlignment.ALIGNED):
            return align, p1, p2
        return align, p2, None

    def set_align_enum(self, align=TextEntityAlignment.LEFT) -> Text:
        """Just for experts: Sets the text alignment without setting the
        alignment points, set adjustment points attr:`dxf.insert` and
        :attr:`dxf.align_point` manually.

        Args:
            align: :class:`~ezdxf.enums.TextEntityAlignment`

        """
        halign, valign = MAP_TEXT_ENUM_TO_ALIGN_FLAGS[align]
        self.dxf.halign = halign
        self.dxf.valign = valign
        return self

    def get_align_enum(self) -> TextEntityAlignment:
        """Returns the current text alignment as :class:`~ezdxf.enums.TextEntityAlignment`,
        see also :meth:`set_placement`.
        """
        halign = self.dxf.get("halign", 0)
        valign = self.dxf.get("valign", 0)
        if halign > 2:
            valign = 0
        return MAP_TEXT_ALIGN_FLAGS_TO_ENUM.get(
            (halign, valign), TextEntityAlignment.LEFT
        )

    def transform(self, m: Matrix44) -> Text:
        """Transform the TEXT entity by transformation matrix `m` inplace."""
        dxf = self.dxf
        if not dxf.hasattr("align_point"):
            dxf.align_point = dxf.insert
        ocs = OCSTransform(self.dxf.extrusion, m)
        dxf.insert = ocs.transform_vertex(dxf.insert)
        dxf.align_point = ocs.transform_vertex(dxf.align_point)
        old_rotation = dxf.rotation
        new_rotation = ocs.transform_deg_angle(old_rotation)
        x_scale = ocs.transform_length(Vec3.from_deg_angle(old_rotation))
        y_scale = ocs.transform_length(Vec3.from_deg_angle(old_rotation + 90.0))

        if not ocs.scale_uniform:
            oblique_vec = Vec3.from_deg_angle(old_rotation + 90.0 - dxf.oblique)
            new_oblique_deg = (
                new_rotation
                + 90.0
                - ocs.transform_direction(oblique_vec).angle_deg
            )
            dxf.oblique = new_oblique_deg
            y_scale *= math.cos(math.radians(new_oblique_deg))

        dxf.width *= x_scale / y_scale
        dxf.height *= y_scale
        dxf.rotation = new_rotation

        if dxf.hasattr("thickness"):  # can be negative
            dxf.thickness = ocs.transform_thickness(dxf.thickness)
        dxf.extrusion = ocs.new_extrusion
        self.post_transform(m)
        return self

    def translate(self, dx: float, dy: float, dz: float) -> Text:
        """Optimized TEXT/ATTRIB/ATTDEF translation about `dx` in x-axis, `dy`
        in y-axis and `dz` in z-axis, returns `self`.

        """
        ocs = self.ocs()
        dxf = self.dxf
        vec = Vec3(dx, dy, dz)

        dxf.insert = ocs.from_wcs(vec + ocs.to_wcs(dxf.insert))
        if dxf.hasattr("align_point"):
            dxf.align_point = ocs.from_wcs(vec + ocs.to_wcs(dxf.align_point))
        # Avoid Matrix44 instantiation if not required:
        if self.is_post_transform_required:
            self.post_transform(Matrix44.translate(dx, dy, dz))
        return self

    def remove_dependencies(self, other: Optional[Drawing] = None) -> None:
        """Remove all dependencies from actual document.

        (internal API)
        """
        if not self.is_alive:
            return

        super().remove_dependencies()
        has_style = other is not None and (self.dxf.style in other.styles)
        if not has_style:
            self.dxf.style = "Standard"

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        super().register_resources(registry)
        if self.dxf.hasattr("style"):
            registry.add_text_style(self.dxf.style)

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        super().map_resources(clone, mapping)
        if clone.dxf.hasattr("style"):
            clone.dxf.style = mapping.get_text_style(clone.dxf.style)

    def plain_text(self) -> str:
        """Returns text content without formatting codes."""
        return plain_text(self.dxf.text)

    def audit(self, auditor: Auditor):
        """Validity check."""
        super().audit(auditor)
        auditor.check_text_style(self)

    @property
    def is_backward(self) -> bool:
        """Get/set text generation flag BACKWARDS, for mirrored text along the
        x-axis.
        """
        return bool(self.dxf.text_generation_flag & const.BACKWARD)

    @is_backward.setter
    def is_backward(self, state) -> None:
        self.set_flag_state(const.BACKWARD, state, "text_generation_flag")

    @property
    def is_upside_down(self) -> bool:
        """Get/set text generation flag UPSIDE_DOWN, for mirrored text along
        the y-axis.

        """
        return bool(self.dxf.text_generation_flag & const.UPSIDE_DOWN)

    @is_upside_down.setter
    def is_upside_down(self, state) -> None:
        self.set_flag_state(const.UPSIDE_DOWN, state, "text_generation_flag")

    def wcs_transformation_matrix(self) -> Matrix44:
        return text_transformation_matrix(self)

    def font_name(self) -> str:
        """Returns the font name of the associated :class:`Textstyle`."""
        font_name = "arial.ttf"
        style_name = self.dxf.style
        if self.doc:
            try:
                style = self.doc.styles.get(style_name)
                font_name = style.dxf.font
            except ValueError:
                pass
        return font_name

    def fit_length(self) -> float:
        """Returns the text length for alignments :attr:`TextEntityAlignment.FIT`
        and :attr:`TextEntityAlignment.ALIGNED`, defined by the distance from
        the insertion point to the align point or 0 for all other alignments.

        """
        length = 0.0
        align, p1, p2 = self.get_placement()
        if align in (TextEntityAlignment.FIT, TextEntityAlignment.ALIGNED):
            # text is stretch between p1 and p2
            length = p1.distance(p2)
        return length


def text_transformation_matrix(entity: Text) -> Matrix44:
    """Apply rotation, width factor, translation to the insertion point
    and if necessary transformation from OCS to WCS.
    """
    angle = math.radians(entity.dxf.rotation)
    width_factor = entity.dxf.width
    align, p1, p2 = entity.get_placement()
    mirror_x = -1 if entity.is_backward else 1
    mirror_y = -1 if entity.is_upside_down else 1
    oblique = math.radians(entity.dxf.oblique)
    location = p1
    if align in (TextEntityAlignment.ALIGNED, TextEntityAlignment.FIT):
        width_factor = 1.0  # text goes from p1 to p2, no stretching applied
        location = p1.lerp(p2, factor=0.5)
        angle = (p2 - p1).angle  # override stored angle

    m = Matrix44()
    if oblique:
        m *= Matrix44.shear_xy(angle_x=oblique)
    sx = width_factor * mirror_x
    sy = mirror_y
    if sx != 1 or sy != 1:
        m *= Matrix44.scale(sx, sy, 1)
    if angle:
        m *= Matrix44.z_rotate(angle)
    if location:
        m *= Matrix44.translate(location.x, location.y, location.z)

    ocs = entity.ocs()
    if ocs.transform:  # to WCS
        m *= ocs.matrix
    return m
