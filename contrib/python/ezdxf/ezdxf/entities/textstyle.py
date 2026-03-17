# Copyright (c) 2019-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
import logging
from ezdxf.lldxf import validator, const
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.lldxf.const import DXF12, SUBCLASS_MARKER
from ezdxf.entities.dxfentity import base_class, SubclassProcessor, DXFEntity
from ezdxf.entities.layer import acdb_symbol_table_record
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.fonts import fonts

__all__ = ["Textstyle"]
logger = logging.getLogger("ezdxf")

acdb_style = DefSubclass(
    "AcDbTextStyleTableRecord",
    {
        "name": DXFAttr(
            2,
            default="Standard",
            validator=validator.is_valid_table_name,
        ),
        # Flags: Standard flag values (bit-coded values):
        # 1 = If set, this entry describes a shape
        # 4 = Vertical text
        # 16 = If set, table entry is externally dependent on a xref
        # 32 = If both this bit and bit 16 are set, the externally dependent xref ...
        # 64 = If set, the table entry was referenced by at least one entity in ...
        # Vertical text works only for SHX fonts in AutoCAD and BricsCAD
        "flags": DXFAttr(70, default=0),
        # Fixed height, 0 if not fixed
        "height": DXFAttr(
            40,
            default=0,
            validator=validator.is_greater_or_equal_zero,
            fixer=RETURN_DEFAULT,
        ),
        # Width factor:  a.k.a. "Stretch"
        "width": DXFAttr(
            41,
            default=1,
            validator=validator.is_greater_zero,
            fixer=RETURN_DEFAULT,
        ),
        # Oblique angle in degree, 0 = vertical
        "oblique": DXFAttr(50, default=0),
        # Generation flags:
        # 2 = backward
        # 4 = mirrored in Y
        "generation_flags": DXFAttr(71, default=0),
        # Last height used:
        "last_height": DXFAttr(42, default=2.5),
        # Primary font file name:
        # ATTENTION: The font file name can be an empty string and the font family
        # may be stored in XDATA! See also posts at the (unrelated) issue #380.
        "font": DXFAttr(3, default=const.DEFAULT_TEXT_FONT),
        # Big font name, blank if none
        "bigfont": DXFAttr(4, default=""),
    },
)
acdb_style_group_codes = group_code_mapping(acdb_style)


# XDATA: This is not a reliable source for font data!
# 1001 <ctrl> ACAD
# 1000 <str> Arial  ; font-family sometimes an empty string!
# 1071 <int> 34  ; flags
# ----
# "Arial" "normal" flags = 34               = 0b00:00000000:00000000:00100010
# "Arial" "italic" flags = 16777250         = 0b01:00000000:00000000:00100010
# "Arial" "bold" flags = 33554466           = 0b10:00000000:00000000:00100010
# "Arial" "bold+italic" flags = 50331682    = 0b11:00000000:00000000:00100010


@register_entity
class Textstyle(DXFEntity):
    """DXF STYLE entity"""

    DXFTYPE = "STYLE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_symbol_table_record, acdb_style)
    ITALIC = 0b01000000000000000000000000
    BOLD = 0b10000000000000000000000000

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, acdb_style_group_codes)  # type: ignore
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_symbol_table_record.name)
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_style.name)

        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "name",
                "flags",
                "height",
                "width",
                "oblique",
                "generation_flags",
                "last_height",
                "font",
                "bigfont",
            ],
        )

    @property
    def has_extended_font_data(self) -> bool:
        """Returns ``True`` if extended font data is present."""
        return self.has_xdata("ACAD")

    def get_extended_font_data(self) -> tuple[str, bool, bool]:
        """Returns extended font data as tuple (font-family, italic-flag,
        bold-flag).

        The extended font data is optional and not reliable! Returns
        ("", ``False``, ``False``) if extended font data is not present.

        """
        family = ""
        italic = False
        bold = False
        try:
            xdata = self.get_xdata("ACAD")
        except const.DXFValueError:
            pass
        else:
            if len(xdata) > 1:
                group_code, value = xdata[0]
                if group_code == 1000:
                    family = value
                group_code, value = xdata[1]
                if group_code == 1071:
                    italic = bool(self.ITALIC & value)
                    bold = bool(self.BOLD & value)
        return family, italic, bold

    def set_extended_font_data(
        self, family: str = "", *, italic=False, bold=False
    ) -> None:
        """Set extended font data, the font-family name `family` is not
        validated by `ezdxf`. Overwrites existing data.
        """
        if self.has_xdata("ACAD"):
            self.discard_xdata("ACAD")

        flags = 34  # unknown default flags
        if italic:
            flags += self.ITALIC
        if bold:
            flags += self.BOLD
        self.set_xdata("ACAD", [(1000, family), (1071, flags)])

    def discard_extended_font_data(self):
        """Discard extended font data."""
        self.discard_xdata("ACAD")

    @property
    def is_backward(self) -> bool:
        """Get/set text generation flag BACKWARDS, for mirrored text along the
        x-axis.
        """
        return self.get_flag_state(const.BACKWARD, "generation_flags")

    @is_backward.setter
    def is_backward(self, state) -> None:
        self.set_flag_state(const.BACKWARD, state, "generation_flags")

    @property
    def is_upside_down(self) -> bool:
        """Get/set text generation flag UPSIDE_DOWN, for mirrored text along
        the y-axis.

        """
        return self.get_flag_state(const.UPSIDE_DOWN, "generation_flags")

    @is_upside_down.setter
    def is_upside_down(self, state) -> None:
        self.set_flag_state(const.UPSIDE_DOWN, state, "generation_flags")

    @property
    def is_vertical_stacked(self) -> bool:
        """Get/set style flag VERTICAL_STACKED, for vertical stacked text."""
        return self.get_flag_state(const.VERTICAL_STACKED, "flags")

    @is_vertical_stacked.setter
    def is_vertical_stacked(self, state) -> None:
        self.set_flag_state(const.VERTICAL_STACKED, state, "flags")

    @property
    def is_shape_file(self) -> bool:
        """``True`` if entry describes a shape."""
        return self.dxf.name == "" and bool(self.dxf.flags & 1)

    def make_font(
        self,
        cap_height: Optional[float] = None,
        width_factor: Optional[float] = None,
    ) -> fonts.AbstractFont:
        """Returns a font abstraction :class:`~ezdxf.tools.fonts.AbstractFont`
        for this text style. Returns a font for a cap height of 1, if the
        text style has auto height (:attr:`Textstyle.dxf.height` is 0) and
        the given `cap_height` is ``None`` or 0.
        Uses the :attr:`Textstyle.dxf.width` attribute if the given `width_factor`
        is ``None`` or 0, the default value is 1.
        The attribute :attr:`Textstyle.dxf.big_font` is ignored.
        """
        from ezdxf.fonts import fonts

        ttf = ""
        if self.has_extended_font_data:
            family, italic, bold = self.get_extended_font_data()
            if family:
                text_style = "Italic" if italic else "Regular"
                text_weight = 700 if bold else 400
                font_face = fonts.FontFace(
                    family=family, style=text_style, weight=text_weight
                )
                ttf = fonts.find_font_file_name(font_face)
        else:
            ttf = self.dxf.get("font", const.DEFAULT_TTF)
        if ttf == "":
            ttf = const.DEFAULT_TTF
        if cap_height is None or cap_height == 0.0:
            cap_height = self.dxf.height
        if cap_height == 0.0:
            cap_height = 1.0
        if width_factor is None or width_factor == 0.0:
            width_factor = self.dxf.width
        return fonts.make_font(ttf, cap_height, width_factor)  # type: ignore
