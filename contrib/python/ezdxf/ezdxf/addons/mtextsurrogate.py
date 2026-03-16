# Copyright (c) 2010-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Any
import math
import enum


from ezdxf.enums import (
    MTextEntityAlignment,
    MAP_MTEXT_ALIGN_TO_FLAGS,
)
from ezdxf.lldxf import const
from ezdxf.math import UVec, Vec3
from .mixins import SubscriptAttributes

if TYPE_CHECKING:
    from ezdxf.eztypes import GenericLayoutType

TOP_ALIGN = {
    MTextEntityAlignment.TOP_LEFT,
    MTextEntityAlignment.TOP_RIGHT,
    MTextEntityAlignment.TOP_CENTER,
}
MIDDLE_ALIGN = {
    MTextEntityAlignment.MIDDLE_LEFT,
    MTextEntityAlignment.MIDDLE_CENTER,
    MTextEntityAlignment.MIDDLE_RIGHT,
}


class Mirror(enum.IntEnum):
    NONE = 0
    MIRROR_X = 2
    MIRROR_Y = 4


class MTextSurrogate(SubscriptAttributes):
    """MTEXT surrogate for DXF R12 build up by TEXT Entities. This add-on was
    added to simplify the transition from :mod:`dxfwrite` to :mod:`ezdxf`.

    The rich-text formatting capabilities for the regular MTEXT entity are not
    supported, if these features are required use the regular MTEXT entity and
    the :class:`~ezdxf.addons.MTextExplode` add-on to explode the MTEXT entity
    into DXF primitives.

    .. important::

        The align-point is always the insert-point, there is no need for
        a second align-point because the horizontal alignments FIT, ALIGN,
        BASELINE_MIDDLE are not supported.

    Args:
        text: content as string
        insert: insert location in drawing units
        line_spacing: line spacing in percent of height, 1.5 = 150% = 1+1/2 lines
        align: text alignment as :class:`~ezdxf.enums.MTextEntityAlignment` enum
        char_height: text height in drawing units
        style: :class:`~ezdxf.entities.Textstyle` name as string
        oblique: oblique angle in degrees, where 0 is vertical
        rotation: text rotation angle in degrees
        width_factor: text width factor as float
        mirror: :attr:`MTextSurrogate.MIRROR_X` to mirror the text horizontal
            or :attr:`MTextSurrogate.MIRROR_Y` to mirror the text vertical
        layer: layer name as string
        color: :ref:`ACI`

    """

    MIRROR_NONE = Mirror.NONE
    MIRROR_X = Mirror.MIRROR_X
    MIRROR_Y = Mirror.MIRROR_Y

    def __init__(
        self,
        text: str,
        insert: UVec,
        line_spacing: float = 1.5,
        align=MTextEntityAlignment.TOP_LEFT,
        char_height: float = 1.0,
        style="STANDARD",
        oblique: float = 0.0,
        rotation: float = 0.0,
        width_factor: float = 1.0,
        mirror=Mirror.NONE,
        layer="0",
        color: int = const.BYLAYER,
    ):
        self.content: list[str] = text.split("\n")
        self.insert = Vec3(insert)
        self.line_spacing = float(line_spacing)
        assert isinstance(align, MTextEntityAlignment)
        self.align = align

        self.char_height = float(char_height)
        self.style = str(style)
        self.oblique = float(oblique)  # in degree
        self.rotation = float(rotation)  # in degree
        self.width_factor = float(width_factor)
        self.mirror = int(mirror)  # renamed to text_generation_flag in ezdxf
        self.layer = str(layer)
        self.color = int(color)

    @property
    def line_height(self) -> float:
        """Absolute line spacing in drawing units."""
        return self.char_height * self.line_spacing

    def render(self, layout: GenericLayoutType) -> None:
        """Render the multi-line content as separated TEXT entities into the
        given `layout` instance.
        """
        text_lines = self.content
        if len(text_lines) > 1:
            if self.mirror & const.MIRROR_Y:
                text_lines.reverse()
            for line_number, text in enumerate(text_lines):
                align_point = self._get_align_point(line_number)
                layout.add_text(
                    text,
                    dxfattribs=self._dxfattribs(align_point),
                )
        elif len(text_lines) == 1:
            layout.add_text(
                text_lines[0],
                dxfattribs=self._dxfattribs(self.insert),
            )

    def _get_align_point(self, line_number: int) -> Vec3:
        """Calculate the align-point depending on the line number."""
        x = self.insert.x
        y = self.insert.y
        try:
            z = self.insert.z
        except IndexError:
            z = 0.0
        # rotation not respected

        if self.align in TOP_ALIGN:
            y -= line_number * self.line_height
        elif self.align in MIDDLE_ALIGN:
            y0 = line_number * self.line_height
            full_height = (len(self.content) - 1) * self.line_height
            y += (full_height / 2) - y0
        else:  # BOTTOM ALIGN
            y += (len(self.content) - 1 - line_number) * self.line_height
        return self._rotate(Vec3(x, y, z))

    def _rotate(self, alignpoint: Vec3) -> Vec3:
        """Rotate `alignpoint` around insert-point about rotation degrees."""
        dx = alignpoint.x - self.insert.x
        dy = alignpoint.y - self.insert.y
        beta = math.radians(self.rotation)
        x = self.insert.x + dx * math.cos(beta) - dy * math.sin(beta)
        y = self.insert.y + dy * math.cos(beta) + dx * math.sin(beta)
        return Vec3(round(x, 6), round(y, 6), alignpoint.z)

    def _dxfattribs(self, align_point: Vec3) -> dict[str, Any]:
        """Build keyword arguments for TEXT entity creation."""
        halign, valign = MAP_MTEXT_ALIGN_TO_FLAGS[self.align]
        return {
            "insert": align_point,
            "align_point": align_point,
            "layer": self.layer,
            "color": self.color,
            "style": self.style,
            "height": self.char_height,
            "width": self.width_factor,
            "text_generation_flag": self.mirror,
            "rotation": self.rotation,
            "oblique": self.oblique,
            "halign": halign,
            "valign": valign,
        }
