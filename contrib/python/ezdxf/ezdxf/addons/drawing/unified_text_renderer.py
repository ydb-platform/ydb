# Copyright (c) 2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING
from ezdxf.fonts import fonts
from ezdxf.fonts.font_measurements import FontMeasurements

from .text_renderer import TextRenderer

if TYPE_CHECKING:
    from ezdxf.npshapes import NumpyPath2d


class UnifiedTextRenderer(TextRenderer):
    """This text renderer supports .ttf, .ttc, .otf, .shx, .shp and .lff fonts.

    The resolving order for .shx fonts is applied in the RenderContext.add_text_style()
    method.

    """

    def __init__(self) -> None:
        self._font_cache: dict[str, fonts.AbstractFont] = dict()

    def get_font(self, font_face: fonts.FontFace) -> fonts.AbstractFont:
        if not font_face.filename and font_face.family:
            found = fonts.find_best_match(
                family=font_face.family,
                weight=700 if font_face.is_bold else 400,
                italic=font_face.is_italic,
            )
            if found is not None:
                font_face = found
        key = font_face.filename.lower()
        try:
            return self._font_cache[key]
        except KeyError:
            pass
        abstract_font = fonts.make_font(font_face.filename, 1.0)
        self._font_cache[key] = abstract_font
        return abstract_font

    def is_stroke_font(self, font_face: fonts.FontFace) -> bool:
        abstract_font = self.get_font(font_face)
        return abstract_font.font_render_type == fonts.FontRenderType.STROKE

    def get_font_measurements(
        self, font_face: fonts.FontFace, cap_height: float = 1.0
    ) -> FontMeasurements:
        abstract_font = self.get_font(font_face)
        return abstract_font.measurements.scale(cap_height)

    def get_text_path(
        self, text: str, font_face: fonts.FontFace, cap_height: float = 1.0
    ) -> NumpyPath2d:
        abstract_font = self.get_font(font_face)
        return abstract_font.text_path_ex(text, cap_height)

    def get_text_glyph_paths(
        self, text: str, font_face: fonts.FontFace, cap_height: float = 1.0
    ) -> list[NumpyPath2d]:
        abstract_font = self.get_font(font_face)
        return abstract_font.text_glyph_paths(text, cap_height)

    def get_text_line_width(
        self,
        text: str,
        font_face: fonts.FontFace,
        cap_height: float = 1.0,
    ) -> float:
        abstract_font = self.get_font(font_face)
        return abstract_font.text_width_ex(text, cap_height)
