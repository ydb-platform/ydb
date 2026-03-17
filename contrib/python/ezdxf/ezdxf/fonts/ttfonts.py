#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Any, no_type_check
from fontTools.pens.basePen import BasePen
from fontTools.ttLib import TTFont

from ezdxf.math import Matrix44, UVec, BoundingBox2d
from ezdxf.path import Path
from .font_manager import FontManager, UnsupportedFont
from .font_measurements import FontMeasurements
from .glyphs import GlyphPath, Glyphs

UNICODE_WHITE_SQUARE = 9633  # U+25A1
UNICODE_REPLACEMENT_CHAR = 65533  # U+FFFD

font_manager = FontManager()


class PathPen(BasePen):
    def __init__(self, glyph_set) -> None:
        super().__init__(glyph_set)
        self._path = Path()

    @property
    def path(self) -> GlyphPath:
        return GlyphPath(self._path)

    def _moveTo(self, pt: UVec) -> None:
        self._path.move_to(pt)

    def _lineTo(self, pt: UVec) -> None:
        self._path.line_to(pt)

    def _curveToOne(self, pt1: UVec, pt2: UVec, pt3: UVec) -> None:
        self._path.curve4_to(pt3, pt1, pt2)

    def _qCurveToOne(self, pt1: UVec, pt2: UVec) -> None:
        self._path.curve3_to(pt2, pt1)

    def _closePath(self) -> None:
        self._path.close_sub_path()


class NoKerning:
    def get(self, c0: str, c1: str) -> float:
        return 0.0


class KerningTable(NoKerning):
    __slots__ = ("_cmap", "_kern_table")

    def __init__(self, font: TTFont, cmap, fmt: int = 0):
        self._cmap = cmap
        self._kern_table = font["kern"].getkern(fmt)

    def get(self, c0: str, c1: str) -> float:
        try:
            return self._kern_table[(self._cmap[ord(c0)], self._cmap[ord(c1)])]
        except (KeyError, TypeError):
            return 0.0


def get_fontname(font: TTFont) -> str:
    names = font["name"].names
    for record in names:
        if record.nameID == 1:
            return record.string.decode(record.getEncoding())
    return "unknown"


class TTFontRenderer(Glyphs):
    def __init__(self, font: TTFont, kerning=False):
        self._glyph_path_cache: dict[str, GlyphPath] = dict()
        self._generic_glyph_cache: dict[str, Any] = dict()
        self._glyph_width_cache: dict[str, float] = dict()
        self.font = font
        self.cmap = self.font.getBestCmap()
        if self.cmap is None:
            raise UnsupportedFont(f"font '{self.font_name}' has no character map.")
        self.glyph_set = self.font.getGlyphSet()
        self.kerning = NoKerning()
        if kerning:
            try:
                self.kerning = KerningTable(self.font, self.cmap)
            except KeyError:  # kerning table does not exist
                pass
        self.undefined_generic_glyph = self.glyph_set[".notdef"]
        self.font_measurements = self._get_font_measurements()
        self.space_width = self.detect_space_width()

    @property
    def font_name(self) -> str:
        return get_fontname(self.font)

    @no_type_check
    def _get_font_measurements(self) -> FontMeasurements:
        bbox = BoundingBox2d(self.get_glyph_path("x").control_vertices())
        baseline = bbox.extmin.y
        x_height = bbox.extmax.y - baseline
        bbox = BoundingBox2d(self.get_glyph_path("A").control_vertices())
        cap_height = bbox.extmax.y - baseline
        bbox = BoundingBox2d(self.get_glyph_path("p").control_vertices())
        descender_height = baseline - bbox.extmin.y
        return FontMeasurements(
            baseline=baseline,
            cap_height=cap_height,
            x_height=x_height,
            descender_height=descender_height,
        )

    def get_scaling_factor(self, cap_height: float) -> float:
        return 1.0 / self.font_measurements.cap_height * cap_height

    def get_generic_glyph(self, char: str):
        try:
            return self._generic_glyph_cache[char]
        except KeyError:
            pass
        try:
            generic_glyph = self.glyph_set[self.cmap[ord(char)]]
        except KeyError:
            generic_glyph = self.undefined_generic_glyph
        self._generic_glyph_cache[char] = generic_glyph
        return generic_glyph

    def get_glyph_path(self, char: str) -> GlyphPath:
        """Returns the raw glyph path, without any scaling applied."""
        try:
            return self._glyph_path_cache[char].clone()
        except KeyError:
            pass
        pen = PathPen(self.glyph_set)
        self.get_generic_glyph(char).draw(pen)
        glyph_path = pen.path
        self._glyph_path_cache[char] = glyph_path
        return glyph_path.clone()

    def get_glyph_width(self, char: str) -> float:
        """Returns the raw glyph width, without any scaling applied."""
        try:
            return self._glyph_width_cache[char]
        except KeyError:
            pass
        width = 0.0
        try:
            width = self.get_generic_glyph(char).width
        except KeyError:
            pass
        self._glyph_width_cache[char] = width
        return width

    def get_text_glyph_paths(
        self, s: str, cap_height: float = 1.0, width_factor: float = 1.0
    ) -> list[GlyphPath]:
        """Returns the glyph paths of string `s` as a list, scaled to cap height."""
        glyph_paths: list[GlyphPath] = []
        x_offset: float = 0
        requires_kerning = isinstance(self.kerning, KerningTable)
        resize_factor = self.get_scaling_factor(cap_height)
        y_factor = resize_factor
        x_factor = resize_factor * width_factor
        # set scaling factor:
        m = Matrix44.scale(x_factor, y_factor, 1.0)
        # set vertical offset:
        m[3, 1] = -self.font_measurements.baseline * y_factor
        prev_char = ""

        for char in s:
            if requires_kerning:
                x_offset += self.kerning.get(prev_char, char) * x_factor
            # set horizontal offset:
            m[3, 0] = x_offset
            glyph_path = self.get_glyph_path(char)
            glyph_path.transform_inplace(m)
            if len(glyph_path):
                glyph_paths.append(glyph_path)
            x_offset += self.get_glyph_width(char) * x_factor
            prev_char = char
        return glyph_paths

    def detect_space_width(self) -> float:
        """Returns the space width for the raw (unscaled) font."""
        return self.get_glyph_width(" ")

    def _get_text_length_with_kerning(self, s: str, cap_height: float = 1.0) -> float:
        length = 0.0
        c0 = ""
        kern = self.kerning.get
        width = self.get_glyph_width
        for c1 in s:
            length += kern(c0, c1) + width(c1)
            c0 = c1
        return length * self.get_scaling_factor(cap_height)

    def get_text_length(
        self, s: str, cap_height: float = 1.0, width_factor: float = 1.0
    ) -> float:
        if isinstance(self.kerning, KerningTable):
            return self._get_text_length_with_kerning(s, cap_height) * width_factor
        width = self.get_glyph_width
        return (
            sum(width(c) for c in s)
            * self.get_scaling_factor(cap_height)
            * width_factor
        )
