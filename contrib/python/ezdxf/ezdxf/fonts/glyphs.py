#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing_extensions import TypeAlias
import abc

from ezdxf.npshapes import NumpyPath2d
from .font_measurements import FontMeasurements

GlyphPath: TypeAlias = NumpyPath2d


class Glyphs(abc.ABC):
    font_measurements: FontMeasurements  # of the raw font
    space_width: float  # word spacing of the raw font

    @abc.abstractmethod
    def get_scaling_factor(self, cap_height: float) -> float:
        ...

    @abc.abstractmethod
    def get_text_length(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> float:
        ...

    def get_text_path(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> GlyphPath:
        glyph_paths = self.get_text_glyph_paths(text, cap_height, width_factor)
        if len(glyph_paths) == 0:
            return GlyphPath(None)
        return NumpyPath2d.concatenate(glyph_paths)

    @abc.abstractmethod
    def get_text_glyph_paths(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> list[GlyphPath]:
        ...
