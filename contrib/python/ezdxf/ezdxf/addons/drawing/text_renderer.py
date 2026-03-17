#  Copyright (c) 2022-2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import TypeVar, TYPE_CHECKING
import abc
from ezdxf.fonts import fonts

if TYPE_CHECKING:
    from ezdxf.npshapes import NumpyPath2d

T = TypeVar("T")


class TextRenderer(abc.ABC):
    """Minimal requirement to be usable as a universal text renderer"""

    @abc.abstractmethod
    def get_font_measurements(
        self, font_face: fonts.FontFace, cap_height: float = 1.0
    ) -> fonts.FontMeasurements:
        ...

    @abc.abstractmethod
    def get_text_line_width(
        self,
        text: str,
        font_face: fonts.FontFace,
        cap_height: float = 1.0,
    ) -> float:
        ...

    @abc.abstractmethod
    def get_text_path(
        self, text: str, font_face: fonts.FontFace, cap_height: float = 1.0
    ) -> NumpyPath2d:
        ...

    @abc.abstractmethod
    def get_text_glyph_paths(
        self, text: str, font_face: fonts.FontFace, cap_height: float = 1.0
    ) -> list[NumpyPath2d]:
        ...

    @abc.abstractmethod
    def is_stroke_font(self, font_face: fonts.FontFace) -> bool:
        ...
