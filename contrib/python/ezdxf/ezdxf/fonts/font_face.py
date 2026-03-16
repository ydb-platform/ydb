#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import NamedTuple


class FontFace(NamedTuple):
    # filename without parent directories e.g. "OpenSans-Regular.ttf"
    filename: str = ""
    family: str = "sans-serif"
    style: str = "Regular"
    weight: int = 400  # Normal - usWeightClass
    width: int = 5  # Medium(Normal) - usWidthClass

    @property
    def is_italic(self) -> bool:
        """Returns ``True`` if font face is italic."""
        return self.style.lower().find("italic") > -1

    @property
    def is_oblique(self) -> bool:
        """Returns ``True`` if font face is oblique."""
        return self.style.lower().find("oblique") > -1

    @property
    def is_bold(self) -> bool:
        """Returns ``True`` if font face weight > 400."""
        return self.weight > 400

    @property
    def weight_str(self) -> str:
        """Returns the :attr:`weight` as string e.g. "Thin", "Normal", "Bold", ..."""
        return get_weight_str(self.weight)

    @property
    def width_str(self) -> str:
        """Returns the :attr:`width` as string e.g. "Condensed", "Expanded", ..."""
        return get_width_str(self.width)

    def distance(self, font_face: FontFace) -> tuple[int, int]:
        return self.weight - font_face.weight, self.width - font_face.width


WEIGHT_STR = {
    100: "Thin",
    200: "ExtraLight",
    300: "Light",
    400: "Normal",
    500: "Medium",
    600: "SemiBold",
    700: "Bold",
    800: "ExtraBold",
    900: "Black",
}

WIDTH_STR = {
    1: "UltraCondensed",
    2: "ExtraCondensed",
    3: "Condensed",
    4: "SemiCondensed",
    5: "Medium",  # Normal
    6: "SemiExpanded",
    7: "Expanded",
    8: "ExtraExpanded",
    9: "UltraExpanded",
}


def get_weight_str(weight: int) -> str:
    """Returns the :attr:`weight` as string e.g. "Thin", "Normal", "Bold", ..."""
    key = max(min(round((weight + 1) / 100) * 100, 900), 100)
    return WEIGHT_STR[key]


def get_width_str(width: int) -> str:
    """Returns the :attr:`width` as string e.g. "Condensed", "Expanded", ..."""
    key = max(min(width, 9), 1)
    return WIDTH_STR[key]
