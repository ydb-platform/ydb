#  Copyright (c) 2020-2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Union, NamedTuple, Sequence
from typing_extensions import Self
import math


class RGB(NamedTuple):
    """Named tuple representing an RGB color value.

    Attributes:
        r: red channel in range [0, 255]
        g: green channel in range [0, 255]
        b: blue channel in range [0, 255]
    """

    r: int
    g: int
    b: int

    def to_floats(self) -> tuple[float, float, float]:
        """Returns the color value as a tuple of floats in range [0, 1]."""
        return self.r / 255, self.g / 255, self.b / 255

    @classmethod
    def from_floats(cls, rgb: tuple[float, float, float]) -> Self:
        """Returns an :class:`RGB` instance from floats in range [0, 1]."""
        r = max(min(255, round(rgb[0] * 255)), 0)
        g = max(min(255, round(rgb[1] * 255)), 0)
        b = max(min(255, round(rgb[2] * 255)), 0)
        return cls(r, g, b)

    def to_hex(self) -> str:
        """Returns the color value as hex string "#RRGGBB"."""
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    @classmethod
    def from_hex(cls, color: str) -> Self:
        """Returns an :class:`RGB` instance from a hex color string, the `color` string
        is a hex string "RRGGBB" with an optional leading "#", an appended alpha
        channel is ignore.
        """
        hex_string = color.lstrip("#")
        r = int(hex_string[0:2], 16)
        g = int(hex_string[2:4], 16)
        b = int(hex_string[4:6], 16)
        return cls(r, g, b)

    @property
    def luminance(self) -> float:
        """Returns perceived luminance for an RGB color in range [0.0, 1.0]
        from dark to light.
        """
        return luminance(self)


class RGBA(NamedTuple):
    """Named tuple representing an RGBA color value. 
    The default alpha channel is 255 (opaque).

    Attributes:
        r: red channel in range [0, 255]
        g: green channel in range [0, 255]
        b: blue channel in range [0, 255]
        a: alpha channel in range [0, 255], where 0 is transparent and 255 is opaque
    """

    r: int
    g: int
    b: int
    a: int = 255  # default alpha channel is opaque

    def to_floats(self) -> tuple[float, float, float, float]:
        """Returns the color value as a tuple of floats in range [0, 1]."""
        return self.r / 255, self.g / 255, self.b / 255, self.a / 255

    @classmethod
    def from_floats(cls, values: Sequence[float]) -> Self:
        """Returns an :class:`RGBA` instance from floats in range [0, 1].

        The alpha channel is optional. 
        The default alpha channel is 255 (opaque).
        """
        r = max(min(255, round(values[0] * 255)), 0)
        g = max(min(255, round(values[1] * 255)), 0)
        b = max(min(255, round(values[2] * 255)), 0)
        try:
            a = max(min(255, round(values[3] * 255)), 0)
        except IndexError:
            a = 255  # default to opaque == RGB(r, g, b)
        return cls(r, g, b, a)

    def to_hex(self) -> str:
        """Returns the color value as hex string "#RRGGBBAA"."""
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}{self.a:02x}"

    @classmethod
    def from_hex(cls, color: str) -> Self:
        """Returns an :class:`RGBA` instance from a hex color string, the `color` string
        is a hex string "RRGGBBAA" with an optional leading "#". 
        The alpha channel is optional. 
        The default alpha channel is 255 (opaque).
        """
        hex_string = color.lstrip("#")
        r = int(hex_string[0:2], 16)
        g = int(hex_string[2:4], 16)
        b = int(hex_string[4:6], 16)
        a = 255
        if len(hex_string) > 6:
            a = int(hex_string[6:8], 16)
        return cls(r, g, b, a)

    @property
    def luminance(self) -> float:
        """Returns perceived luminance for an RGB color in range [0.0, 1.0]
        from dark to light.
        """
        return luminance(self)


BYBLOCK = 0
BYLAYER = 256
BYOBJECT = 257
RED = 1
YELLOW = 2
GREEN = 3
CYAN = 4
BLUE = 5
MAGENTA = 6
BLACK = 7
WHITE = 7
GRAY = 8
LIGHT_GRAY = 9

# Flags for raw color int values:
# Take color from layer, ignore other bytes.
COLOR_TYPE_BY_LAYER = 0xC0
# Take color from insertion, ignore other bytes
COLOR_TYPE_BY_BLOCK = 0xC1
# RGB value, other bytes are R,G,B.
COLOR_TYPE_RGB = 0xC2
# ACI, AutoCAD color index, other bytes are 0,0,index ???
COLOR_TYPE_ACI = 0xC3

# Found in MLEADER text background color (group code 91) = -939524096
# guess: use window background color
COLOR_TYPE_WINDOW_BG = 0xC8


def decode_raw_color(value: int) -> tuple[int, Union[int, RGB]]:
    """Decode :term:`raw-color` value as tuple(type, Union[aci, (r, g, b)]), the
    true color value is a (r, g, b) tuple.
    """
    data = decode_raw_color_int(value)
    if data[0] == COLOR_TYPE_RGB:
        return COLOR_TYPE_RGB, int2rgb(data[1])
    return data


def decode_raw_color_int(value: int) -> tuple[int, int]:
    """Decode :term:`raw-color` value as tuple(type, int), the true color value
    is a 24-bit int value.
    """
    flags = (value >> 24) & 0xFF
    if flags == COLOR_TYPE_BY_BLOCK:
        return COLOR_TYPE_BY_BLOCK, BYBLOCK
    elif flags == COLOR_TYPE_BY_LAYER:
        return COLOR_TYPE_BY_LAYER, BYLAYER
    elif flags == COLOR_TYPE_ACI:
        return COLOR_TYPE_ACI, value & 0xFF
    elif flags == COLOR_TYPE_RGB:
        return COLOR_TYPE_RGB, value & 0xFFFFFF
    elif flags == COLOR_TYPE_WINDOW_BG:
        return COLOR_TYPE_WINDOW_BG, 0
    else:
        raise ValueError(f"Unknown color type: 0x{flags:02x}")


BY_LAYER_RAW_VALUE = -1073741824  # -(-(0xc0 << 24) & 0xffffffff)
BY_BLOCK_RAW_VALUE = -1056964608  # -(-(0xc1 << 24) & 0xffffffff)
WINDOW_BG_RAW_VALUE = -939524096


def encode_raw_color(value: Union[int, RGB]) -> int:
    """Encode :term:`true-color` value or :ref:`ACI` color value into a :term:
    `raw color` value.
    """
    if isinstance(value, int):
        if value == BYBLOCK:
            return BY_BLOCK_RAW_VALUE
        elif value == BYLAYER:
            return BY_LAYER_RAW_VALUE
        elif 0 < value < 256:
            return -(-(COLOR_TYPE_ACI << 24) & 0xFFFFFFFF) | value
        else:  # BYOBJECT (257) -> resolve to object color
            raise ValueError(f"Invalid color index: {value}")
    else:
        return -(-((COLOR_TYPE_RGB << 24) + rgb2int(value)) & 0xFFFFFFFF)


TRANSPARENCY_BYBLOCK = 0x1000000
OPAQUE = 0x20000FF
TRANSPARENCY_10 = 0x20000E5
TRANSPARENCY_20 = 0x20000CC
TRANSPARENCY_30 = 0x20000B2
TRANSPARENCY_40 = 0x2000099
TRANSPARENCY_50 = 0x200007F
TRANSPARENCY_60 = 0x2000066
TRANSPARENCY_70 = 0x200004C
TRANSPARENCY_80 = 0x2000032
TRANSPARENCY_90 = 0x2000019


def float2transparency(value: float) -> int:
    """
    Returns DXF transparency value as integer in the range from 0 to 255,
    where 0 is 100% transparent and 255 is opaque.

    Args:
        value: transparency value as float in the range from 0 to 1, where 0 is
            opaque and 1 is 100% transparent.

    """
    return int((1.0 - float(value)) * 255) | 0x02000000


def transparency2float(value: int) -> float:
    """
    Returns transparency value as float from 0 to 1, 0 for no transparency
    (opaque) and 1 for 100% transparency.

    Args:
        value: DXF integer transparency value, 0 for 100% transparency and 255
            for opaque

    """
    # Transparency value 0x020000TT 0 = fully transparent / 255 = opaque
    # 255 -> 0.0
    # 0 -> 1.0
    return 1.0 - float(int(value) & 0xFF) / 255.0


def int2rgb(value: int) -> RGB:
    """Split RGB integer `value` into (r, g, b) tuple."""
    return RGB(
        (value >> 16) & 0xFF,  # red
        (value >> 8) & 0xFF,  # green
        value & 0xFF,  # blue
    )


def rgb2int(rgb: RGB | tuple[int, int, int]) -> int:
    """Combined integer value from (r, g, b) tuple."""
    r, g, b = rgb
    return ((int(r) & 0xFF) << 16) | ((int(g) & 0xFF) << 8) | (int(b) & 0xFF)


def aci2rgb(index: int) -> RGB:
    """Convert :ref:`ACI` into (r, g, b) tuple, based on default AutoCAD
    colors.
    """
    if index < 1:
        raise IndexError(index)
    return int2rgb(DXF_DEFAULT_COLORS[index])


def luminance(color: Sequence[float]) -> float:
    """Returns perceived luminance for an RGB color in the range [0.0, 1.0]
    from dark to light.
    """
    r = float(color[0]) / 255
    g = float(color[1]) / 255
    b = float(color[2]) / 255
    return round(math.sqrt(0.299 * r * r + 0.587 * g * g + 0.114 * b * b), 3)


# color codes are 1-indexed so an additional entry was put in the 0th position
# different plot styles may choose different colors for the same code
# from ftp://ftp.ecn.purdue.edu/jshan/86/help/html/import_export/dxf_colortable.htm
# alternative color tables can be found at:
#  - https://www.temblast.com/songview/color3.htm
#  - https://gohtx.com/acadcolors.php

# modelspace color palette for AutoCAD 2020
#  - DXF_DEFAULT_COLORS values change depending on Uniform background
#    color setting in options, these values correspond the default
#    2D model space Uniform background RGB color for AutoCAD 2020: 33,40,48
DXF_DEFAULT_COLORS = [
    0x000000,  # dummy value for index [0]
    0xFF0000,
    0xFFFF00,
    0x00FF00,
    0x00FFFF,
    0x0000FF,
    0xFF00FF,
    0xFFFFFF,
    0x808080,
    0xC0C0C0,
    0xFF0000,
    0xFF7F7F,
    0xA50000,
    0xA55252,
    0x7F0000,
    0x7F3F3F,
    0x4C0000,
    0x4C2626,
    0x260000,
    0x261313,
    0xFF3F00,
    0xFF9F7F,
    0xA52900,
    0xA56752,
    0x7F1F00,
    0x7F4F3F,
    0x4C1300,
    0x4C2F26,
    0x260900,
    0x261713,
    0xFF7F00,
    0xFFBF7F,
    0xA55200,
    0xA57C52,
    0x7F3F00,
    0x7F5F3F,
    0x4C2600,
    0x4C3926,
    0x261300,
    0x261C13,
    0xFFBF00,
    0xFFDF7F,
    0xA57C00,
    0xA59152,
    0x7F5F00,
    0x7F6F3F,
    0x4C3900,
    0x4C4226,
    0x261C00,
    0x262113,
    0xFFFF00,
    0xFFFF7F,
    0xA5A500,
    0xA5A552,
    0x7F7F00,
    0x7F7F3F,
    0x4C4C00,
    0x4C4C26,
    0x262600,
    0x262613,
    0xBFFF00,
    0xDFFF7F,
    0x7CA500,
    0x91A552,
    0x5F7F00,
    0x6F7F3F,
    0x394C00,
    0x424C26,
    0x1C2600,
    0x212613,
    0x7FFF00,
    0xBFFF7F,
    0x52A500,
    0x7CA552,
    0x3F7F00,
    0x5F7F3F,
    0x264C00,
    0x394C26,
    0x132600,
    0x1C2613,
    0x3FFF00,
    0x9FFF7F,
    0x29A500,
    0x67A552,
    0x1F7F00,
    0x4F7F3F,
    0x134C00,
    0x2F4C26,
    0x092600,
    0x172613,
    0x00FF00,
    0x7FFF7F,
    0x00A500,
    0x52A552,
    0x007F00,
    0x3F7F3F,
    0x004C00,
    0x264C26,
    0x002600,
    0x132613,
    0x00FF3F,
    0x7FFF9F,
    0x00A529,
    0x52A567,
    0x007F1F,
    0x3F7F4F,
    0x004C13,
    0x264C2F,
    0x002609,
    0x135817,
    0x00FF7F,
    0x7FFFBF,
    0x00A552,
    0x52A57C,
    0x007F3F,
    0x3F7F5F,
    0x004C26,
    0x264C39,
    0x002613,
    0x13581C,
    0x00FFBF,
    0x7FFFDF,
    0x00A57C,
    0x52A591,
    0x007F5F,
    0x3F7F6F,
    0x004C39,
    0x264C42,
    0x00261C,
    0x135858,
    0x00FFFF,
    0x7FFFFF,
    0x00A5A5,
    0x52A5A5,
    0x007F7F,
    0x3F7F7F,
    0x004C4C,
    0x264C4C,
    0x002626,
    0x135858,
    0x00BFFF,
    0x7FDFFF,
    0x007CA5,
    0x5291A5,
    0x005F7F,
    0x3F6F7F,
    0x00394C,
    0x26427E,
    0x001C26,
    0x135858,
    0x007FFF,
    0x7FBFFF,
    0x0052A5,
    0x527CA5,
    0x003F7F,
    0x3F5F7F,
    0x00264C,
    0x26397E,
    0x001326,
    0x131C58,
    0x003FFF,
    0x7F9FFF,
    0x0029A5,
    0x5267A5,
    0x001F7F,
    0x3F4F7F,
    0x00134C,
    0x262F7E,
    0x000926,
    0x131758,
    0x0000FF,
    0x7F7FFF,
    0x0000A5,
    0x5252A5,
    0x00007F,
    0x3F3F7F,
    0x00004C,
    0x26267E,
    0x000026,
    0x131358,
    0x3F00FF,
    0x9F7FFF,
    0x2900A5,
    0x6752A5,
    0x1F007F,
    0x4F3F7F,
    0x13004C,
    0x2F267E,
    0x090026,
    0x171358,
    0x7F00FF,
    0xBF7FFF,
    0x5200A5,
    0x7C52A5,
    0x3F007F,
    0x5F3F7F,
    0x26004C,
    0x39267E,
    0x130026,
    0x1C1358,
    0xBF00FF,
    0xDF7FFF,
    0x7C00A5,
    0x9152A5,
    0x5F007F,
    0x6F3F7F,
    0x39004C,
    0x42264C,
    0x1C0026,
    0x581358,
    0xFF00FF,
    0xFF7FFF,
    0xA500A5,
    0xA552A5,
    0x7F007F,
    0x7F3F7F,
    0x4C004C,
    0x4C264C,
    0x260026,
    0x581358,
    0xFF00BF,
    0xFF7FDF,
    0xA5007C,
    0xA55291,
    0x7F005F,
    0x7F3F6F,
    0x4C0039,
    0x4C2642,
    0x26001C,
    0x581358,
    0xFF007F,
    0xFF7FBF,
    0xA50052,
    0xA5527C,
    0x7F003F,
    0x7F3F5F,
    0x4C0026,
    0x4C2639,
    0x260013,
    0x58131C,
    0xFF003F,
    0xFF7F9F,
    0xA50029,
    0xA55267,
    0x7F001F,
    0x7F3F4F,
    0x4C0013,
    0x4C262F,
    0x260009,
    0x581317,
    0x000000,
    0x656565,
    0x666666,
    0x999999,
    0xCCCCCC,
    0xFFFFFF,
]


# paperspace color palette for AutoCAD 2020
#  - DXF_DEFAULT_PAPERSPACE_COLORS values change depending on Uniform background
#    color setting in options, these values correspond the default
#    Sheet/layout Uniform background color for AutoCAD 2020: White (RGB 255,255,255)
DXF_DEFAULT_PAPERSPACE_COLORS = [
    0x000000,  # dummy value for index [0]
    0xFF0000,
    0xFFFF00,
    0x00FF00,
    0x00FFFF,
    0x0000FF,
    0xFF00FF,
    0x000000,
    0x808080,
    0xC0C0C0,
    0xFF0000,
    0xFF7F7F,
    0xA50000,
    0xA55252,
    0x7F0000,
    0x7F3F3F,
    0x4C0000,
    0x4C2626,
    0x260000,
    0x261313,
    0xFF3F00,
    0xFF9F7F,
    0xA52900,
    0xA56752,
    0x7F1F00,
    0x7F4F3F,
    0x4C1300,
    0x4C2F26,
    0x260900,
    0x261713,
    0xFF7F00,
    0xFFBF7F,
    0xA55200,
    0xA57C52,
    0x7F3F00,
    0x7F5F3F,
    0x4C2600,
    0x4C3926,
    0x261300,
    0x261C13,
    0xFFBF00,
    0xFFDF7F,
    0xA57C00,
    0xA59152,
    0x7F5F00,
    0x7F6F3F,
    0x4C3900,
    0x4C4226,
    0x261C00,
    0x262113,
    0xFFFF00,
    0xFFFF7F,
    0xA5A500,
    0xA5A552,
    0x7F7F00,
    0x7F7F3F,
    0x4C4C00,
    0x4C4C26,
    0x262600,
    0x262613,
    0xBFFF00,
    0xDFFF7F,
    0x7CA500,
    0x91A552,
    0x5F7F00,
    0x6F7F3F,
    0x394C00,
    0x424C26,
    0x1C2600,
    0x212613,
    0x7FFF00,
    0xBFFF7F,
    0x52A500,
    0x7CA552,
    0x3F7F00,
    0x5F7F3F,
    0x264C00,
    0x394C26,
    0x132600,
    0x1C2613,
    0x3FFF00,
    0x9FFF7F,
    0x29A500,
    0x67A552,
    0x1F7F00,
    0x4F7F3F,
    0x134C00,
    0x2F4C26,
    0x092600,
    0x172613,
    0x00FF00,
    0x7FFF7F,
    0x00A500,
    0x52A552,
    0x007F00,
    0x3F7F3F,
    0x004C00,
    0x264C26,
    0x002600,
    0x132613,
    0x00FF3F,
    0x7FFF9F,
    0x00A529,
    0x52A567,
    0x007F1F,
    0x3F7F4F,
    0x004C13,
    0x264C2F,
    0x002609,
    0x132617,
    0x00FF7F,
    0x7FFFBF,
    0x00A552,
    0x52A57C,
    0x007F3F,
    0x3F7F5F,
    0x004C26,
    0x264C39,
    0x002613,
    0x13261C,
    0x00FFBF,
    0x7FFFDF,
    0x00A57C,
    0x52A591,
    0x007F5F,
    0x3F7F6F,
    0x004C39,
    0x264C42,
    0x00261C,
    0x132621,
    0x00FFFF,
    0x7FFFFF,
    0x00A5A5,
    0x52A5A5,
    0x007F7F,
    0x3F7F7F,
    0x004C4C,
    0x264C4C,
    0x002626,
    0x132626,
    0x00BFFF,
    0x7FDFFF,
    0x007CA5,
    0x5291A5,
    0x005F7F,
    0x3F6F7F,
    0x00394C,
    0x26424C,
    0x001C26,
    0x132126,
    0x007FFF,
    0x7FBFFF,
    0x0052A5,
    0x527CA5,
    0x003F7F,
    0x3F5F7F,
    0x00264C,
    0x26394C,
    0x001326,
    0x131C26,
    0x003FFF,
    0x7F9FFF,
    0x0029A5,
    0x5267A5,
    0x001F7F,
    0x3F4F7F,
    0x00134C,
    0x262F4C,
    0x000926,
    0x131726,
    0x0000FF,
    0x7F7FFF,
    0x0000A5,
    0x5252A5,
    0x00007F,
    0x3F3F7F,
    0x00004C,
    0x26264C,
    0x000026,
    0x131326,
    0x3F00FF,
    0x9F7FFF,
    0x2900A5,
    0x6752A5,
    0x1F007F,
    0x4F3F7F,
    0x13004C,
    0x2F264C,
    0x090026,
    0x171326,
    0x7F00FF,
    0xBF7FFF,
    0x5200A5,
    0x7C52A5,
    0x3F007F,
    0x5F3F7F,
    0x26004C,
    0x39264C,
    0x130026,
    0x1C1326,
    0xBF00FF,
    0xDF7FFF,
    0x7C00A5,
    0x9152A5,
    0x5F007F,
    0x6F3F7F,
    0x39004C,
    0x42264C,
    0x1C0026,
    0x211326,
    0xFF00FF,
    0xFF7FFF,
    0xA500A5,
    0xA552A5,
    0x7F007F,
    0x7F3F7F,
    0x4C004C,
    0x4C264C,
    0x260026,
    0x261326,
    0xFF00BF,
    0xFF7FDF,
    0xA5007C,
    0xA55291,
    0x7F005F,
    0x7F3F6F,
    0x4C0039,
    0x4C2642,
    0x26001C,
    0x261321,
    0xFF007F,
    0xFF7FBF,
    0xA50052,
    0xA5527C,
    0x7F003F,
    0x7F3F5F,
    0x4C0026,
    0x4C2639,
    0x260013,
    0x26131C,
    0xFF003F,
    0xFF7F9F,
    0xA50029,
    0xA55267,
    0x7F001F,
    0x7F3F4F,
    0x4C0013,
    0x4C262F,
    0x260009,
    0x261317,
    0x000000,
    0x2D2D2D,
    0x5B5B5B,
    0x898989,
    0xB7B7B7,
    0xB3B3B3,
]
