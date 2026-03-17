# Copyright (c) 2016 Felix Krull <f_krull@gmx.de>
# Released under the terms of the MIT license; see README.md.

"""
Generate a color based on an object's hash value.

Quick start:

>>> from colorhash import ColorHash
>>> c = ColorHash("Hello World")
>>> c.hsl
(131, 0.65, 0.5)
>>> c.rgb
(45, 210, 75)
>>> c.hex
'#2dd24b'
"""

from __future__ import annotations

from binascii import crc32
from typing import Any
from typing import Sequence
from typing import Union

MIN_HUE = 0
MAX_HUE = 360

IntOrFloat = Union[int, float]


def crc32_hash(obj: Any) -> int:
    """
    Generate a hash for ``obj``.

    This function first converts the object to a string and encodes it into
    UTF-8, then calculates and returns the CRC-32 checksum of the result. The
    hash is guaranteed to be as stable as the result of the object's ``__str__``
    method.
    """
    bs = str(obj).encode("utf-8")
    return crc32(bs) & 0xFFFFFFFF


def hue_to_rgb(p: float, q: float, t: float):
    """
    Converts hue to RGB component for HSL to RGB color conversion.

    Args:
        p (float): First intermediate value, typically calculated from lightness and
                   saturation.
        q (float): Second intermediate value, typically calculated from lightness and
                   saturation.
        t (float): Adjusted hue value, should be in the range [0, 1].

    Returns:
        float: The RGB component corresponding to the given hue.

    Notes:
        This function is used internally in HSL to RGB conversion algorithms.
    """
    if t < 0:
        t += 1
    elif t > 1:
        t -= 1

    if t < 1 / 6:
        return p + (q - p) * 6 * t
    if t < 1 / 2:
        return q
    if t < 2 / 3:
        return p + (q - p) * (2 / 3 - t) * 6
    return p


def hsl2rgb(hsl: tuple[float, float, float]) -> tuple[int, int, int]:
    """
    Converts an HSL color value to its corresponding RGB representation.

    Args:
        hsl (tuple[float, float, float]): A tuple containing the hue (0-360), saturation
        (0-1), and lightness (0-1) values.

    Returns:
        tuple[int, int, int]: A tuple containing the red, green, and blue values (each
        in the range 0-255).

    Note:
        The hue value should be in degrees (0-360), while saturation and lightness
        should be in the range 0 to 1.
    """
    h, s, l = hsl  # noqa: E741
    h /= MAX_HUE
    q = l * (1 + s) if l < 0.5 else l + s - l * s  # noqa: PLR2004
    p = 2 * l - q

    r = round(hue_to_rgb(p, q, h + 1 / 3) * 255)
    g = round(hue_to_rgb(p, q, h) * 255)
    b = round(hue_to_rgb(p, q, h - 1 / 3) * 255)

    return r, g, b


def rgb2hex(rgb: tuple[int, int, int]) -> str:
    """
    Format an RGB color value into a hexadecimal color string.

    >>> rgb2hex((255, 0, 0))
    '#ff0000'
    """
    try:
        return "#{:02x}{:02x}{:02x}".format(*rgb)
    except TypeError as exc:
        raise ValueError(rgb) from exc


def color_hash(
    obj: Any,
    lightness: Sequence[float] = (0.35, 0.5, 0.65),
    saturation: Sequence[float] = (0.35, 0.5, 0.65),
    min_h: int | None = None,
    max_h: int | None = None,
) -> tuple[float, float, float]:
    """
    Calculate the color for the given object.

    This function takes the same arguments as the ``ColorHash`` class.

    Returns:
        A ``(H, S, L)`` tuple.
    """
    # "all([x for x ...])" is actually faster than "all(x for x ...)"
    if not all([0.0 <= x <= 1.0 for x in lightness]):  # noqa: C419
        msg = "lightness params must be in range (0.0, 1.0)"
        raise ValueError(msg)
    if not all([0.0 <= x <= 1.0 for x in saturation]):  # noqa: C419
        msg = "saturation params must be in range (0.0, 1.0)"
        raise ValueError(msg)

    if min_h is None and max_h is not None:
        min_h = MIN_HUE
    if min_h is not None and max_h is None:
        max_h = MAX_HUE

    hash_val = crc32_hash(obj)
    h = hash_val % 359
    if min_h is not None and max_h is not None:
        if not (
            MIN_HUE <= min_h <= MAX_HUE
            and MIN_HUE <= max_h <= MAX_HUE
            and min_h <= max_h
        ):
            msg: str = "min_h and max_h must be in range [0, 360] with min_h <= max_h"
            raise ValueError(msg)
        h = (h / 1000) * (max_h - min_h) + min_h
    hash_val //= 360
    s = saturation[hash_val % len(saturation)]
    hash_val //= len(saturation)
    l = lightness[hash_val % len(lightness)]  # noqa

    return (h, s, l)


class ColorHash:
    """
    Generate a color value and provide it in several format.

    Args:
        obj: the value.
        lightness: a range of values, one of which will be picked for the
                   lightness component of the result. Can also be a single
                   number.
        saturation: a range of values, one of which will be picked for the
                    saturation component of the result. Can also be a single
                    number.
        min_h: if set, limit the hue component to this lower value.
        max_h: if set, limit the hue component to this upper value.

    Attributes:
        hsl: HSL representation of the color value.
        rgb: RGB representation of the color value.
        hex: hex-formatted RGB color value.
    """

    def __init__(
        self,
        obj: Any,
        lightness: Sequence[float] = (0.35, 0.5, 0.65),
        saturation: Sequence[float] = (0.35, 0.5, 0.65),
        min_h: int | None = None,
        max_h: int | None = None,
    ):
        self.hsl: tuple[float, float, float] = color_hash(
            obj=obj,
            lightness=lightness,
            saturation=saturation,
            min_h=min_h,
            max_h=max_h,
        )

    @property
    def rgb(self) -> tuple[int, int, int]:
        return hsl2rgb(self.hsl)

    @property
    def hex(self) -> str:
        return rgb2hex(self.rgb)
