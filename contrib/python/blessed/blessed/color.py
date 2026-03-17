"""
Sub-module providing color functions.

References,

- https://en.wikipedia.org/wiki/Color_difference
- http://www.easyrgb.com/en/math.php
- Measuring Colour by R.W.G. Hunt and M.R. Pointer
"""

# std imports
from math import cos, exp, sin, sqrt, atan2
from typing import Dict, Tuple, Callable
from functools import lru_cache

_RGB = Tuple[int, int, int]


def rgb_to_xyz(red: int, green: int, blue: int) -> Tuple[float, float, float]:
    """
    Convert standard RGB color to XYZ color.

    D65/2° standard illuminant.

    :arg int red: RGB value of Red.
    :arg int green: RGB value of Green.
    :arg int blue: RGB value of Blue.
    :returns: Tuple (X, Y, Z) representing XYZ color
    :rtype: tuple
    """
    rgb = []
    for int_val in red, green, blue:
        val = float(int_val) / 255.0
        if val > 0.04045:
            val = pow((val + 0.055) / 1.055, 2.4)
        else:
            val /= 12.92
        val *= 100
        rgb.append(val)

    r_float, g_float, b_float = rgb  # pylint: disable=unbalanced-tuple-unpacking
    x_val = r_float * 0.4124 + g_float * 0.3576 + b_float * 0.1805
    y_val = r_float * 0.2126 + g_float * 0.7152 + b_float * 0.0722
    z_val = r_float * 0.0193 + g_float * 0.1192 + b_float * 0.9505

    return x_val, y_val, z_val


def xyz_to_lab(x_val: float, y_val: float, z_val: float) -> Tuple[float, float, float]:
    """
    Convert XYZ color to CIE-Lab color.

    :arg float x_val: XYZ value of X.
    :arg float y_val: XYZ value of Y.
    :arg float z_val: XYZ value of Z.
    :returns: Tuple (L, a, b) representing CIE-Lab color
    :rtype: tuple  D65/2° standard illuminant
    """
    xyz = []
    for float_val, ref in (x_val, 95.047), (y_val, 100.0), (z_val, 108.883):
        val = float_val / ref
        val = pow(val, 1 / 3.0) if val > 0.008856 else 7.787 * val + 16 / 116.0
        xyz.append(val)

    x_float, y_float, z_float = xyz  # pylint: disable=unbalanced-tuple-unpacking
    cie_l = 116 * y_float - 16
    cie_a = 500 * (x_float - y_float)
    cie_b = 200 * (y_float - z_float)

    return cie_l, cie_a, cie_b


@lru_cache(maxsize=256)
def rgb_to_lab(red: int, green: int, blue: int) -> Tuple[float, float, float]:
    """
    Convert RGB color to CIE-Lab color.

    :arg int red: RGB value of Red.
    :arg int green: RGB value of Green.
    :arg int blue: RGB value of Blue.
    :returns: Tuple (L, a, b) representing CIE-Lab color
    :rtype: tuple  D65/2° standard illuminant
    """
    return xyz_to_lab(*rgb_to_xyz(red, green, blue))


def dist_rgb(rgb1: _RGB, rgb2: _RGB) -> float:
    """
    Determine distance between two rgb colors.

    :arg tuple rgb1: RGB color definition
    :arg tuple rgb2: RGB color definition
    :returns: Square of the distance between provided colors
    :rtype: float

    This works by treating RGB colors as coordinates in three dimensional
    space and finding the closest point within the configured color range
    using the formula::

        d^2 = (r2 - r1)^2 + (g2 - g1)^2 + (b2 - b1)^2

    For efficiency, the square of the distance is returned
    which is sufficient for comparisons
    """
    return sum(pow(rgb1[idx] - rgb2[idx], 2) for idx in (0, 1, 2))


def dist_rgb_weighted(rgb1: _RGB, rgb2: _RGB) -> float:
    """
    Determine the weighted distance between two rgb colors.

    :arg tuple rgb1: RGB color definition
    :arg tuple rgb2: RGB color definition
    :returns: Square of the distance between provided colors
    :rtype: float Similar to a standard distance formula, the values are weighted to approximate
        human perception of color differences For efficiency, the square of the distance is returned
        which is sufficient for comparisons
    """
    red_mean = (rgb1[0] + rgb2[0]) / 2.0

    return ((2 + red_mean / 256) * pow(rgb1[0] - rgb2[0], 2) +
            4 * pow(rgb1[1] - rgb2[1], 2) +
            (2 + (255 - red_mean) / 256) * pow(rgb1[2] - rgb2[2], 2))


def dist_cie76(rgb1: _RGB, rgb2: _RGB) -> float:
    """
    Determine distance between two rgb colors using the CIE76 algorithm.

    :arg tuple rgb1: RGB color definition
    :arg tuple rgb2: RGB color definition
    :returns: Square of the distance between provided colors
    :rtype: float For efficiency, the square of the distance is returned which is sufficient for
        comparisons
    """
    l_1, a_1, b_1 = rgb_to_lab(*rgb1)
    l_2, a_2, b_2 = rgb_to_lab(*rgb2)
    return pow(l_1 - l_2, 2) + pow(a_1 - a_2, 2) + pow(b_1 - b_2, 2)


def dist_cie94(rgb1: _RGB, rgb2: _RGB) -> float:
    # pylint: disable=too-many-locals
    """
    Determine distance between two rgb colors using the CIE94 algorithm.

    :arg tuple rgb1: RGB color definition
    :arg tuple rgb2: RGB color definition
    :returns: Square of the distance between provided colors
    :rtype: float For efficiency, the square of the distance is returned which is sufficient for
        comparisons
    """
    l_1, a_1, b_1 = rgb_to_lab(*rgb1)
    l_2, a_2, b_2 = rgb_to_lab(*rgb2)

    s_l = k_l = k_c = k_h = 1
    k_1 = 0.045
    k_2 = 0.015

    delta_l = l_1 - l_2
    delta_a = a_1 - a_2
    delta_b = b_1 - b_2
    c_1 = sqrt(a_1 ** 2 + b_1 ** 2)
    c_2 = sqrt(a_2 ** 2 + b_2 ** 2)
    delta_c = c_1 - c_2
    delta_h = sqrt(delta_a ** 2 + delta_b ** 2 + delta_c ** 2)
    s_c = 1 + k_1 * c_1
    s_h = 1 + k_2 * c_1

    return ((delta_l / (k_l * s_l)) ** 2 +
            (delta_c / (k_c * s_c)) ** 2 +
            (delta_h / (k_h * s_h)) ** 2)


def dist_cie2000(rgb1: _RGB, rgb2: _RGB) -> float:
    # pylint: disable=too-many-locals
    """
    Determine distance between two rgb colors using the CIE2000 algorithm.

    :arg tuple rgb1: RGB color definition
    :arg tuple rgb2: RGB color definition
    :returns: Square of the distance between provided colors
    :rtype: float For efficiency, the square of the distance is returned which is sufficient for
        comparisons
    """
    s_l = k_l = k_c = k_h = 1.0

    l_1, a_1, b_1 = rgb_to_lab(*rgb1)
    l_2, a_2, b_2 = rgb_to_lab(*rgb2)

    delta_l = l_2 - l_1
    l_mean = (l_1 + l_2) / 2

    c_1 = sqrt(a_1 ** 2 + b_1 ** 2)
    c_2 = sqrt(a_2 ** 2 + b_2 ** 2)
    c_mean = (c_1 + c_2) / 2
    delta_c = c_1 - c_2

    g_x = sqrt(c_mean ** 7 / (c_mean ** 7 + 25 ** 7))
    h_1 = atan2(b_1, a_1 + (a_1 / 2) * (1 - g_x)) % 360
    h_2 = atan2(b_2, a_2 + (a_2 / 2) * (1 - g_x)) % 360

    if 0 in (c_1, c_2):
        delta_h_prime = 0.0
        h_mean = h_1 + h_2
    else:
        delta_h_prime = h_2 - h_1
        if abs(delta_h_prime) <= 180:
            h_mean = (h_1 + h_2) / 2
        else:
            if h_2 <= h_1:
                delta_h_prime += 360.0
            else:
                delta_h_prime -= 360.0
            h_mean = (h_1 + h_2 + 360) / 2 if h_1 + h_2 < 360 else (h_1 + h_2 - 360) / 2

    delta_h = 2 * sqrt(c_1 * c_2) * sin(delta_h_prime / 2)

    t_x = (1 -
           0.17 * cos(h_mean - 30) +
           0.24 * cos(2 * h_mean) +
           0.32 * cos(3 * h_mean + 6) -
           0.20 * cos(4 * h_mean - 63))

    s_l = 1 + (0.015 * (l_mean - 50) ** 2) / sqrt(20 + (l_mean - 50) ** 2)
    s_c = 1 + 0.045 * c_mean
    s_h = 1 + 0.015 * c_mean * t_x
    r_t = -2 * g_x * sin(abs(60 * exp(-1 * abs((delta_h - 275) / 25) ** 2)))

    delta_l = delta_l / (k_l * s_l)
    delta_c = delta_c / (k_c * s_c)
    delta_h = delta_h / (k_h * s_h)

    return delta_l ** 2 + delta_c ** 2 + delta_h ** 2 + r_t * delta_c * delta_h


COLOR_DISTANCE_ALGORITHMS: Dict[str,
                                Callable[[_RGB,
                                          _RGB],
                                         float]] = {'rgb': dist_rgb,
                                                    'rgb-weighted': dist_rgb_weighted,
                                                    'cie76': dist_cie76,
                                                    'cie94': dist_cie94,
                                                    'cie2000': dist_cie2000}

# Precomputed lookup tables for fast 256-color xterm cube mapping
# Based on xterm's 256colres.pl: levels [0, 95, 135, 175, 215, 255] for 6x6x6 cube
_CUBE_LEVELS = (0, 95, 135, 175, 215, 255)

# Precomputed RGB to cube index mapping "level", (0-5) for each RGB value (0-255)
# Uses xterm thresholds based on midpoints between cube levels [0,95,135,175,215,255]
# Thresholds: 48, 115, 155, 195, 235
_RGB_TO_CUBE_IDX = tuple(
    0 if v < 48 else 1 if v < 115 else 2 if v < 155 else 3 if v < 195 else 4 if v < 235 else 5
    for v in range(256)
)

# Precomputed RGB to cube value mapping for each RGB value (0-255)
_RGB_TO_CUBE_VAL = tuple(_CUBE_LEVELS[_RGB_TO_CUBE_IDX[v]] for v in range(256))

# Precomputed grayscale index mapping from brightness value (0-255) to gray index (0-23)
# Formula: 8 + 10*i gives gray values, so i = (v-8)/10, clamped to [0,23]
_GRAY_IDX_FROM_V = tuple(
    0 if v < 8 else 23 if v > 238 else int(round((v - 8) / 10.0))
    for v in range(256)
)

# Precomputed gray values for each gray index (0-23)
_GRAY_VAL_FROM_IDX = tuple(8 + 10 * i for i in range(24))


def xterm256color_from_rgb(red: int, green: int, blue: int) -> Tuple[int, _RGB]:
    """
    Convert RGB values to xterm 256-color cube index and RGB approximation.

    Uses the 6x6x6 color cube (indices 16-231) with levels [0,95,135,175,215,255].

    :arg int red: RGB value of Red (0-255).
    :arg int green: RGB value of Green (0-255).
    :arg int blue: RGB value of Blue (0-255).
    :returns: Tuple (cube_index, (r, g, b)) representing the xterm cube index and RGB approximation
    :rtype: tuple
    """
    # Find nearest candidate by "6x6x6 cube", (indices 16-231):
    # 6x6x6 cube with levels [0,95,135,175,215,255]
    r_idx = _RGB_TO_CUBE_IDX[red]
    g_idx = _RGB_TO_CUBE_IDX[green]
    b_idx = _RGB_TO_CUBE_IDX[blue]
    cube_idx = 16 + 36 * r_idx + 6 * g_idx + b_idx
    cube_rgb = (_RGB_TO_CUBE_VAL[red], _RGB_TO_CUBE_VAL[green], _RGB_TO_CUBE_VAL[blue])

    return cube_idx, cube_rgb


def xterm256gray_from_rgb(red: int, green: int, blue: int) -> Tuple[int, _RGB]:
    """
    Convert RGB values to xterm 256-color grayscale index and RGB approximation.

    Uses the 24 grayscale entries (indices 232-255) with values 8+10*i.

    :arg int red: RGB value of Red (0-255).
    :arg int green: RGB value of Green (0-255).
    :arg int blue: RGB value of Blue (0-255).
    :returns: Tuple (gray_index, (r, g, b)) representing the xterm gray index and RGB approximation
    :rtype: tuple
    """
    # Grayscale candidate (indices 232-255):
    # 24 grays with values 8+10*i
    brightness = (red + green + blue) // 3
    gray_idx_offset = _GRAY_IDX_FROM_V[brightness]
    gray_idx = 232 + gray_idx_offset
    gray_val = _GRAY_VAL_FROM_IDX[gray_idx_offset]
    gray_rgb = (gray_val, gray_val, gray_val)

    return gray_idx, gray_rgb
