# Copyright 2010 Dirk Holtwick, holtwick.it
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import contextlib
import logging
import re
from copy import copy
from typing import Any

import arabic_reshaper
import reportlab
import reportlab.pdfbase._cidfontdata
from bidi.algorithm import get_display
from reportlab.lib.colors import Color, toColor
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.lib.units import cm, inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

import xhtml2pdf.default

log = logging.getLogger(__name__)

rgb_re = re.compile(
    r"^.*?rgb[a]?[(]([0-9]+).*?([0-9]+).*?([0-9]+)(?:.*?(?:[01]\.(?:[0-9]+)))?[)].*?[ ]*$"
)

# =========================================================================
# Memoize decorator
# =========================================================================


class Memoized:
    """
    A kwargs-aware memoizer, better than the one in python :).

    Don't pass in too large kwargs, since this turns them into a tuple of
    tuples. Also, avoid mutable types (as usual for memoizers)

    What this does is to create a dictionary of {(*parameters):return value},
    and uses it as a cache for subsequent calls to the same method.
    It is especially useful for functions that don't rely on external variables
    and that are called often. It's a perfect match for our getSize etc...
    """

    def __init__(self, func) -> None:
        self.cache: dict = {}
        self.func = func
        self.__doc__ = self.func.__doc__  # To avoid great confusion
        self.__name__ = self.func.__name__  # This also avoids great confusion

    def __call__(self, *args, **kwargs):
        # Make sure the following line is not actually slower than what you're
        # trying to memoize
        args_plus = tuple(kwargs.items())
        key = (args, args_plus)
        try:
            if key not in self.cache:
                res = self.func(*args, **kwargs)
                self.cache[key] = res
            return self.cache[key]
        except TypeError:
            # happens if any of the parameters is a list
            return self.func(*args, **kwargs)


def toList(value: Any, *, cast_tuple: bool = True) -> list:
    cls: tuple[type, ...] = (list, tuple) if cast_tuple else (list,)
    return list(value) if isinstance(value, cls) else [value]  # type: ignore[call-overload]


def transform_attrs(obj, keys, container, func, extras=None):
    """
    Allows to apply one function to set of keys checking if key is in container,
    also transform ccs key to report lab keys.

    extras = Are extra params for func, it will be call like func(*[param1, param2])

    obj = frag
    keys = [(reportlab, css), ... ]
    container = cssAttr
    """
    cpextras = extras

    for reportlab_key, css in keys:
        extras = cpextras
        if extras is None:
            extras = []
        elif not isinstance(extras, list):
            extras = [extras]
        if css in container:
            extras.insert(0, container[css])
            setattr(obj, reportlab_key, func(*extras))


def copy_attrs(obj1, obj2, attrs):
    """
    Allows copy a list of attributes from object2 to object1.
    Useful for copy ccs attributes to fragment.
    """
    for attr in attrs:
        value = getattr(obj2, attr) if hasattr(obj2, attr) else None
        if value is None and isinstance(obj2, dict) and attr in obj2:
            value = obj2[attr]
        setattr(obj1, attr, value)


def set_value(obj, attrs, value, *, do_copy=False):
    """Allows set the same value to a list of attributes."""
    for attr in attrs:
        if do_copy:
            value = copy(value)
        setattr(obj, attr, value)


@Memoized
def getColor(value, default=None):
    """
    Convert to color value.
    This returns a Color object instance from a text bit.
    Mitigation for ReDoS attack applied by limiting input length and validating input.
    """
    if value is None:
        return None
    if isinstance(value, Color):
        return value
    value = str(value).strip().lower()

    # Limit the length of the value to prevent excessive input causing ReDoS
    if len(value) > 100:  # Set a reasonable length limit to avoid extreme inputs
        return default

    if value in {"transparent", "none"}:
        return default
    if value in COLOR_BY_NAME:
        return COLOR_BY_NAME[value]
    if value.startswith("#") and len(value) == 4:
        value = "#" + value[1] + value[1] + value[2] + value[2] + value[3] + value[3]
    elif rgb_re.match(value):
        # Use match instead of search to ensure proper regex usage and limit to valid patterns
        try:
            r, g, b = (int(x) for x in rgb_re.match(value).groups())
            value = f"#{r:02x}{g:02x}{b:02x}"
        except ValueError:
            pass
    else:
        # Shrug
        pass

    return toColor(value, default)  # Calling the reportlab function


def getBorderStyle(value, default=None):
    if value and (str(value).lower() not in {"none", "hidden"}):
        return value
    return default


MM: float = cm / 10.0
DPI96: float = 1.0 / 96.0 * inch

ABSOLUTE_SIZE_TABLE: dict[str, float] = {
    "1": 50.0 / 100.0,
    "xx-small": 50.0 / 100.0,
    "x-small": 50.0 / 100.0,
    "2": 75.0 / 100.0,
    "small": 75.0 / 100.0,
    "3": 1.0,
    "medium": 1.0,
    "4": 125.0 / 100.0,
    "large": 125.0 / 100.0,
    "5": 150.0 / 100.0,
    "x-large": 150.0 / 100.0,
    "6": 175.0 / 100.0,
    "xx-large": 175.0 / 100.0,
    "7": 200.0 / 100.0,
    "xxx-large": 200.0 / 100.0,
}

RELATIVE_SIZE_TABLE: dict[str, float] = {
    "larger": 1.25,
    "smaller": 0.75,
    "+4": 200.0 / 100.0,
    "+3": 175.0 / 100.0,
    "+2": 150.0 / 100.0,
    "+1": 125.0 / 100.0,
    "-1": 75.0 / 100.0,
    "-2": 50.0 / 100.0,
    "-3": 25.0 / 100.0,
}

MIN_FONT_SIZE: float = 1.0


@Memoized
def getSize(
    value: str | float | list | tuple,
    relative=0,
    base: int | None = None,
    default: float = 0.0,
) -> float:
    """
    Converts strings to standard sizes.
    That is the function taking a string of CSS size ('12pt', '1cm' and so on)
    and converts it into a float in a standard unit (in our case, points).

    >>> getSize('12pt')
    12.0
    >>> getSize('1cm')
    28.346456692913385
    """
    try:
        original = value
        if value is None:
            return relative
        if isinstance(value, float):
            return value
        if isinstance(value, int):
            return float(value)
        if isinstance(value, (tuple, list)):
            value = "".join(value)
        value = str(value).strip().lower().replace(",", ".")
        if value.endswith("cm"):
            return float(value[:-2].strip()) * cm
        if value.endswith("mm"):
            return float(value[:-2].strip()) * MM  # 1MM = 0.1cm
        if value.endswith("in"):
            return float(value[:-2].strip()) * inch  # 1pt == 1/72inch
        if value.endswith("pt"):
            return float(value[:-2].strip())
        if value.endswith("pc"):
            return float(value[:-2].strip()) * 12.0  # 1pc == 12pt
        if value.endswith("px"):
            # XXX W3C says, use 96pdi
            # http://www.w3.org/TR/CSS21/syndata.html#length-units
            return float(value[:-2].strip()) * DPI96
        if value in {"none", "0", "0.0", "auto"}:
            return 0.0
        if relative:
            if value.endswith("rem"):  # XXX
                # 1rem = 1 * fontSize
                return float(value[:-3].strip()) * relative
            if value.endswith("em"):  # XXX
                # 1em = 1 * fontSize
                return float(value[:-2].strip()) * relative
            if value.endswith("ex"):  # XXX
                # 1ex = 1/2 fontSize
                return float(value[:-2].strip()) * (relative / 2.0)
            if value.endswith("%"):
                # 1% = (fontSize * 1) / 100
                return (relative * float(value[:-1].strip())) / 100.0
            if value in {"normal", "inherit"}:
                return relative
            if value in RELATIVE_SIZE_TABLE:
                if base:
                    return max(MIN_FONT_SIZE, base * RELATIVE_SIZE_TABLE[value])
                return max(MIN_FONT_SIZE, relative * RELATIVE_SIZE_TABLE[value])
            if value in ABSOLUTE_SIZE_TABLE:
                if base:
                    return max(MIN_FONT_SIZE, base * ABSOLUTE_SIZE_TABLE[value])
                return max(MIN_FONT_SIZE, relative * ABSOLUTE_SIZE_TABLE[value])
            return max(MIN_FONT_SIZE, relative * float(value))
        try:
            value = float(value)
        except ValueError:
            log.warning("getSize: Not a float %r", value)
            return default  # value = 0
        return max(0, value)
    except Exception:
        log.warning("getSize %r %r", original, relative, exc_info=True)
        return default


@Memoized
def getCoords(x, y, w, h, pagesize):
    """
    As a stupid programmer I like to use the upper left
    corner of the document as the 0,0 coords therefore
    we need to do some fancy calculations.
    """
    # ~ print pagesize
    ax, ay = pagesize
    if x < 0:
        x = ax + x
    if y < 0:
        y = ay + y
    if w is not None and h is not None:
        if w <= 0:
            w = ax - x + w
        if h <= 0:
            h = ay - y + h
        return x, (ay - y - h), w, h
    return x, (ay - y)


@Memoized
def getBox(box, pagesize):
    """
    Parse sizes by corners in the form:
    <X-Left> <Y-Upper> <Width> <Height>
    The last to values with negative values are interpreted as offsets form
    the right and lower border.
    """
    box = str(box).split()
    if len(box) != 4:
        msg = "box not defined right way"
        raise RuntimeError(msg)
    x, y, w, h = (getSize(pos) for pos in box)
    return getCoords(x, y, w, h, pagesize)


def getFrameDimensions(
    data, page_width: float, page_height: float
) -> tuple[float, float, float, float]:
    """
    Calculate dimensions of a frame.

    Returns left, top, width and height of the frame in points.
    """
    box = data.get("-pdf-frame-box", [])
    if len(box) == 4:
        return (getSize(box[0]), getSize(box[1]), getSize(box[2]), getSize(box[3]))
    top = getSize(data.get("top", 0))
    left = getSize(data.get("left", 0))
    bottom = getSize(data.get("bottom", 0))
    right = getSize(data.get("right", 0))
    if "height" in data:
        height = getSize(data["height"])
        if "top" in data:
            top = getSize(data["top"])
            bottom = page_height - (top + height)
        elif "bottom" in data:
            bottom = getSize(data["bottom"])
            top = page_height - (bottom + height)
    if "width" in data:
        width = getSize(data["width"])
        if "left" in data:
            left = getSize(data["left"])
            right = page_width - (left + width)
        elif "right" in data:
            right = getSize(data["right"])
            left = page_width - (right + width)
    top += getSize(data.get("margin-top", 0))
    left += getSize(data.get("margin-left", 0))
    bottom += getSize(data.get("margin-bottom", 0))
    right += getSize(data.get("margin-right", 0))

    width = page_width - (left + right)
    height = page_height - (top + bottom)
    return left, top, width, height


@Memoized
def getPos(position, pagesize):
    """Pair of coordinates."""
    position = str(position).split()
    if len(position) != 2:
        msg = "position not defined right way"
        raise RuntimeError(msg)
    x, y = (getSize(pos) for pos in position)
    return getCoords(x, y, None, None, pagesize)


def getBool(s):
    """Is it a boolean?."""
    return str(s).lower() in {"y", "yes", "1", "true"}


def getFloat(s):
    with contextlib.suppress(Exception):
        return float(s)


ALIGNMENTS = {
    "left": TA_LEFT,
    "center": TA_CENTER,
    "middle": TA_CENTER,
    "right": TA_RIGHT,
    "justify": TA_JUSTIFY,
}


def getAlign(value, default=TA_LEFT):
    return ALIGNMENTS.get(str(value).lower(), default)


_rx_datauri = re.compile(
    "^data:(?P<mime>[a-z]+/[a-z]+);base64,(?P<data>.*)$", re.M | re.DOTALL
)

COLOR_BY_NAME = {
    "activeborder": Color(212, 208, 200),
    "activecaption": Color(10, 36, 106),
    "aliceblue": Color(0.941176, 0.972549, 1),
    "antiquewhite": Color(0.980392, 0.921569, 0.843137),
    "appworkspace": Color(128, 128, 128),
    "aqua": Color(0, 1, 1),
    "aquamarine": Color(0.498039, 1, 0.831373),
    "azure": Color(0.941176, 1, 1),
    "background": Color(58, 110, 165),
    "beige": Color(0.960784, 0.960784, 0.862745),
    "bisque": Color(1, 0.894118, 0.768627),
    "black": Color(0, 0, 0),
    "blanchedalmond": Color(1, 0.921569, 0.803922),
    "blue": Color(0, 0, 1),
    "blueviolet": Color(0.541176, 0.168627, 0.886275),
    "brown": Color(0.647059, 0.164706, 0.164706),
    "burlywood": Color(0.870588, 0.721569, 0.529412),
    "buttonface": Color(212, 208, 200),
    "buttonhighlight": Color(255, 255, 255),
    "buttonshadow": Color(128, 128, 128),
    "buttontext": Color(0, 0, 0),
    "cadetblue": Color(0.372549, 0.619608, 0.627451),
    "captiontext": Color(255, 255, 255),
    "chartreuse": Color(0.498039, 1, 0),
    "chocolate": Color(0.823529, 0.411765, 0.117647),
    "coral": Color(1, 0.498039, 0.313725),
    "cornflowerblue": Color(0.392157, 0.584314, 0.929412),
    "cornsilk": Color(1, 0.972549, 0.862745),
    "crimson": Color(0.862745, 0.078431, 0.235294),
    "cyan": Color(0, 1, 1),
    "darkblue": Color(0, 0, 0.545098),
    "darkcyan": Color(0, 0.545098, 0.545098),
    "darkgoldenrod": Color(0.721569, 0.52549, 0.043137),
    "darkgray": Color(0.662745, 0.662745, 0.662745),
    "darkgreen": Color(0, 0.392157, 0),
    "darkgrey": Color(0.662745, 0.662745, 0.662745),
    "darkkhaki": Color(0.741176, 0.717647, 0.419608),
    "darkmagenta": Color(0.545098, 0, 0.545098),
    "darkolivegreen": Color(0.333333, 0.419608, 0.184314),
    "darkorange": Color(1, 0.54902, 0),
    "darkorchid": Color(0.6, 0.196078, 0.8),
    "darkred": Color(0.545098, 0, 0),
    "darksalmon": Color(0.913725, 0.588235, 0.478431),
    "darkseagreen": Color(0.560784, 0.737255, 0.560784),
    "darkslateblue": Color(0.282353, 0.239216, 0.545098),
    "darkslategray": Color(0.184314, 0.309804, 0.309804),
    "darkslategrey": Color(0.184314, 0.309804, 0.309804),
    "darkturquoise": Color(0, 0.807843, 0.819608),
    "darkviolet": Color(0.580392, 0, 0.827451),
    "deeppink": Color(1, 0.078431, 0.576471),
    "deepskyblue": Color(0, 0.74902, 1),
    "dimgray": Color(0.411765, 0.411765, 0.411765),
    "dimgrey": Color(0.411765, 0.411765, 0.411765),
    "dodgerblue": Color(0.117647, 0.564706, 1),
    "firebrick": Color(0.698039, 0.133333, 0.133333),
    "floralwhite": Color(1, 0.980392, 0.941176),
    "forestgreen": Color(0.133333, 0.545098, 0.133333),
    "fuchsia": Color(1, 0, 1),
    "gainsboro": Color(0.862745, 0.862745, 0.862745),
    "ghostwhite": Color(0.972549, 0.972549, 1),
    "gold": Color(1, 0.843137, 0),
    "goldenrod": Color(0.854902, 0.647059, 0.12549),
    "gray": Color(0.501961, 0.501961, 0.501961),
    "graytext": Color(128, 128, 128),
    "green": Color(0, 0.501961, 0),
    "greenyellow": Color(0.678431, 1, 0.184314),
    "grey": Color(0.501961, 0.501961, 0.501961),
    "highlight": Color(10, 36, 106),
    "highlighttext": Color(255, 255, 255),
    "honeydew": Color(0.941176, 1, 0.941176),
    "hotpink": Color(1, 0.411765, 0.705882),
    "inactiveborder": Color(212, 208, 200),
    "inactivecaption": Color(128, 128, 128),
    "inactivecaptiontext": Color(212, 208, 200),
    "indianred": Color(0.803922, 0.360784, 0.360784),
    "indigo": Color(0.294118, 0, 0.509804),
    "infobackground": Color(255, 255, 225),
    "infotext": Color(0, 0, 0),
    "ivory": Color(1, 1, 0.941176),
    "khaki": Color(0.941176, 0.901961, 0.54902),
    "lavender": Color(0.901961, 0.901961, 0.980392),
    "lavenderblush": Color(1, 0.941176, 0.960784),
    "lawngreen": Color(0.486275, 0.988235, 0),
    "lemonchiffon": Color(1, 0.980392, 0.803922),
    "lightblue": Color(0.678431, 0.847059, 0.901961),
    "lightcoral": Color(0.941176, 0.501961, 0.501961),
    "lightcyan": Color(0.878431, 1, 1),
    "lightgoldenrodyellow": Color(0.980392, 0.980392, 0.823529),
    "lightgray": Color(0.827451, 0.827451, 0.827451),
    "lightgreen": Color(0.564706, 0.933333, 0.564706),
    "lightgrey": Color(0.827451, 0.827451, 0.827451),
    "lightpink": Color(1, 0.713725, 0.756863),
    "lightsalmon": Color(1, 0.627451, 0.478431),
    "lightseagreen": Color(0.12549, 0.698039, 0.666667),
    "lightskyblue": Color(0.529412, 0.807843, 0.980392),
    "lightslategray": Color(0.466667, 0.533333, 0.6),
    "lightslategrey": Color(0.466667, 0.533333, 0.6),
    "lightsteelblue": Color(0.690196, 0.768627, 0.870588),
    "lightyellow": Color(1, 1, 0.878431),
    "lime": Color(0, 1, 0),
    "limegreen": Color(0.196078, 0.803922, 0.196078),
    "linen": Color(0.980392, 0.941176, 0.901961),
    "magenta": Color(1, 0, 1),
    "maroon": Color(0.501961, 0, 0),
    "mediumaquamarine": Color(0.4, 0.803922, 0.666667),
    "mediumblue": Color(0, 0, 0.803922),
    "mediumorchid": Color(0.729412, 0.333333, 0.827451),
    "mediumpurple": Color(0.576471, 0.439216, 0.858824),
    "mediumseagreen": Color(0.235294, 0.701961, 0.443137),
    "mediumslateblue": Color(0.482353, 0.407843, 0.933333),
    "mediumspringgreen": Color(0, 0.980392, 0.603922),
    "mediumturquoise": Color(0.282353, 0.819608, 0.8),
    "mediumvioletred": Color(0.780392, 0.082353, 0.521569),
    "menu": Color(212, 208, 200),
    "menutext": Color(0, 0, 0),
    "midnightblue": Color(0.098039, 0.098039, 0.439216),
    "mintcream": Color(0.960784, 1, 0.980392),
    "mistyrose": Color(1, 0.894118, 0.882353),
    "moccasin": Color(1, 0.894118, 0.709804),
    "navajowhite": Color(1, 0.870588, 0.678431),
    "navy": Color(0, 0, 0.501961),
    "oldlace": Color(0.992157, 0.960784, 0.901961),
    "olive": Color(0.501961, 0.501961, 0),
    "olivedrab": Color(0.419608, 0.556863, 0.137255),
    "orange": Color(1, 0.647059, 0),
    "orangered": Color(1, 0.270588, 0),
    "orchid": Color(0.854902, 0.439216, 0.839216),
    "palegoldenrod": Color(0.933333, 0.909804, 0.666667),
    "palegreen": Color(0.596078, 0.984314, 0.596078),
    "paleturquoise": Color(0.686275, 0.933333, 0.933333),
    "palevioletred": Color(0.858824, 0.439216, 0.576471),
    "papayawhip": Color(1, 0.937255, 0.835294),
    "peachpuff": Color(1, 0.854902, 0.72549),
    "peru": Color(0.803922, 0.521569, 0.247059),
    "pink": Color(1, 0.752941, 0.796078),
    "plum": Color(0.866667, 0.627451, 0.866667),
    "powderblue": Color(0.690196, 0.878431, 0.901961),
    "purple": Color(0.501961, 0, 0.501961),
    "red": Color(1, 0, 0),
    "rosybrown": Color(0.737255, 0.560784, 0.560784),
    "royalblue": Color(0.254902, 0.411765, 0.882353),
    "saddlebrown": Color(0.545098, 0.270588, 0.07451),
    "salmon": Color(0.980392, 0.501961, 0.447059),
    "sandybrown": Color(0.956863, 0.643137, 0.376471),
    "scrollbar": Color(212, 208, 200),
    "seagreen": Color(0.180392, 0.545098, 0.341176),
    "seashell": Color(1, 0.960784, 0.933333),
    "sienna": Color(0.627451, 0.321569, 0.176471),
    "silver": Color(0.752941, 0.752941, 0.752941),
    "skyblue": Color(0.529412, 0.807843, 0.921569),
    "slateblue": Color(0.415686, 0.352941, 0.803922),
    "slategray": Color(0.439216, 0.501961, 0.564706),
    "slategrey": Color(0.439216, 0.501961, 0.564706),
    "snow": Color(1, 0.980392, 0.980392),
    "springgreen": Color(0, 1, 0.498039),
    "steelblue": Color(0.27451, 0.509804, 0.705882),
    "tan": Color(0.823529, 0.705882, 0.54902),
    "teal": Color(0, 0.501961, 0.501961),
    "thistle": Color(0.847059, 0.74902, 0.847059),
    "threeddarkshadow": Color(64, 64, 64),
    "threedface": Color(212, 208, 200),
    "threedhighlight": Color(255, 255, 255),
    "threedlightshadow": Color(212, 208, 200),
    "threedshadow": Color(128, 128, 128),
    "tomato": Color(1, 0.388235, 0.278431),
    "turquoise": Color(0.25098, 0.878431, 0.815686),
    "violet": Color(0.933333, 0.509804, 0.933333),
    "wheat": Color(0.960784, 0.870588, 0.701961),
    "white": Color(1, 1, 1),
    "whitesmoke": Color(0.960784, 0.960784, 0.960784),
    "window": Color(255, 255, 255),
    "windowframe": Color(0, 0, 0),
    "windowtext": Color(0, 0, 0),
    "yellow": Color(1, 1, 0),
    "yellowgreen": Color(0.603922, 0.803922, 0.196078),
}


def get_default_asian_font():
    lower_font_list = []
    upper_font_list = []

    font_dict = copy(reportlab.pdfbase._cidfontdata.defaultUnicodeEncodings)
    fonts = font_dict.keys()

    for font in fonts:
        upper_font_list.append(font)
        lower_font_list.append(font.lower())
    return {lower_font_list[i]: upper_font_list[i] for i in range(len(lower_font_list))}


def set_asian_fonts(fontname):
    font_dict = copy(reportlab.pdfbase._cidfontdata.defaultUnicodeEncodings)
    fonts = font_dict.keys()
    if fontname in fonts:
        pdfmetrics.registerFont(UnicodeCIDFont(fontname))


def detect_language(name):
    asian_language_list = xhtml2pdf.default.DEFAULT_LANGUAGE_LIST
    if name in asian_language_list:
        return name
    return None


def arabic_format(text, language):
    # Note: right now all of the languages are treated the same way.
    # But maybe in the future we have to for example implement something
    # for "hebrew" that isn't used in "arabic"
    if detect_language(language) in {
        "arabic",
        "hebrew",
        "persian",
        "urdu",
        "pashto",
        "sindhi",
    }:
        ar = arabic_reshaper.reshape(text)
        return get_display(ar)
    return None


def frag_text_language_check(context, frag_text):
    if hasattr(context, "language"):
        language = context.__getattribute__("language")
        detect_language_result = arabic_format(frag_text, language)
        if detect_language_result:
            return detect_language_result
        return None
    return None


class ImageWarning(Exception):  # noqa: N818
    pass
