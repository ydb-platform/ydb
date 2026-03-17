# encoding: utf-8

"""
DrawingML objects related to color, ColorFormat being the most prominent.
"""

from __future__ import absolute_import, print_function, unicode_literals

from ..enum.dml import MSO_COLOR_TYPE, MSO_THEME_COLOR
from ..oxml.dml.color import (
    CT_HslColor,
    CT_PresetColor,
    CT_SchemeColor,
    CT_ScRgbColor,
    CT_SRgbColor,
    CT_SystemColor,
)


class ColorFormat(object):
    """
    Provides access to color settings such as RGB color, theme color, and
    luminance adjustments.
    """

    def __init__(self, eg_colorChoice_parent, color):
        super(ColorFormat, self).__init__()
        self._xFill = eg_colorChoice_parent
        self._color = color

    @property
    def brightness(self):
        """
        Read/write float value between -1.0 and 1.0 indicating the brightness
        adjustment for this color, e.g. -0.25 is 25% darker and 0.4 is 40%
        lighter. 0 means no brightness adjustment.
        """
        return self._color.brightness

    @brightness.setter
    def brightness(self, value):
        self._validate_brightness_value(value)
        self._color.brightness = value

    @classmethod
    def from_colorchoice_parent(cls, eg_colorChoice_parent):
        xClr = eg_colorChoice_parent.eg_colorChoice
        color = _Color(xClr)
        color_format = cls(eg_colorChoice_parent, color)
        return color_format

    @property
    def rgb(self):
        """
        |RGBColor| value of this color, or None if no RGB color is explicitly
        defined for this font. Setting this value to an |RGBColor| instance
        causes its type to change to MSO_COLOR_TYPE.RGB. If the color was a
        theme color with a brightness adjustment, the brightness adjustment
        is removed when changing it to an RGB color.
        """
        return self._color.rgb

    @rgb.setter
    def rgb(self, rgb):
        if not isinstance(rgb, RGBColor):
            raise ValueError("assigned value must be type RGBColor")
        # change to rgb color format if not already
        if not isinstance(self._color, _SRgbColor):
            srgbClr = self._xFill.get_or_change_to_srgbClr()
            self._color = _SRgbColor(srgbClr)
        # call _SRgbColor instance to do the setting
        self._color.rgb = rgb

    @property
    def theme_color(self):
        """Theme color value of this color.

        Value is a member of :ref:`MsoThemeColorIndex`, e.g.
        ``MSO_THEME_COLOR.ACCENT_1``. Raises AttributeError on access if the
        color is not type ``MSO_COLOR_TYPE.SCHEME``. Assigning a member of
        :ref:`MsoThemeColorIndex` causes the color's type to change to
        ``MSO_COLOR_TYPE.SCHEME``.
        """
        return self._color.theme_color

    @theme_color.setter
    def theme_color(self, mso_theme_color_idx):
        # change to theme color format if not already
        if not isinstance(self._color, _SchemeColor):
            schemeClr = self._xFill.get_or_change_to_schemeClr()
            self._color = _SchemeColor(schemeClr)
        self._color.theme_color = mso_theme_color_idx

    @property
    def type(self):
        """
        Read-only. A value from :ref:`MsoColorType`, either RGB or SCHEME,
        corresponding to the way this color is defined, or None if no color
        is defined at the level of this font.
        """
        return self._color.color_type

    def _validate_brightness_value(self, value):
        if value < -1.0 or value > 1.0:
            raise ValueError("brightness must be number in range -1.0 to 1.0")
        if isinstance(self._color, _NoneColor):
            msg = (
                "can't set brightness when color.type is None. Set color.rgb"
                " or .theme_color first."
            )
            raise ValueError(msg)


class _Color(object):
    """
    Object factory for color object of the appropriate type, also the base
    class for all color type classes such as SRgbColor.
    """

    def __new__(cls, xClr):
        color_cls = {
            type(None): _NoneColor,
            CT_HslColor: _HslColor,
            CT_PresetColor: _PrstColor,
            CT_SchemeColor: _SchemeColor,
            CT_ScRgbColor: _ScRgbColor,
            CT_SRgbColor: _SRgbColor,
            CT_SystemColor: _SysColor,
        }[type(xClr)]
        return super(_Color, cls).__new__(color_cls)

    def __init__(self, xClr):
        super(_Color, self).__init__()
        self._xClr = xClr

    @property
    def brightness(self):
        lumMod, lumOff = self._xClr.lumMod, self._xClr.lumOff
        # a tint is lighter, a shade is darker
        # only tints have lumOff child
        if lumOff is not None:
            brightness = lumOff.val
            return brightness
        # which leaves shades, if lumMod is present
        if lumMod is not None:
            brightness = lumMod.val - 1.0
            return brightness
        # there's no brightness adjustment if no lum{Mod|Off} elements
        return 0

    @brightness.setter
    def brightness(self, value):
        if value > 0:
            self._tint(value)
        elif value < 0:
            self._shade(value)
        else:
            self._xClr.clear_lum()

    @property
    def color_type(self):  # pragma: no cover
        tmpl = ".color_type property must be implemented on %s"
        raise NotImplementedError(tmpl % self.__class__.__name__)

    @property
    def rgb(self):
        """
        Raises TypeError on access unless overridden by subclass.
        """
        tmpl = "no .rgb property on color type '%s'"
        raise AttributeError(tmpl % self.__class__.__name__)

    @property
    def theme_color(self):
        """
        Raises TypeError on access unless overridden by subclass.
        """
        return MSO_THEME_COLOR.NOT_THEME_COLOR

    def _shade(self, value):
        lumMod_val = 1.0 - abs(value)
        color_elm = self._xClr.clear_lum()
        color_elm.add_lumMod(lumMod_val)

    def _tint(self, value):
        lumOff_val = value
        lumMod_val = 1.0 - lumOff_val
        color_elm = self._xClr.clear_lum()
        color_elm.add_lumMod(lumMod_val)
        color_elm.add_lumOff(lumOff_val)


class _HslColor(_Color):
    @property
    def color_type(self):
        return MSO_COLOR_TYPE.HSL


class _NoneColor(_Color):
    @property
    def color_type(self):
        return None

    @property
    def theme_color(self):
        """
        Raise TypeError on attempt to access .theme_color when no color
        choice is present.
        """
        tmpl = "no .theme_color property on color type '%s'"
        raise AttributeError(tmpl % self.__class__.__name__)


class _PrstColor(_Color):
    @property
    def color_type(self):
        return MSO_COLOR_TYPE.PRESET


class _SchemeColor(_Color):
    def __init__(self, schemeClr):
        super(_SchemeColor, self).__init__(schemeClr)
        self._schemeClr = schemeClr

    @property
    def color_type(self):
        return MSO_COLOR_TYPE.SCHEME

    @property
    def theme_color(self):
        """
        Theme color value of this color, one of those defined in the
        MSO_THEME_COLOR enumeration, e.g. MSO_THEME_COLOR.ACCENT_1. None if
        no theme color is explicitly defined for this font. Setting this to a
        value in MSO_THEME_COLOR causes the color's type to change to
        ``MSO_COLOR_TYPE.SCHEME``.
        """
        return self._schemeClr.val

    @theme_color.setter
    def theme_color(self, mso_theme_color_idx):
        self._schemeClr.val = mso_theme_color_idx


class _ScRgbColor(_Color):
    @property
    def color_type(self):
        return MSO_COLOR_TYPE.SCRGB


class _SRgbColor(_Color):
    def __init__(self, srgbClr):
        super(_SRgbColor, self).__init__(srgbClr)
        self._srgbClr = srgbClr

    @property
    def color_type(self):
        return MSO_COLOR_TYPE.RGB

    @property
    def rgb(self):
        """
        |RGBColor| value of this color, corresponding to the value in the
        required ``val`` attribute of the ``<a:srgbColr>`` element.
        """
        return RGBColor.from_string(self._srgbClr.val)

    @rgb.setter
    def rgb(self, rgb):
        self._srgbClr.val = str(rgb)


class _SysColor(_Color):
    @property
    def color_type(self):
        return MSO_COLOR_TYPE.SYSTEM


class RGBColor(tuple):
    """
    Immutable value object defining a particular RGB color.
    """

    def __new__(cls, r, g, b):
        msg = "RGBColor() takes three integer values 0-255"
        for val in (r, g, b):
            if not isinstance(val, int) or val < 0 or val > 255:
                raise ValueError(msg)
        return super(RGBColor, cls).__new__(cls, (r, g, b))

    def __str__(self):
        """
        Return a hex string rgb value, like '3C2F80'
        """
        return "%02X%02X%02X" % self

    @classmethod
    def from_string(cls, rgb_hex_str):
        """
        Return a new instance from an RGB color hex string like ``'3C2F80'``.
        """
        r = int(rgb_hex_str[:2], 16)
        g = int(rgb_hex_str[2:4], 16)
        b = int(rgb_hex_str[4:], 16)
        return cls(r, g, b)
