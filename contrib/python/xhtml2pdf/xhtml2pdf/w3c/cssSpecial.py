"""
Helper for complex CSS definitions like font, margin, padding and border
Optimized for use with PISA.

Copyright 2010 Dirk Holtwick, holtwick.it

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

# ruff: noqa: N802, N803, N815, N816, N999

import logging

from xhtml2pdf.util import toList

log = logging.getLogger(__name__)


_styleTable = {"normal": "", "italic": "", "oblique": ""}

_variantTable = {"normal": None, "small-caps": None}

_weightTable = {
    "light": 300,
    "lighter": 300,  # fake relativness for now
    "normal": 400,
    "bold": 700,
    "bolder": 700,  # fake relativness for now
    "100": 100,
    "200": 200,
    "300": 300,
    "400": 400,
    "500": 500,
    "600": 600,
    "700": 700,
    "800": 800,
    "900": 900,
    # wx.LIGHT: 300,
    # wx.NORMAL: 400,
    # wx.BOLD: 700,
}

# _absSizeTable = {
#    "xx-small" : 3./5.,
#    "x-small": 3./4.,
#    "small": 8./9.,
#    "medium": 1./1.,
#    "large": 6./5.,
#    "x-large": 3./2.,
#    "xx-large": 2./1.,
#    "xxx-large": 3./1.,
#    "larger": 1.25,      # XXX Not totally CSS conform:
#    "smaller": 0.75,     # http://www.w3.org/TR/CSS21/fonts.html#propdef-font-size
#    }

_borderStyleTable = {
    "none": 0,
    "hidden": 0,
    "dotted": 1,
    "dashed": 1,
    "solid": 1,
    "double": 1,
    "groove": 1,
    "ridge": 1,
    "inset": 1,
    "outset": 1,
}


def getNextPart(parts):
    return parts.pop(0) if parts else None


def isSize(value):
    return value and (isinstance(value, tuple) or value == "0")


def splitBorder(parts):
    """
    The order of the elements seems to be of no importance:

    http://www.w3.org/TR/CSS21/box.html#border-shorthand-properties
    """
    width = style = color = None

    if len(parts) > 3:
        log.warning("To many elements for border style %r", parts)

    for part in parts:
        # Width
        if isSize(part):
            width = part

        # Style
        elif hasattr(part, "lower") and part.lower() in _borderStyleTable:
            style = part

        # Color
        else:
            color = part

    # log.debug("Border styles: %r -> %r ", copy_parts, (width, style, color))

    return (width, style, color)


def parseSpecialRules(declarations, debug=0):
    # print selectors, declarations
    # CSS MODIFY!
    dd = []

    for d in declarations:
        if debug:
            log.debug("CSS special  IN: %r", d)

        name, parts, last = d
        oparts = parts
        parts = toList(parts, cast_tuple=False)

        # FONT
        if name == "font":
            # [ [ <'font-style'> || <'font-variant'> || <'font-weight'> ]? <'font-size'> [ / <'line-height'> ]? <'font-family'> ] | inherit
            part = getNextPart(parts)
            # Style
            if part and part in _styleTable:
                dd.append(("font-style", part, last))
                part = getNextPart(parts)
                # Variant
            if part and part in _variantTable:
                dd.append(("font-variant", part, last))
                part = getNextPart(parts)
                # Weight
            if part and part in _weightTable:
                dd.append(("font-weight", part, last))
                part = getNextPart(parts)
                # Size and Line Height
            if isinstance(part, tuple) and len(part) == 3:
                fontSize, slash, lineHeight = part
                assert slash == "/"
                dd.extend(
                    (("font-size", fontSize, last), ("line-height", lineHeight, last))
                )
            else:
                dd.append(("font-size", part, last))
                # Face/ Family
            dd.append(("font-face", parts, last))

        # BACKGROUND
        elif name == "background":
            # [<'background-color'> || <'background-image'> || <'background-repeat'> || <'background-attachment'> || <'background-position'>] | inherit

            # XXX We do not receive url() and parts list, so we go for a dirty work around
            part = getNextPart(parts) or oparts
            if part:
                if isinstance(part, str) and (("." in part) or ("data:" in part)):
                    dd.append(("background-image", part, last))
                else:
                    dd.append(("background-color", part, last))

            if 0:
                part = getNextPart(parts) or oparts
                print("~", part, parts, oparts, declarations)
                # Color
                if part and (not part.startswith("url")):
                    dd.append(("background-color", part, last))
                    part = getNextPart(parts)
                    # Background
                if part:
                    dd.append(("background-image", part, last))
                    # XXX Incomplete! Error in url()!

        # TODO: We should definitely outsource the "if len() ==" part into a separate function!
        # Because we're repeating the same if-elif-else statement for MARGIN, PADDING,
        # BORDER-WIDTH, BORDER-COLOR and BORDER-STYLE. That's pretty messy. (fbernhart)
        # MARGIN
        elif name == "margin":
            if len(parts) == 1:
                top = bottom = left = right = parts[0]
            elif len(parts) == 2:
                top = bottom = parts[0]
                left = right = parts[1]
            elif len(parts) == 3:
                top = parts[0]
                left = right = parts[1]
                bottom = parts[2]
            elif len(parts) == 4:
                top = parts[0]
                right = parts[1]
                bottom = parts[2]
                left = parts[3]
            else:
                continue
            dd.extend(
                (
                    ("margin-left", left, last),
                    ("margin-right", right, last),
                    ("margin-top", top, last),
                    ("margin-bottom", bottom, last),
                )
            )

        # PADDING
        elif name == "padding":
            if len(parts) == 1:
                top = bottom = left = right = parts[0]
            elif len(parts) == 2:
                top = bottom = parts[0]
                left = right = parts[1]
            elif len(parts) == 3:
                top = parts[0]
                left = right = parts[1]
                bottom = parts[2]
            elif len(parts) == 4:
                top = parts[0]
                right = parts[1]
                bottom = parts[2]
                left = parts[3]
            else:
                continue
            dd.extend(
                (
                    ("padding-left", left, last),
                    ("padding-right", right, last),
                    ("padding-top", top, last),
                    ("padding-bottom", bottom, last),
                )
            )

        # BORDER WIDTH
        elif name == "border-width":
            if len(parts) == 1:
                top = bottom = left = right = parts[0]
            elif len(parts) == 2:
                top = bottom = parts[0]
                left = right = parts[1]
            elif len(parts) == 3:
                top = parts[0]
                left = right = parts[1]
                bottom = parts[2]
            elif len(parts) == 4:
                top = parts[0]
                right = parts[1]
                bottom = parts[2]
                left = parts[3]
            else:
                continue
            dd.extend(
                (
                    ("border-left-width", left, last),
                    ("border-right-width", right, last),
                    ("border-top-width", top, last),
                    ("border-bottom-width", bottom, last),
                )
            )

        # BORDER COLOR
        elif name == "border-color":
            if len(parts) == 1:
                top = bottom = left = right = parts[0]
            elif len(parts) == 2:
                top = bottom = parts[0]
                left = right = parts[1]
            elif len(parts) == 3:
                top = parts[0]
                left = right = parts[1]
                bottom = parts[2]
            elif len(parts) == 4:
                top = parts[0]
                right = parts[1]
                bottom = parts[2]
                left = parts[3]
            else:
                continue
            dd.extend(
                (
                    ("border-left-color", left, last),
                    ("border-right-color", right, last),
                    ("border-top-color", top, last),
                    ("border-bottom-color", bottom, last),
                )
            )

        # BORDER STYLE
        elif name == "border-style":
            if len(parts) == 1:
                top = bottom = left = right = parts[0]
            elif len(parts) == 2:
                top = bottom = parts[0]
                left = right = parts[1]
            elif len(parts) == 3:
                top = parts[0]
                left = right = parts[1]
                bottom = parts[2]
            elif len(parts) == 4:
                top = parts[0]
                right = parts[1]
                bottom = parts[2]
                left = parts[3]
            else:
                continue
            dd.extend(
                (
                    ("border-left-style", left, last),
                    ("border-right-style", right, last),
                    ("border-top-style", top, last),
                    ("border-bottom-style", bottom, last),
                )
            )

        # BORDER
        elif name == "border":
            width, style, color = splitBorder(parts)
            if width is not None:
                dd.extend(
                    (
                        ("border-left-width", width, last),
                        ("border-right-width", width, last),
                        ("border-top-width", width, last),
                        ("border-bottom-width", width, last),
                    )
                )
            if style is not None:
                dd.extend(
                    (
                        ("border-left-style", style, last),
                        ("border-right-style", style, last),
                        ("border-top-style", style, last),
                        ("border-bottom-style", style, last),
                    )
                )
            if color is not None:
                dd.extend(
                    (
                        ("border-left-color", color, last),
                        ("border-right-color", color, last),
                        ("border-top-color", color, last),
                        ("border-bottom-color", color, last),
                    )
                )

        # BORDER TOP, BOTTOM, LEFT, RIGHT
        elif name in {"border-top", "border-bottom", "border-left", "border-right"}:
            direction = name[7:]
            width, style, color = splitBorder(parts)
            # print direction, width
            if width is not None:
                dd.append(("border-" + direction + "-width", width, last))
            if style is not None:
                dd.append(("border-" + direction + "-style", style, last))
            if color is not None:
                dd.append(("border-" + direction + "-color", color, last))

        # REST
        else:
            dd.append(d)

    if debug and dd:
        log.debug("CSS special OUT:\n%s", "\n".join([repr(d) for d in dd]))

    if 0:  # declarations!=dd:
        print("###", declarations)
        print("#->", dd)
        # CSS MODIFY! END
    return dd


# import re
# _rxhttp = re.compile(r"url\([\'\"]?http\:\/\/[^\/]", re.IGNORECASE|re.DOTALL)


def cleanupCSS(src):
    # src = _rxhttp.sub('url(', src)
    return src
