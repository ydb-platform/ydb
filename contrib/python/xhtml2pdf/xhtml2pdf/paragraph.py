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

"""
A paragraph class to be used with ReportLab Platypus.

Todo:
----
- Bullets
- Weblinks and internal links
- Borders and margins (Box)
- Underline, Background, Strike
- Images
- Hyphenation
+ Alignment
+ Breakline, empty lines
+ TextIndent
- Sub and super

"""
from __future__ import annotations

import copy
import logging
import re
from typing import TYPE_CHECKING, Any, ClassVar

from reportlab.lib.colors import Color
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus.flowables import Flowable

if TYPE_CHECKING:
    from reportlab.pdfgen.canvas import Canvas

logger = logging.getLogger(__name__)


class Style(dict):
    """
    Style.

    Single place for style definitions: Paragraphs and Fragments. The
    naming follows the convention of CSS written in camelCase letters.
    """

    DEFAULT: ClassVar[dict[str, Any]] = {
        "color": Color(0, 0, 0),
        "fontName": "Times-Roman",
        "fontSize": 10.0,
        "height": None,
        "lineHeight": 1.5,
        "lineHeightAbsolute": None,
        "link": None,
        "pdfLineSpacing": 0,
        "textAlign": TA_LEFT,
        "textIndent": 0.0,
        "width": None,
    }

    def __init__(self, **kwargs) -> None:
        self.update(self.DEFAULT)
        self.update(kwargs)
        self.spaceBefore: int = 0
        self.spaceAfter: int = 0
        self.leftIndent: int = 0
        self.keepWithNext: bool = False


class Box(dict):
    """
    Box.

    Handles the following styles:

        backgroundColor, backgroundImage
        paddingLeft, paddingRight, paddingTop, paddingBottom
        marginLeft, marginRight, marginTop, marginBottom
        borderLeftColor, borderLeftWidth, borderLeftStyle
        borderRightColor, borderRightWidth, borderRightStyle
        borderTopColor, borderTopWidth, borderTopStyle
        borderBottomColor, borderBottomWidth, borderBottomStyle

    Not used in inline Elements:

        paddingTop, paddingBottom
        marginTop, marginBottom

    """

    name: str = "box"

    def drawBox(self, canvas: Canvas, x: int, y: int, w: int, h: int):
        canvas.saveState()

        # Background
        bg = self.get("backgroundColor", None)
        if bg is not None:
            # draw a filled rectangle (with no stroke) using bg color
            canvas.setFillColor(bg)
            canvas.rect(x, y, w, h, fill=1, stroke=0)

        # Borders
        def _drawBorderLine(bstyle, width, color, x1, y1, x2, y2):
            # We need width and border style to be able to draw a border
            if width and bstyle:
                # If no color for border is given, the text color is used (like defined by W3C)
                if color is None:
                    color = self.get("textColor", Color(0, 0, 0))
                if color is not None:
                    canvas.setStrokeColor(color)
                    canvas.setLineWidth(width)
                    canvas.line(x1, y1, x2, y2)

        _drawBorderLine(
            self.get("borderLeftStyle", None),
            self.get("borderLeftWidth", None),
            self.get("borderLeftColor", None),
            x,
            y,
            x,
            y + h,
        )
        _drawBorderLine(
            self.get("borderRightStyle", None),
            self.get("borderRightWidth", None),
            self.get("borderRightColor", None),
            x + w,
            y,
            x + w,
            y + h,
        )
        _drawBorderLine(
            self.get("borderTopStyle", None),
            self.get("borderTopWidth", None),
            self.get("borderTopColor", None),
            x,
            y + h,
            x + w,
            y + h,
        )
        _drawBorderLine(
            self.get("borderBottomStyle", None),
            self.get("borderBottomWidth", None),
            self.get("borderBottomColor", None),
            x,
            y,
            x + w,
            y,
        )

        canvas.restoreState()


class Fragment(Box):
    """
    Fragment.

    text:       String containing text
    fontName:
    fontSize:
    width:      Width of string
    height:     Height of string
    """

    name: str = "fragment"
    isSoft: bool = False
    isText: bool = False
    isLF: bool = False

    def calc(self) -> None:
        self["width"] = 0


class Word(Fragment):
    """A single word."""

    name: str = "word"
    isText: bool = True

    def calc(self) -> None:
        """XXX Cache stringWith if not accelerated?!."""
        self["width"] = stringWidth(self["text"], self["fontName"], self["fontSize"])


class Space(Fragment):
    """A space between fragments that is the usual place for line breaking."""

    name: str = "space"
    isSoft: bool = True

    def calc(self) -> None:
        self["width"] = stringWidth(" ", self["fontName"], self["fontSize"])


class LineBreak(Fragment):
    """Line break."""

    name: str = "br"
    isSoft: bool = True
    isLF: bool = True


class BoxBegin(Fragment):
    name: str = "begin"

    def calc(self) -> None:
        self["width"] = self.get("marginLeft", 0) + self.get(
            "paddingLeft", 0
        )  # + border if border

    def draw(self, canvas, y):
        # if not self["length"]:
        x = self.get("marginLeft", 0) + self["x"]
        w = self["length"] + self.get("paddingRight", 0)
        h = self["fontSize"]
        self.drawBox(canvas, x, y, w, h)


class BoxEnd(Fragment):
    name: str = "end"

    def calc(self) -> None:
        self["width"] = self.get("marginRight", 0) + self.get(
            "paddingRight", 0
        )  # + border


class Image(Fragment):
    name: str = "image"


class Line(list):
    """Container for line fragments."""

    LINEHEIGHT: float = 1.0

    def __init__(self, style) -> None:
        self.width: int = 0
        self.height: int = 0
        self.isLast: bool = False
        self.style = style
        self.boxStack: list = []
        super().__init__()

    def doAlignment(self, width, alignment):
        # Apply alignment
        if alignment != TA_LEFT:
            lineWidth = self[-1]["x"] + self[-1]["width"]
            emptySpace = width - lineWidth
            if alignment == TA_RIGHT:
                for frag in self:
                    frag["x"] += emptySpace
            elif alignment == TA_CENTER:
                for frag in self:
                    frag["x"] += emptySpace / 2.0
            elif (
                alignment == TA_JUSTIFY and not self.isLast
            ):  # XXX last line before split
                delta = emptySpace / (len(self) - 1)
                for i, frag in enumerate(self):
                    frag["x"] += i * delta

        # Boxes
        for frag in self:
            x = frag["x"] + frag["width"]
            if isinstance(frag, BoxBegin):
                self.boxStack.append(frag)
            elif isinstance(frag, BoxEnd) and self.boxStack:
                frag = self.boxStack.pop()
                frag["length"] = x - frag["x"]

        # Handle the rest
        for frag in self.boxStack:
            frag["length"] = x - frag["x"]

    def doLayout(self, width):
        """Align words in previous line."""
        # Calculate dimensions
        self.width = width

        font_sizes = [0] + [frag.get("fontSize", 0) for frag in self]
        self.fontSize = max(font_sizes)
        self.height = self.lineHeight = max(
            frag * self.LINEHEIGHT for frag in font_sizes
        )

        # Apply line height
        y = self.lineHeight - self.fontSize  # / 2
        for frag in self:
            frag["y"] = y

        return self.height

    def dumpFragments(self):
        logger.debug("Line")
        logger.debug(40 * "-")
        for frag in self:
            logger.debug("%s", frag.get("text", frag.name.upper()))


class Text(list):
    """
    Container for text fragments.

    Helper functions for splitting text into lines and calculating sizes
    and positions.
    """

    def __init__(self, data: list | None = None, style: Style | None = None) -> None:
        # Mutable arguments are a shit idea
        if data is None:
            data = []

        self.lines: list = []
        self.width: int = 0
        self.height: int = 0
        self.maxWidth: int = 0
        self.maxHeight: int = 0
        self.style: Style | None = style
        super().__init__(data)

    def calc(self) -> None:
        """Calculate sizes of fragments."""
        for word in self:
            word.calc()

    def splitIntoLines(
        self, maxWidth: int, maxHeight: int, *, splitted: bool = False
    ) -> int | None:
        """
        Split text into lines and calculate X positions. If we need more
        space in height than available we return the rest of the text.
        """
        self.lines = []
        self.height = 0
        self.width = maxWidth
        self.maxHeight = maxHeight
        self.maxWidth = maxWidth
        boxStack: list = []

        style = self.style
        x: int = 0

        # Start with indent in first line of text
        if not splitted and style:
            x = style["textIndent"]

        lenText: int = len(self)
        pos: int = 0
        while pos < lenText:
            # Reset values for new line
            posBegin = pos
            line = Line(style)

            # Update boxes for next line
            for box in copy.copy(boxStack):
                box["x"] = 0
                line.append(BoxBegin(box))

            while pos < lenText:
                # Get fragment, its width and set X
                frag = self[pos]
                fragWidth = frag["width"]
                frag["x"] = x
                pos += 1

                # Keep in mind boxes for next lines
                if isinstance(frag, BoxBegin):
                    boxStack.append(frag)
                elif isinstance(frag, BoxEnd):
                    boxStack.pop()

                # If space or linebreak handle special way
                if frag.isSoft:
                    if frag.isLF:
                        line.append(frag)
                        break
                        # First element of line should not be a space
                    if x == 0:
                        continue
                        # Keep in mind last possible line break

                # The elements exceed the current line
                elif fragWidth + x > maxWidth:
                    break

                # Add fragment to line and update x
                x += fragWidth
                line.append(frag)

            # Remove trailing white spaces
            while line and line[-1].name in {"space", "br"}:
                line.pop()

            # Add line to list
            line.dumpFragments()
            # if line:
            self.height += line.doLayout(self.width)
            self.lines.append(line)

            # If not enough space for current line force to split
            if self.height > maxHeight:
                return posBegin

            # Reset variables
            x = 0

        # Apply alignment
        self.lines[-1].isLast = True
        if style:
            for line in self.lines:
                line.doAlignment(maxWidth, style["textAlign"])

        return None

    def dumpLines(self):
        """For debugging dump all line and their content."""
        for i, line in enumerate(self.lines):
            logger.debug("Line %d:", i)
            logger.debug(line.dumpFragments())

    def __getitem__(self, key):
        """Make sure slices return also Text object and not lists"""
        if isinstance(key, slice):
            return type(self)(super().__getitem__(key))
        return super().__getitem__(key)


class Paragraph(Flowable):
    """
    A simple Paragraph class respecting alignment.

    Does text without tags.

    Respects only the following global style attributes:
    fontName, fontSize, leading, firstLineIndent, leftIndent,
    rightIndent, textColor, alignment.
    (spaceBefore, spaceAfter are handled by the Platypus framework.)

    """

    def __init__(
        self,
        text: Text,
        style: Style,
        *,
        debug: bool = False,
        splitted: bool = False,
        **kwDict,
    ) -> None:
        super().__init__()

        self.text: Text = text
        self.text.calc()
        self.style: Style = style
        self.text.style = style

        self.debug: bool = debug
        self.splitted: bool = splitted

        # More attributes
        for k, v in kwDict.items():
            setattr(self, k, v)

        # set later...
        self.splitIndex: int | None = None

    # overwritten methods from Flowable class
    def wrap(self, availWidth: int, availHeight: int) -> tuple[int, int]:
        """Determine the rectangle this paragraph really needs."""
        # memorize available space
        self.avWidth: int = availWidth
        self.avHeight: int = availHeight

        logger.debug("*** wrap (%f, %f)", availWidth, availHeight)

        if not self.text:
            logger.debug("*** wrap (%f, %f) needed", 0, 0)
            return 0, 0

        # Split lines
        width: int = availWidth
        self.splitIndex = self.text.splitIntoLines(width, availHeight)

        self.width: int = availWidth
        self.height: int = self.text.height

        logger.debug(
            "*** wrap (%f, %f) needed, splitIndex %r",
            self.width,
            self.height,
            self.splitIndex,
        )

        return self.width, self.height

    def split(self, availWidth: int, availHeight: int) -> list[Paragraph]:
        """Split ourselves in two paragraphs."""
        logger.debug("*** split (%f, %f)", availWidth, availHeight)

        splitted: list[Paragraph] = []
        if self.splitIndex:
            text1: Text = self.text[: self.splitIndex]
            text2: Text = self.text[self.splitIndex :]
            p1: Paragraph = Paragraph(Text(text1), self.style, debug=self.debug)
            p2: Paragraph = Paragraph(
                Text(text2), self.style, debug=self.debug, splitted=True
            )
            splitted = [p1, p2]

            logger.debug("*** text1 %s / text %s", len(text1), len(text2))

        logger.debug("*** return %s", self.splitted)

        return splitted

    def draw(self) -> None:
        """Render the content of the paragraph."""
        logger.debug("*** draw")

        if not self.text:
            return

        canvas: Canvas = self.canv
        style: Style = self.style

        canvas.saveState()

        # Draw box around paragraph for debugging
        if self.debug:
            bw: float = 0.5
            bc: Color = Color(1, 1, 0)
            bg: Color = Color(0.9, 0.9, 0.9)
            canvas.setStrokeColor(bc)
            canvas.setLineWidth(bw)
            canvas.setFillColor(bg)
            canvas.rect(style.leftIndent, 0, self.width, self.height, fill=1, stroke=1)

        y: int = 0
        dy: int = self.height
        for line in self.text.lines:
            y += line.height
            for frag in line:
                # Box
                if hasattr(frag, "draw"):
                    frag.draw(canvas, dy - y)

                # Text
                if frag.get("text", ""):
                    canvas.setFont(frag["fontName"], frag["fontSize"])
                    canvas.setFillColor(frag.get("color", style["color"]))
                    canvas.drawString(frag["x"], dy - y + frag["y"], frag["text"])

                # XXX LINK
                link: bytes | str = frag.get("link", None)
                if link:
                    _scheme_re = re.compile("^[a-zA-Z][-+a-zA-Z0-9]+$")
                    x, y, w, h = frag["x"], dy - y, frag["width"], frag["fontSize"]
                    rect = (x, y, w, h)
                    if isinstance(link, bytes):
                        link = link.decode("utf8")
                    parts = link.split(":", maxsplit=1)
                    scheme = len(parts) == 2 and parts[0].lower() or ""
                    if _scheme_re.match(scheme) and scheme != "document":
                        kind = scheme.lower() == "pdf" and "GoToR" or "URI"
                        if kind == "GoToR":
                            link = parts[1]

                        canvas.linkURL(link, rect, relative=1, kind=kind)
                    else:
                        if link[0] == "#":
                            link = link[1:]
                            scheme = ""
                        canvas.linkRect(
                            "",
                            scheme != "document" and link or parts[1],
                            rect,
                            relative=1,
                        )

        canvas.restoreState()


class PageNumberFlowable(Flowable):
    def __init__(self) -> None:
        super().__init__()
        self.page: str | None = None
        self.pagecount: str | None = None

    def draw(self) -> None:
        self.page = str(self.canv._doctemplate.page)
        self.pagecount = str(self.canv._doctemplate._page_count)
