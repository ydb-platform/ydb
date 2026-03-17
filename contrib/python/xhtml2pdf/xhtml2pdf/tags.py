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

import copy
import json
import logging
import re
import string
import warnings
from typing import TYPE_CHECKING, Callable, ClassVar

from reportlab.graphics.barcode import createBarcodeDrawing
from reportlab.graphics.charts.legends import Legend
from reportlab.graphics.charts.textlabels import Label
from reportlab.graphics.shapes import Drawing, Rect
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch, mm
from reportlab.platypus.doctemplate import FrameBreak, NextPageTemplate
from reportlab.platypus.flowables import Flowable, HRFlowable, PageBreak, Spacer
from reportlab.platypus.frames import Frame
from reportlab.platypus.paraparser import ABag, tt2ps

from xhtml2pdf.charts import (
    DoughnutChart,
    HorizontalBar,
    HorizontalLine,
    LegendedPieChart,
    PieChart,
    VerticalBar,
)
from xhtml2pdf.paragraph import PageNumberFlowable
from xhtml2pdf.util import DPI96, ImageWarning, getAlign, getColor, getSize
from xhtml2pdf.xhtml2pdf_reportlab import PmlImage, PmlInput, PmlPageTemplate

if TYPE_CHECKING:
    from xml.dom.minidom import Element

    from reportlab.pdfgen.canvas import Canvas
    from reportlab.platypus.paraparser import ParaFrag

    from xhtml2pdf.context import pisaContext
    from xhtml2pdf.files import pisaFileObject
    from xhtml2pdf.parser import AttrContainer

log = logging.getLogger(__name__)


def deprecation(message):
    warnings.warn(f"<{message}> is deprecated!", DeprecationWarning, stacklevel=2)


class pisaTag:
    """The default class for a tag definition."""

    def __init__(self, node: Element, attr: AttrContainer) -> None:
        self.node: Element = node
        self.tag: str = node.tagName
        self.attr: AttrContainer = attr

    def start(self, c: pisaContext) -> None:
        pass

    def end(self, c: pisaContext) -> None:
        pass


class pisaTagBODY(pisaTag):
    """
    We can also assume that there is a BODY tag because html5lib
    adds it for us. Here we take the base font size for later calculations
    in the FONT tag.
    """

    def start(self, c: pisaContext) -> None:
        c.baseFontSize = c.frag.fontSize
        if "dir" in self.attr and self.attr["dir"]:
            c.setDir(self.attr["dir"])
        # print("base font size", c.baseFontSize)


class pisaTagTITLE(pisaTag):
    def end(self, c: pisaContext) -> None:  # noqa: PLR6301
        c.meta["title"] = c.text
        c.clearFrag()


class pisaTagSTYLE(pisaTag):
    def start(self, c: pisaContext) -> None:  # noqa: PLR6301
        c.addPara()

    def end(self, c: pisaContext) -> None:  # noqa: PLR6301
        c.clearFrag()


class pisaTagMETA(pisaTag):
    def start(self, c: pisaContext) -> None:
        name: str = self.attr.name.lower()
        if name in {"author", "subject", "keywords"}:
            c.meta[name] = self.attr.content


class pisaTagSUP(pisaTag):
    def start(self, c: pisaContext) -> None:  # noqa: PLR6301
        c.frag.super = 1


class pisaTagSUB(pisaTag):
    def start(self, c: pisaContext) -> None:  # noqa: PLR6301
        c.frag.sub = 1


class pisaTagA(pisaTag):
    rxLink = r"^(#|[a-z]+\:).*"

    def start(self, c: pisaContext) -> None:
        attr = self.attr
        # XXX Also support attr.id ?
        if attr.name:
            # Important! Make sure that cbDefn is not inherited by other
            # fragments because of a bug in Reportlab!
            afrag = c.frag.clone()
            # These 3 lines are needed to fix an error with non internal fonts
            afrag.fontName = "Helvetica"
            afrag.bold = 0
            afrag.italic = 0
            afrag.cbDefn = ABag(kind="anchor", name=attr.name, label="anchor")
            c.fragAnchor.append(afrag)
            c.anchorName.append(attr.name)
        if attr.href and re.match(self.rxLink, attr.href):
            c.frag.link = attr.href

    def end(self, c: pisaContext) -> None:
        pass


class pisaTagFONT(pisaTag):
    # Source: http://www.w3.org/TR/CSS21/fonts.html#propdef-font-size

    def start(self, c: pisaContext) -> None:
        if self.attr["color"] is not None:
            c.frag.textColor = getColor(self.attr["color"])
        if self.attr["face"] is not None:
            c.frag.fontName = c.getFontName(self.attr["face"])
        if self.attr["size"] is not None:
            size = getSize(self.attr["size"], c.frag.fontSize, c.baseFontSize)
            c.frag.fontSize = max(size, 1.0)

    def end(self, c: pisaContext) -> None:
        pass


class pisaTagP(pisaTag):
    def start(self, c: pisaContext) -> None:
        # save the type of tag; it's used in PmlBaseDoc.afterFlowable()
        # to check if we need to add an outline-entry
        # c.frag.tag = self.tag
        if "dir" in self.attr and self.attr["dir"]:
            c.setDir(self.attr["dir"])
        if self.attr.align is not None:
            c.frag.alignment = getAlign(self.attr.align)


class pisaTagDIV(pisaTagP):
    pass


class pisaTagH1(pisaTagP):
    pass


class pisaTagH2(pisaTagP):
    pass


class pisaTagH3(pisaTagP):
    pass


class pisaTagH4(pisaTagP):
    pass


class pisaTagH5(pisaTagP):
    pass


class pisaTagH6(pisaTagP):
    pass


def listDecimal(c: pisaContext) -> str:
    c.listCounter += 1
    return str("%d." % c.listCounter)


roman_numeral_map: tuple[tuple[int, str], ...] = (
    (1000, "M"),
    (900, "CM"),
    (500, "D"),
    (400, "CD"),
    (100, "C"),
    (90, "XC"),
    (50, "L"),
    (40, "XL"),
    (10, "X"),
    (9, "IX"),
    (5, "V"),
    (4, "IV"),
    (1, "I"),
)


def int_to_roman(i: int) -> str:
    result: list[str] = []
    for integer, numeral in roman_numeral_map:
        count: int = int(i / integer)
        result.append(numeral * count)
        i -= integer * count
    return "".join(result)


def listUpperRoman(c: pisaContext) -> str:
    c.listCounter += 1
    roman: str = int_to_roman(c.listCounter)
    return f"{roman}."


def listLowerRoman(c: pisaContext) -> str:
    return listUpperRoman(c).lower()


def listUpperAlpha(c: pisaContext) -> str:
    c.listCounter += 1
    index: int = c.listCounter - 1
    try:
        alpha: str = string.ascii_uppercase[index]
    except IndexError:
        # needs to start over and double the character
        # this will probably fail for anything past the 2nd time
        alpha = string.ascii_uppercase[index - 26]
        alpha *= 2
    return f"{alpha}."


def listLowerAlpha(c: pisaContext) -> str:
    return listUpperAlpha(c).lower()


_bullet: str = "\u2022"
_list_style_type: dict[str, str | Callable] = {
    "none": "",
    "disc": _bullet,
    "circle": _bullet,  # XXX PDF has no equivalent
    "square": _bullet,  # XXX PDF has no equivalent
    "decimal": listDecimal,
    "decimal-leading-zero": listDecimal,
    "lower-roman": listLowerRoman,
    "upper-roman": listUpperRoman,
    "hebrew": listDecimal,
    "georgian": listDecimal,
    "armenian": listDecimal,
    "cjk-ideographic": listDecimal,
    "hiragana": listDecimal,
    "katakana": listDecimal,
    "hiragana-iroha": listDecimal,
    "katakana-iroha": listDecimal,
    "lower-latin": listDecimal,
    "lower-alpha": listLowerAlpha,
    "upper-latin": listDecimal,
    "upper-alpha": listUpperAlpha,
    "lower-greek": listDecimal,
}


class pisaTagUL(pisaTagP):
    def start(self, c: pisaContext) -> None:
        self.counter, c.listCounter = c.listCounter, 0

    def end(self, c: pisaContext):
        c.addPara()
        # XXX Simulate margin for the moment
        c.addStory(Spacer(width=1, height=c.fragBlock.spaceAfter))
        c.listCounter = self.counter


class pisaTagOL(pisaTagUL):
    def start(self, c: pisaContext) -> None:
        start = self.attr.start - 1 if self.attr.start else 0
        self.counter, c.listCounter = c.listCounter, start


class pisaTagLI(pisaTag):
    def start(self, c: pisaContext) -> None:
        lst: str | Callable = _list_style_type.get(
            c.frag.listStyleType or "disc", _bullet
        )
        frag: ParaFrag = copy.copy(c.frag)

        self.offset: int = 0
        if frag.listStyleImage is not None:
            frag.text = ""
            f = frag.listStyleImage
            if f and (not f.notFound()):
                img = PmlImage(f.getData(), src=f.uri, width=None, height=None)
                img.drawHeight *= DPI96
                img.drawWidth *= DPI96
                img.pisaZoom = frag.zoom
                img.drawWidth *= img.pisaZoom
                img.drawHeight *= img.pisaZoom
                frag.image = img
                self.offset = max(0, img.drawHeight - c.frag.fontSize)
        elif isinstance(lst, str):
            frag.text = lst
        else:
            # XXX This should be the recent font, but it throws errors in Reportlab!
            frag.text = lst(c)

        # XXX This should usually be done in the context!!!
        frag.fontName = frag.bulletFontName = tt2ps(
            frag.fontName, frag.bold, frag.italic
        )
        c.frag.bulletText = [frag]

    def end(self, c: pisaContext) -> None:
        c.fragBlock.spaceBefore += self.offset


class pisaTagBR(pisaTag):
    def start(self, c: pisaContext) -> None:  # noqa: PLR6301
        c.frag.lineBreak = 1
        c.addFrag()
        c.fragStrip = True
        del c.frag.lineBreak
        c.force = True


class pisaTagIMG(pisaTag):
    def start(self, c: pisaContext) -> None:
        attr: AttrContainer = self.attr
        log.debug("Parsing img tag, src: %r", attr.src)
        log.debug("Attrs: %r", attr)

        if attr.src:
            filedata: pisaFileObject = attr.src.getData()
            if filedata:
                try:
                    align = attr.align or c.frag.vAlign or "baseline"
                    width = c.frag.width
                    height = c.frag.height

                    if attr.width:
                        width = attr.width * DPI96
                    if attr.height:
                        height = attr.height * DPI96

                    img = PmlImage(filedata, src=attr.src.uri, width=None, height=None)

                    img.pisaZoom = c.frag.zoom

                    img.drawHeight *= DPI96
                    img.drawWidth *= DPI96

                    if (width is None) and (height is not None):
                        factor = (
                            getSize(height, default=img.drawHeight) / img.drawHeight
                        )
                        img.drawWidth *= factor
                        img.drawHeight = getSize(height, default=img.drawHeight)
                    elif (height is None) and (width is not None):
                        factor = getSize(width, default=img.drawWidth) / img.drawWidth
                        img.drawHeight *= factor
                        img.drawWidth = getSize(width, default=img.drawWidth)
                    elif (width is not None) and (height is not None):
                        img.drawWidth = getSize(width, default=img.drawWidth)
                        img.drawHeight = getSize(height, default=img.drawHeight)

                    img.drawWidth *= img.pisaZoom
                    img.drawHeight *= img.pisaZoom

                    img.spaceBefore = c.frag.spaceBefore
                    img.spaceAfter = c.frag.spaceAfter

                    # print "image", id(img), img.drawWidth, img.drawHeight

                    """
                    TODO:

                    - Apply styles
                    - vspace etc.
                    - Borders
                    - Test inside tables
                    """

                    c.force = True
                    if align in {"left", "right"}:
                        c.image = img
                        c.imageData = {"align": align}

                    else:
                        # Important! Make sure that cbDefn is not inherited by other
                        # fragments because of a bug in Reportlab!
                        # afrag = c.frag.clone()

                        valign = align
                        if valign in {"texttop"}:
                            valign = "top"
                        elif valign in {"absmiddle"}:
                            valign = "middle"
                        elif valign in {"absbottom", "baseline"}:
                            valign = "bottom"

                        afrag = c.frag.clone()
                        afrag.text = ""
                        afrag.fontName = "Helvetica"  # Fix for a nasty bug!!!
                        afrag.cbDefn = ABag(
                            kind="img",
                            image=img,  # .getImage(), # XXX Inline?
                            valign=valign,
                            fontName="Helvetica",
                            fontSize=img.drawHeight,
                            width=img.drawWidth,
                            height=img.drawHeight,
                        )

                        c.fragList.append(afrag)
                        c.fontSize = img.drawHeight

                except ImageWarning as e:
                    log.warning(c.warning(f"{e}:"))
                except Exception:
                    log.warning(c.warning("Error in handling image:"), exc_info=True)
            else:
                log.warning(
                    c.warning(
                        f"Could not get image data from src attribute: {attr.src.uri}"
                    )
                )
        else:
            log.warning(c.warning("The src attribute of image tag is empty!"))


class pisaTagHR(pisaTag):
    def start(self, c: pisaContext) -> None:
        c.addPara()
        c.addStory(
            HRFlowable(
                color=self.attr.color,
                thickness=self.attr.size,
                width=self.attr.get("width", "100%") or "100%",
                spaceBefore=c.frag.spaceBefore,
                spaceAfter=c.frag.spaceAfter,
            )
        )


# --- Forms


class pisaTagINPUT(pisaTag):
    @staticmethod
    def _render(c: pisaContext, attr: AttrContainer) -> None:
        width: int = 10
        height: int = 10
        if attr.type == "text":
            width = 100
            height = 12
        c.addStory(
            PmlInput(
                attr.name,
                input_type=attr.type,
                default=attr.value,
                width=width,
                height=height,
            )
        )

    def end(self, c: pisaContext) -> None:
        c.addPara()
        attr = self.attr
        if attr.name:
            self._render(c, attr)
        c.addPara()


class pisaTagTEXTAREA(pisaTagINPUT):
    @staticmethod
    def _render(c: pisaContext, attr: AttrContainer) -> None:
        multiline: int = 1 if int(attr.rows) > 1 else 0
        height: int = int(attr.rows) * 15
        width: int = int(attr.cols) * 5

        # this does not currently support the ability to pre-populate the text field with data that appeared within the <textarea></textarea> tags
        c.addStory(
            PmlInput(
                attr.name,
                input_type="text",
                default="",
                width=width,
                height=height,
                multiline=multiline,
            )
        )


class pisaTagSELECT(pisaTagINPUT):
    def start(self, c: pisaContext) -> None:  # noqa: PLR6301
        c.select_options = ["One", "Two", "Three"]

    @staticmethod
    def _render(c: pisaContext, attr: AttrContainer) -> None:
        c.addStory(
            PmlInput(
                attr.name,
                input_type="select",
                default=c.select_options[0],
                options=c.select_options,
                width=100,
                height=40,
            )
        )
        c.select_options = []


class pisaTagOPTION(pisaTag):
    pass


class pisaTagPDFNEXTPAGE(pisaTag):
    """<pdf:nextpage name="" />."""

    def start(self, c: pisaContext) -> None:
        c.addPara()
        if self.attr.name:
            c.addStory(NextPageTemplate(self.attr.name))
        c.addStory(PageBreak())


class pisaTagPDFNEXTTEMPLATE(pisaTag):
    """<pdf:nexttemplate name="" />."""

    def start(self, c: pisaContext) -> None:
        c.addStory(NextPageTemplate(self.attr["name"]))


class pisaTagPDFNEXTFRAME(pisaTag):
    """<pdf:nextframe name="" />."""

    def start(self, c: pisaContext) -> None:  # noqa: PLR6301
        c.addPara()
        c.addStory(FrameBreak())


class pisaTagPDFSPACER(pisaTag):
    """<pdf:spacer height="" />."""

    def start(self, c: pisaContext) -> None:
        c.addPara()
        c.addStory(Spacer(1, self.attr.height))


class pisaTagPDFPAGENUMBER(pisaTag):
    """<pdf:pagenumber example="" />."""

    def start(self, c: pisaContext) -> None:  # noqa: PLR6301
        flow = PageNumberFlowable()
        pageNumber = c.addPageNumber(flow)
        c.addStory(flow)
        c.frag.pageNumber = True
        c.addFrag(pageNumber)
        c.frag.pageNumber = False


class pisaTagPDFPAGECOUNT(pisaTag):
    """<pdf:pagecount />."""

    def start(self, c: pisaContext) -> None:  # noqa: PLR6301
        flow = PageNumberFlowable()
        pageCount = c.getPageCount(flow)
        c.addStory(flow)
        c.frag.pageCount = True
        c.addFrag(pageCount)
        c.frag.pageCount = False

    def end(self, c: pisaContext) -> None:  # noqa: PLR6301
        c.addPageCount()


class pisaTagPDFTOC(pisaTag):
    """<pdf:toc />."""

    def end(self, c: pisaContext) -> None:  # noqa: PLR6301
        c.multiBuild = True
        c.addTOC()


class pisaTagPDFFRAME(pisaTag):
    """<pdf:frame name="" static box="" />."""

    def start(self, c: pisaContext) -> None:
        deprecation("pdf:frame")
        attrs = self.attr

        name = attrs["name"]
        if name is None:
            name = f"frame{c.UID()}"

        x, y, w, h = attrs.box
        self.frame = Frame(
            x,
            y,
            w,
            h,
            id=name,
            leftPadding=0,
            rightPadding=0,
            bottomPadding=0,
            topPadding=0,
            showBoundary=attrs.border,
        )

        self.static = False
        if self.attr.static:
            self.static = True
            c.addPara()
            self.story = c.swapStory()
        else:
            c.frameList.append(self.frame)

    def end(self, c: pisaContext):
        if self.static:
            c.addPara()
            self.frame.pisaStaticStory = c.story
            c.frameStaticList.append(self.frame)
            c.swapStory(self.story)


class pisaTagPDFTEMPLATE(pisaTag):
    """
    <pdf:template name="" static box="" >
        <pdf:frame...>
    </pdf:template>.
    """

    def start(self, c: pisaContext) -> None:
        deprecation("pdf:template")
        attrs = self.attr
        name = attrs["name"]
        c.frameList = []
        c.frameStaticList = []
        if name in c.templateList:
            log.warning(c.warning("template '%s' has already been defined", name))

    def end(self, c: pisaContext):
        attrs = self.attr
        name = attrs["name"]
        if len(c.frameList) <= 0:
            log.warning(c.warning("missing frame definitions for template"))

        pt = PmlPageTemplate(id=name, frames=c.frameList, pagesize=A4)
        pt.pisaStaticList = c.frameStaticList
        pt.pisaBackgroundList = c.pisaBackgroundList
        pt.pisaBackground = self.attr.background

        c.templateList[name] = pt
        c.template = None
        c.frameList = []
        c.frameStaticList = []


class pisaTagPDFLANGUAGE(pisaTag):
    """<pdf:language name=""/>."""

    def start(self, c: pisaContext) -> None:
        c.language = self.attr.name


class pisaTagPDFFONT(pisaTag):
    """<pdf:fontembed name="" src="" />."""

    def start(self, c: pisaContext) -> None:
        deprecation("pdf:font")
        c.loadFont(self.attr.name, self.attr.src, self.attr.encoding)


class pisaTagPDFBARCODE(pisaTag):
    _codeName: ClassVar[dict[str, str]] = {
        "I2OF5": "I2of5",
        "ITF": "I2of5",
        "CODE39": "Standard39",
        "EXTENDEDCODE39": "Extended39",
        "CODE93": "Standard93",
        "EXTENDEDCODE93": "Extended93",
        "MSI": "MSI",
        "CODABAR": "Codabar",
        "NW7": "Codabar",
        "CODE11": "Code11",
        "FIM": "FIM",
        "POSTNET": "POSTNET",
        "USPS4S": "USPS_4State",
        "CODE128": "Code128",
        "EAN13": "EAN13",
        "EAN8": "EAN8",
        "QR": "QR",
    }

    class _barcodeWrapper(Flowable):
        """Wrapper for barcode widget."""

        def __init__(self, codeName: str = "Code128", value: str = "", **kw) -> None:
            self.vertical: int = kw.get("vertical", 0)
            self.widget = createBarcodeDrawing(codeName, value=value, **kw)

        def draw(self, canvas: Canvas, xoffset: int = 0, **kw) -> None:
            # NOTE: 'canvas' is mutable, so canvas.restoreState() is a MUST.
            canvas.saveState()
            # NOTE: checking vertical value to rotate the barcode
            if self.vertical:
                width, height = self.wrap(0, 0)
                # Note: moving our canvas to the new origin
                canvas.translate(height, -width)
                canvas.rotate(90)
            else:
                canvas.translate(xoffset, 0)
            self.widget.canv = canvas
            self.widget.draw()
            canvas.restoreState()

        def wrap(self, aW, aH):
            return self.widget.wrap(aW, aH)

    def start(self, c: pisaContext) -> None:
        attr = self.attr
        codeName: str = attr.type or "Code128"
        codeName = pisaTagPDFBARCODE._codeName[codeName.upper().replace("-", "")]
        humanReadable: int = int(attr.humanreadable)
        vertical: int = int(attr.vertical)
        checksum: int = int(attr.checksum)
        barWidth: float = attr.barwidth or 0.01 * inch
        barHeight: float = attr.barheight or 0.5 * inch
        fontName: str = c.getFontName("OCRB10,OCR-B,OCR B,OCRB")  # or "Helvetica"
        fontSize: float = attr.fontsize or 2.75 * mm

        # Assure minimal size.
        if codeName in {"EAN13", "EAN8"}:
            barWidth = max(barWidth, 0.264 * mm)
            fontSize = max(fontSize, 2.75 * mm)
        else:  # Code39 etc.
            barWidth = max(barWidth, 0.0075 * inch)

        barcode = pisaTagPDFBARCODE._barcodeWrapper(
            codeName=codeName,
            value=attr.value,
            barWidth=barWidth,
            barHeight=barHeight,
            humanReadable=humanReadable,
            vertical=vertical,
            checksum=checksum,
            fontName=fontName,
            fontSize=fontSize,
        )

        width, height = barcode.wrap(c.frag.width, c.frag.height)
        c.force = True

        valign = attr.align or c.frag.vAlign or "baseline"
        if valign in {"texttop"}:
            valign = "top"
        elif valign in {"absmiddle"}:
            valign = "middle"
        elif valign in {"absbottom", "baseline"}:
            valign = "bottom"

        afrag = c.frag.clone()
        afrag.text = ""
        afrag.fontName = fontName
        afrag.cbDefn = ABag(
            kind="barcode", barcode=barcode, width=width, height=height, valign=valign
        )
        c.fragList.append(afrag)


class pisaTagCANVAS(pisaTag):
    def __init__(self, node: Element, attr: AttrContainer) -> None:
        super().__init__(node, attr)
        self.chart = None
        self.shapes = {
            "horizontalbar": HorizontalBar,
            "verticalbar": VerticalBar,
            "horizontalline": HorizontalLine,
            "pie": PieChart,
            "doughnut": DoughnutChart,
            "legendedPie": LegendedPieChart,
        }

    def start(self, c: pisaContext) -> None:
        pass

    def end(self, c: pisaContext) -> None:
        data = None
        width: int = 350
        height: int = 150

        try:
            data = json.loads(c.text)
        except json.JSONDecodeError:
            print("JSON Decode Error")

        if data and c.node:
            nodetype = dict(c.node.attributes).get("type")
            nodewidth = dict(c.node.attributes).get("width")
            nodeheight = dict(c.node.attributes).get("height")
            canvastype = None

            if nodetype is not None:
                canvastype = nodetype.nodeValue

            if canvastype:
                c.clearFrag()

            if nodewidth:
                width = int(nodewidth.nodeValue)
            if nodeheight:
                height = int(nodeheight.nodeValue)

            self.chart = self.shapes[data["type"]]()
            draw = Drawing(width, height)  # CONTAINER
            draw.background = Rect(
                115,
                25,
                width,
                height,
                strokeWidth=1,
                strokeColor="#868686",
                fillColor="#f8fce8",
            )

            # REQUIRED DATA
            self.chart.set_properties(data)

            # OPTIONAL DATA
            if "title" in data:
                title = Label()
                self.chart.set_title_properties(data["title"], title)
                draw.add(title)

            if "legend" in data and data["legend"]:
                legend = Legend()
                self.chart.set_legend(data["legend"], legend)
                self.chart.load_data_legend(data, legend)
                draw.add(legend)

            # ADD CHART TO DRAW OBJECT
            draw.add(self.chart)
            c.addStory(draw)
