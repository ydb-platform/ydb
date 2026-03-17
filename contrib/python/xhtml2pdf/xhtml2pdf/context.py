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
import logging
import re
import urllib.parse as urlparse
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from reportlab import rl_settings
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.fonts import addMapping
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus.frames import Frame

try:
    from reportlab.pdfgen.canvas import ShowBoundaryValue
except ImportError:
    # reportlab < 4.0.9.1
    from reportlab.platypus.frames import ShowBoundaryValue
from reportlab.platypus.paraparser import ParaFrag, ps2tt, tt2ps

from xhtml2pdf import default, parser
from xhtml2pdf.files import B64InlineURI, getFile, pisaFileObject
from xhtml2pdf.tables import TableData
from xhtml2pdf.util import (
    arabic_format,
    copy_attrs,
    frag_text_language_check,
    get_default_asian_font,
    getColor,
    getCoords,
    getFloat,
    getFrameDimensions,
    getSize,
    set_asian_fonts,
    set_value,
)
from xhtml2pdf.w3c import css
from xhtml2pdf.xhtml2pdf_reportlab import (
    PmlPageCount,
    PmlPageTemplate,
    PmlParagraph,
    PmlParagraphAndImage,
    PmlTableOfContents,
)

if TYPE_CHECKING:
    from reportlab.platypus.flowables import Flowable

    from xhtml2pdf.xhtml2pdf_reportlab import PmlImage


rl_settings.warnOnMissingFontGlyphs = 0
log = logging.getLogger(__name__)

sizeDelta = 2  # amount to reduce font size by for super and sub script
subFraction = 0.4  # fraction of font size that a sub script should be lowered
superFraction = 0.4

NBSP = "\u00a0"


def clone(self, **kwargs) -> ParaFrag:
    n = ParaFrag(**self.__dict__)
    if kwargs:
        d = n.__dict__
        d.update(kwargs)
        # This else could cause trouble in Paragraphs with images etc.
        if "cbDefn" in d:
            del d["cbDefn"]
    n.bulletText = None
    return n


ParaFrag.clone = clone


def getParaFrag(style) -> ParaFrag:
    frag: ParaFrag = ParaFrag()

    set_value(
        frag,
        (
            "sub",
            "super",
            "rise",
            "underline",
            "strike",
            "greek",
            "leading",
            "leadingSpace",
            "spaceBefore",
            "spaceAfter",
            "leftIndent",
            "rightIndent",
            "firstLineIndent",
            "borderPadding",
            "paddingLeft",
            "paddingRight",
            "paddingTop",
            "paddingBottom",
            "bulletIndent",
            "insideStaticFrame",
            "outlineLevel",
        ),
        0,
    )

    set_value(
        frag,
        (
            "backColor",
            "vAlign",
            "link",
            "borderStyle",
            "borderColor",
            "listStyleType",
            "listStyleImage",
            "wordWrap",
            "height",
            "width",
            "bulletText",
        ),
        None,
    )
    set_value(
        frag,
        ("pageNumber", "pageCount", "outline", "outlineOpen", "keepWithNext", "rtl"),
        False,  # noqa: FBT003
    )

    frag.text = ""
    frag.fontName = "Times-Roman"
    frag.fontName, frag.bold, frag.italic = ps2tt(style.fontName)
    frag.fontSize = style.fontSize
    frag.textColor = style.textColor

    # Extras
    frag.letterSpacing = "normal"
    frag.leadingSource = "150%"
    frag.alignment = TA_LEFT
    frag.borderWidth = 1

    frag.borderLeftWidth = frag.borderWidth
    frag.borderLeftColor = frag.borderColor
    frag.borderLeftStyle = frag.borderStyle
    frag.borderRightWidth = frag.borderWidth
    frag.borderRightColor = frag.borderColor
    frag.borderRightStyle = frag.borderStyle
    frag.borderTopWidth = frag.borderWidth
    frag.borderTopColor = frag.borderColor
    frag.borderTopStyle = frag.borderStyle
    frag.borderBottomWidth = frag.borderWidth
    frag.borderBottomColor = frag.borderColor
    frag.borderBottomStyle = frag.borderStyle

    frag.whiteSpace = "normal"
    frag.bulletFontName = "Helvetica"
    frag.zoom = 1.0

    return frag


def getDirName(path) -> str:
    parts = urlparse.urlparse(path)
    if parts.scheme:
        return path
    return str(Path(path).parent.resolve())


class pisaCSSBuilder(css.CSSBuilder):
    c: pisaContext

    def atFontFace(self, declarations) -> tuple[dict, dict]:
        """Embed fonts."""
        result = self.ruleset([self.selector("*")], declarations)
        data = next(iter(result[0].values()))
        if "src" not in data:
            # invalid - source is required, ignore this specification
            return {}, {}
        names = data["font-family"]

        # Font weight
        fweight = str(data.get("font-weight", "normal")).lower()
        bold = fweight in {"bold", "bolder", "500", "600", "700", "800", "900"}
        if not bold and fweight != "normal":
            log.warning(
                self.c.warning("@fontface, unknown value font-weight '%s'", fweight)
            )

        # Font style
        italic = str(data.get("font-style", "")).lower() in {"italic", "oblique"}

        # The "src" attribute can be a CSS group but in that case
        # ignore everything except the font URI
        if isinstance(data["src"], list):
            fonts = [part for part in data["src"] if isinstance(part, str)]
        else:
            fonts = [data["src"]]

        for font in fonts:
            src = self.c.getFile(font, relative=self.c.cssParser.rootPath)
            if src and not src.notFound():
                self.c.loadFont(names, src, bold=bold, italic=italic)
        return {}, {}

    def _pisaAddFrame(
        self,
        name: str,
        data: dict,
        *,
        first: bool = False,
        border=None,
        size: tuple[float, float] = (0, 0),
    ) -> tuple[str, str | None, str | None, float, float, float, float, dict]:
        c = self.c
        if not name:
            name = "-pdf-frame-%d" % c.UID()
        if data.get("is_landscape", False):
            size = (size[1], size[0])
        x, y, w, h = getFrameDimensions(data, size[0], size[1])
        # print name, x, y, w, h
        # if not (w and h):
        #    return None
        if first:
            return name, None, data.get("-pdf-frame-border", border), x, y, w, h, data

        return (
            name,
            data.get("-pdf-frame-content", None),
            data.get("-pdf-frame-border", border),
            x,
            y,
            w,
            h,
            data,
        )

    @staticmethod
    def _getFromData(data, attr, default=None, func: Callable | None = None):
        if not func:

            def func(x):
                return x

        if isinstance(attr, (list, tuple)):
            for a in attr:
                return func(data[a]) if a in data else default
            return None
        return func(data[attr]) if attr in data else default

    @staticmethod
    def get_background_context(data: dict) -> dict:
        object_position = data.get("background-object-position")
        height = data.get("background-height")
        width = data.get("background-width")
        opacity = data.get("background-opacity")
        dev: dict = {"step": getFloat(data.get("background-page-step", 1))}
        if object_position:
            dev["object_position"] = [
                getSize(object_position[0]),
                getSize(object_position[1]),
            ]
        if height:
            dev["height"] = getSize(height)
        if width:
            dev["width"] = getSize(width)
        if opacity:
            dev["opacity"] = getFloat(opacity)
        return dev

    def atPage(
        self,
        name: str,
        pseudopage: str | None,
        data: dict,
        *,
        isLandscape: bool,
        pageBorder,
    ) -> tuple[dict, dict]:
        c = self.c
        name = name or "body"

        if name in c.templateList:
            log.warning(self.c.warning("template '%s' has already been defined", name))

        padding_top = self._getFromData(data, "padding-top", 0, getSize)
        padding_left = self._getFromData(data, "padding-left", 0, getSize)
        padding_right = self._getFromData(data, "padding-right", 0, getSize)
        padding_bottom = self._getFromData(data, "padding-bottom", 0, getSize)
        border_color = self._getFromData(
            data,
            (
                "border-top-color",
                "border-bottom-color",
                "border-left-color",
                "border-right-color",
            ),
            None,
            getColor,
        )
        border_width = self._getFromData(
            data,
            (
                "border-top-width",
                "border-bottom-width",
                "border-left-width",
                "border-right-width",
            ),
            0,
            getSize,
        )

        for prop in (
            "margin-top",
            "margin-left",
            "margin-right",
            "margin-bottom",
            "top",
            "left",
            "right",
            "bottom",
            "width",
            "height",
        ):
            if prop in data:
                c.frameList.append(
                    self._pisaAddFrame(
                        name, data, first=True, border=pageBorder, size=c.pageSize
                    )
                )
                break

        # Frames have to be calculated after we know the pagesize
        frameList = []
        staticList = []
        for fname, static, border, x, y, w, h, fdata in c.frameList:
            fpadding_top = self._getFromData(fdata, "padding-top", padding_top, getSize)
            fpadding_left = self._getFromData(
                fdata, "padding-left", padding_left, getSize
            )
            fpadding_right = self._getFromData(
                fdata, "padding-right", padding_right, getSize
            )
            fpadding_bottom = self._getFromData(
                fdata, "padding-bottom", padding_bottom, getSize
            )
            fborder_color = self._getFromData(
                fdata,
                (
                    "border-top-color",
                    "border-bottom-color",
                    "border-left-color",
                    "border-right-color",
                ),
                border_color,
                getColor,
            )
            fborder_width = self._getFromData(
                fdata,
                (
                    "border-top-width",
                    "border-bottom-width",
                    "border-left-width",
                    "border-right-width",
                ),
                border_width,
                getSize,
            )

            if border or pageBorder:
                frame_border = ShowBoundaryValue(
                    width=int(border)
                )  # frame_border = ShowBoundaryValue() to
                # frame_border = ShowBoundaryValue(width=int(border))
            else:
                frame_border = ShowBoundaryValue(
                    color=fborder_color, width=fborder_width
                )

            # fix frame sizing problem.
            if static:
                x, y, w, h = getFrameDimensions(fdata, c.pageSize[0], c.pageSize[1])
            x, y, w, h = getCoords(x, y, w, h, c.pageSize)
            if w <= 0 or h <= 0:
                log.warning(
                    self.c.warning(
                        "Negative width or height of frame. Check @frame definitions."
                    )
                )

            frame = Frame(
                x,
                y,
                w,
                h,
                id=fname,
                leftPadding=fpadding_left,
                rightPadding=fpadding_right,
                bottomPadding=fpadding_bottom,
                topPadding=fpadding_top,
                showBoundary=frame_border,
            )

            if static:
                frame.pisaStaticStory = []
                c.frameStatic[static] = [frame, *c.frameStatic.get(static, [])]
                staticList.append(frame)
            else:
                frameList.append(frame)

        background = data.get("background-image", None)
        background_context = self.get_background_context(data)
        if background:
            # should be relative to the css file
            background = self.c.getFile(background, relative=self.c.cssParser.rootPath)

        if not frameList:
            log.warning(
                c.warning(
                    "missing explicit frame definition for content or just static"
                    " frames"
                )
            )
            fname, static, border, x, y, w, h, data = self._pisaAddFrame(
                name, data, first=True, border=pageBorder, size=c.pageSize
            )
            x, y, w, h = getCoords(x, y, w, h, c.pageSize)
            if w <= 0 or h <= 0:
                log.warning(
                    c.warning(
                        "Negative width or height of frame. Check @page definitions."
                    )
                )

            if border or pageBorder:
                frame_border = ShowBoundaryValue()
            else:
                frame_border = ShowBoundaryValue(color=border_color, width=border_width)

            frameList.append(
                Frame(
                    x,
                    y,
                    w,
                    h,
                    id=fname,
                    leftPadding=padding_left,
                    rightPadding=padding_right,
                    bottomPadding=padding_bottom,
                    topPadding=padding_top,
                    showBoundary=frame_border,
                )
            )

        pt = PmlPageTemplate(id=name, frames=frameList, pagesize=c.pageSize)
        pt.pisaStaticList = staticList
        pt.pisaBackground = background
        pt.pisaBackgroundList = c.pisaBackgroundList
        pt.backgroundContext = background_context

        if isLandscape:
            pt.pageorientation = pt.LANDSCAPE

        c.templateList[name] = pt
        c.template = None
        c.frameList = []
        c.frameStaticList = []

        return {}, {}

    def atFrame(self, name: str, declarations) -> tuple[dict, dict]:
        if declarations:
            result = self.ruleset([self.selector("*")], declarations)
            # print "@BOX", name, declarations, result

            data = result[0]
            if data:
                try:
                    data = data.values()[0]
                except Exception:
                    data = data.popitem()[1]
                self.c.frameList.append(
                    self._pisaAddFrame(name, data, size=self.c.pageSize)
                )

        return {}, {}  # TODO: It always returns empty dicts?


class pisaCSSParser(css.CSSParser):
    def parseExternal(self, cssResourceName):
        result = None
        oldRootPath = self.rootPath
        cssFile = self.c.getFile(cssResourceName, relative=self.rootPath)
        if not cssFile:
            return None
        if self.rootPath and urlparse.urlparse(self.rootPath).scheme:
            self.rootPath = urlparse.urljoin(self.rootPath, cssResourceName)
        else:
            self.rootPath = getDirName(cssFile.uri)
        try:
            result = self.parse(cssFile.getData())
            self.rootPath = oldRootPath
        except Exception:
            log.exception("Error while parsing CSS file")
        return result


class PageNumberText:
    def __init__(self, *args, **kwargs) -> None:
        self.data: str = ""

    def __contains__(self, key) -> bool:
        if self.flowable.page is not None:
            self.data = str(self.flowable.page)
        return False

    def split(self, text: str) -> list[str]:
        return [self.data]

    def __getitem__(self, index: int) -> str:
        return self.data[index] if self.data else self.data

    def setFlowable(self, flowable: Flowable) -> None:
        self.flowable = flowable

    def __str__(self) -> str:
        return self.data


class PageCountText:
    def __init__(self, *args, **kwargs) -> None:
        self.data: str = ""

    def __str__(self) -> str:
        return self.data

    def __contains__(self, key) -> bool:
        if self.flowable.pagecount is not None:
            self.data = str(self.flowable.pagecount)
        return False

    def split(self, text: str) -> list[str]:
        return [self.data]

    def __getitem__(self, index: int) -> str:
        return self.data if not self.data else self.data[index]

    def setFlowable(self, flowable: Flowable) -> None:
        self.flowable = flowable


def reverse_sentence(sentence: str) -> str:
    words = sentence.split(" ")
    reverse_sentence = " ".join(reversed(words))
    return reverse_sentence[::-1]


class pisaContext:
    """
    Helper class for creation of reportlab story and container for
    various data.
    """

    def __init__(self, path: str = "", debug: int = 0, capacity: int = -1) -> None:
        self.fontList: dict[str, str] = copy.copy(default.DEFAULT_FONT)
        self.asianFontList: dict[str, str] = copy.copy(get_default_asian_font())
        self.anchorFrag: list = []
        self.anchorName: list = []
        self.fragAnchor: list = []
        self.fragList: list = []
        self.fragStack: list = []
        self.frameList: list = []
        self.frameStaticList: list = []
        self.frameStatioundList: list = []
        self.log: list = []
        self.path: list = []
        self.pisaBackgroundList: list = []
        self.select_options: list[str] = []
        self.story: list = []
        self.image: PmlImage | None = None
        self.indexing_story: PmlPageCount | None = None
        self.keepInFrameIndex = None
        self.node = None
        self.template = None
        self.tableData: TableData = TableData()
        self.err: int = 0
        self.fontSize: float = 0.0
        self.listCounter: int = 0
        self.uidctr: int = 0
        self.warn: int = 0
        self.cssDefaultText: str = ""
        self.cssText: str = ""
        self.language: str = ""
        self.text: str = ""
        self.frameStatic: dict = {}
        self.imageData: dict = {}
        self.templateList: dict = {}
        self.capacity: int = capacity
        self.toc: PmlTableOfContents = PmlTableOfContents()
        self.multiBuild: bool = False
        self.pageSize: tuple[float, float] = A4
        self.baseFontSize: float = getSize("12pt")
        self.frag: ParaFrag = getParaFrag(ParagraphStyle(f"default{self.UID()}"))
        self.fragBlock: ParaFrag = self.frag
        self.fragStrip: bool = True
        self.force: bool = False
        self.dir: str = "ltr"

        #: External callback function for path calculations
        self.pathCallback = None

        # Store path to document
        self.pathDocument: str = path or "__dummy__"
        parts = urlparse.urlparse(self.pathDocument)
        if not parts.scheme:
            self.pathDocument = str(Path(self.pathDocument).absolute().resolve())
        self.pathDirectory: str = getDirName(self.pathDocument)

        self.meta: dict[str, str | tuple[float, float]] = {
            "author": "",
            "title": "",
            "subject": "",
            "keywords": "",
            "pagesize": A4,
        }

    def setDir(self, direction):
        if direction == "rtl":
            self.frag.rtl = True
        self.dir = direction

    def UID(self):
        self.uidctr += 1
        return self.uidctr

    # METHODS FOR CSS
    def addCSS(self, value):
        value = value.strip()
        if value.startswith("<![CDATA["):
            value = value[9:-3]
        if value.startswith("<!--"):
            value = value[4:-3]
        self.cssText += value.strip() + "\n"

    # METHODS FOR CSS
    def addDefaultCSS(self, value):
        value = value.strip()
        if value.startswith("<![CDATA["):
            value = value[9:-3]
        if value.startswith("<!--"):
            value = value[4:-3]
        self.cssDefaultText += value.strip() + "\n"

    def parseCSS(self):
        # This self-reference really should be refactored. But for now
        # we'll settle for using weak references. This avoids memory
        # leaks because the garbage collector (at least on cPython
        # 2.7.3) isn't aggressive enough.
        import weakref

        self.cssBuilder = pisaCSSBuilder(mediumSet=["all", "print", "pdf"])
        # self.cssBuilder.c = self
        self.cssBuilder._c = weakref.ref(self)
        pisaCSSBuilder.c = property(lambda self: self._c())

        self.cssParser = pisaCSSParser(self.cssBuilder)
        self.cssParser.rootPath = self.pathDirectory
        # self.cssParser.c = self
        self.cssParser._c = weakref.ref(self)
        pisaCSSParser.c = property(lambda self: self._c())

        self.css = self.cssParser.parse(self.cssText)
        self.cssDefault = self.cssParser.parse(self.cssDefaultText)
        self.cssCascade = css.CSSCascadeStrategy(
            userAgent=self.cssDefault, user=self.css
        )
        self.cssCascade.parser = self.cssParser

    # METHODS FOR STORY
    def addStory(self, data):
        self.story.append(data)

    def swapStory(self, story=None):
        story = story if story is not None else []
        self.story, story = copy.copy(story), copy.copy(self.story)
        return story

    def toParagraphStyle(self, first) -> ParagraphStyle:
        style = ParagraphStyle(
            "default%d" % self.UID(), keepWithNext=first.keepWithNext
        )

        copy_attrs(
            style,
            first,
            (
                "fontName",
                "fontSize",
                "letterSpacing",
                "backColor",
                "spaceBefore",
                "spaceAfter",
                "leftIndent",
                "rightIndent",
                "firstLineIndent",
                "textColor",
                "alignment",
                "bulletIndent",
                "wordWrap",
                "borderTopStyle",
                "borderTopWidth",
                "borderTopColor",
                "borderBottomStyle",
                "borderBottomWidth",
                "borderBottomColor",
                "borderLeftStyle",
                "borderLeftWidth",
                "borderLeftColor",
                "borderRightStyle",
                "borderRightWidth",
                "borderRightColor",
                "paddingTop",
                "paddingBottom",
                "paddingLeft",
                "paddingRight",
                "borderPadding",
            ),
        )

        style.leading = max(first.leading + first.leadingSpace, first.fontSize * 1.25)
        style.bulletFontName = first.bulletFontName or first.fontName
        style.bulletFontSize = first.fontSize

        # Border handling for Paragraph

        # Transfer the styles for each side of the border, *not* the whole
        # border values that reportlab supports. We'll draw them ourselves in
        # PmlParagraph.

        # If no border color is given, the text color is used (XXX Tables!)
        if (style.borderTopColor is None) and style.borderTopWidth:
            style.borderTopColor = first.textColor
        if (style.borderBottomColor is None) and style.borderBottomWidth:
            style.borderBottomColor = first.textColor
        if (style.borderLeftColor is None) and style.borderLeftWidth:
            style.borderLeftColor = first.textColor
        if (style.borderRightColor is None) and style.borderRightWidth:
            style.borderRightColor = first.textColor

        style.fontName = tt2ps(first.fontName, first.bold, first.italic)

        return style

    def addTOC(self) -> None:
        if not self.node:
            return

        styles = []
        for i in range(20):
            self.node.attributes["class"] = "pdftoclevel%d" % i
            self.cssAttr = parser.CSSCollect(self.node, self)
            parser.CSS2Frag(
                self,
                {
                    "margin-top": 0,
                    "margin-bottom": 0,
                    "margin-left": 0,
                    "margin-right": 0,
                },
                isBlock=True,
            )
            pstyle = self.toParagraphStyle(self.frag)
            styles.append(pstyle)

        self.toc.levelStyles = styles
        self.addStory(self.toc)
        self.indexing_story = None

    def addPageCount(self) -> None:
        if not self.multiBuild:
            self.indexing_story = PmlPageCount()
            self.multiBuild = True

    @staticmethod
    def getPageCount(flow: Flowable) -> PageCountText:
        pc = PageCountText()
        pc.setFlowable(flow)
        return pc

    @staticmethod
    def addPageNumber(flow):
        pgnumber = PageNumberText()
        pgnumber.setFlowable(flow)
        return pgnumber

    @staticmethod
    def dumpPara(_frags, _style):
        return

    def addPara(self, *, force: bool = False) -> None:
        force = force or self.force
        self.force = False

        # Cleanup the trail
        reversed(self.fragList)

        # Find maximum lead
        maxLeading: int = 0
        # fontSize = 0
        for frag in self.fragList:
            leading = getSize(frag.leadingSource, frag.fontSize) + frag.leadingSpace
            maxLeading = max(leading, frag.fontSize + frag.leadingSpace, maxLeading)
            frag.leading = leading

        if force or (self.text.strip() and self.fragList):
            # Update paragraph style by style of first fragment
            first = self.fragBlock
            style = self.toParagraphStyle(first)
            # style.leading = first.leading + first.leadingSpace
            if first.leadingSpace:
                style.leading = maxLeading
            else:
                style.leading = (
                    getSize(first.leadingSource, first.fontSize) + first.leadingSpace
                )

            bulletText = copy.copy(first.bulletText)
            first.bulletText = None

            # Add paragraph to story
            if force or len(self.fragAnchor + self.fragList) > 0:
                # We need this empty fragment to work around problems in
                # Reportlab paragraphs regarding backGround etc.
                if self.fragList:
                    # Reset not only text but also page#, otherwise you might end up with duplicate rendered page#s
                    self.fragList.append(
                        self.fragList[-1].clone(
                            text="", pageNumber=False, pageCount=False
                        )
                    )
                else:
                    blank = self.frag.clone()
                    blank.fontName = "Helvetica"
                    blank.text = ""
                    self.fragList.append(blank)

                self.dumpPara(self.fragAnchor + self.fragList, style)
                if hasattr(self, "language"):
                    language = self.__getattribute__("language")
                    detect_language_result = arabic_format(self.text, language)
                    if detect_language_result is not None:
                        self.text = detect_language_result

                para = PmlParagraph(
                    self.text,
                    style,
                    frags=self.fragAnchor + self.fragList,
                    bulletText=bulletText,
                    dir=self.dir,
                )

                para.outline = first.outline
                para.outlineLevel = first.outlineLevel
                para.outlineOpen = first.outlineOpen
                para.keepWithNext = first.keepWithNext
                para.autoLeading = "max"

                if self.image:
                    para = PmlParagraphAndImage(
                        para, self.image, side=self.imageData.get("align", "left")
                    )

                self.addStory(para)

            self.fragAnchor = []
            first.bulletText = None

        # Reset data

        self.image = None
        self.imageData = {}

        self.clearFrag()

    # METHODS FOR FRAG
    def clearFrag(self) -> None:
        self.fragList = []
        self.fragStrip = True
        self.text = ""

    def copyFrag(self, **kw):
        return self.frag.clone(**kw)

    def newFrag(self, **kw):
        self.frag = self.frag.clone(**kw)
        return self.frag

    def _appendFrag(self, frag) -> None:
        if frag.link and frag.link.startswith("#"):
            self.anchorFrag.append((frag, frag.link[1:]))
        self.fragList.append(frag)

    # XXX Argument frag is useless!
    def addFrag(self, text="", frag=None):
        frag = baseFrag = self.frag.clone()

        # if sub and super are both on they will cancel each other out
        if frag.sub == 1 and frag.super == 1:
            frag.sub = 0
            frag.super = 0

        # XXX Has to be replaced by CSS styles like vertical-align and
        # font-size
        if frag.sub:
            frag.rise = -frag.fontSize * subFraction
            frag.fontSize = max(frag.fontSize - sizeDelta, 3)
        elif frag.super:
            frag.rise = frag.fontSize * superFraction
            frag.fontSize = max(frag.fontSize - sizeDelta, 3)

        # bold, italic, and underline
        frag.fontName = frag.bulletFontName = tt2ps(
            frag.fontName, frag.bold, frag.italic
        )
        if isinstance(text, (PageNumberText, PageCountText)):
            frag.text = text
            # self.text += frag.text
            self._appendFrag(frag)
            return

        # Replace &shy; with empty and normalize NBSP
        text = text.replace("\xad", "").replace("\xc2\xa0", NBSP).replace("\xa0", NBSP)

        if frag.whiteSpace == "pre":
            # Handle by lines
            for text in re.split(r"(\r\n|\n|\r)", text):
                # This is an exceptionally expensive piece of code
                self.text += text
                if ("\n" in text) or ("\r" in text):
                    # If EOL insert a linebreak
                    frag = baseFrag.clone()
                    frag.text = ""
                    frag.lineBreak = 1
                    self._appendFrag(frag)
                else:
                    # Handle tabs in a simple way
                    text = text.replace("\t", 8 * " ")
                    # Somehow for Reportlab NBSP have to be inserted
                    # as single character fragments
                    for text in re.split(r"(\ )", text):
                        frag = baseFrag.clone()
                        if text == " ":
                            text = NBSP
                        frag.text = text
                        self._appendFrag(frag)
        else:
            for text in re.split("(" + NBSP + ")", text):
                frag = baseFrag.clone()
                if text == NBSP:
                    self.force = True
                    frag.text = NBSP
                    self.text += text
                    self._appendFrag(frag)
                else:
                    frag.text = " ".join(("x" + text + "x").split())[1:-1]
                    language_check = frag_text_language_check(self, frag.text)
                    if language_check:
                        frag.text = language_check
                    if self.fragStrip:
                        frag.text = frag.text.lstrip()
                        if frag.text:
                            self.fragStrip = False
                    self.text += frag.text
                    self._appendFrag(frag)

    def pushFrag(self) -> None:
        self.fragStack.append(self.frag)
        self.newFrag()

    def pullFrag(self) -> None:
        self.frag = self.fragStack.pop()

    # XXX
    def _getFragment(self, line=20):
        try:
            return repr(" ".join(self.node.toxml().split()[:line]))
        except Exception:
            return ""

    @staticmethod
    def _getLineNumber() -> int:
        return 0

    def context(self, msg: str) -> str:
        return f"{msg!s}\n{self._getFragment(50)}"

    def warning(self, msg, *args):
        self.warn += 1
        self.log.append(
            (
                default.PML_WARNING,
                self._getLineNumber(),
                str(msg),
                self._getFragment(50),
            )
        )
        try:
            return self.context(msg % args)
        except Exception:
            return self.context(msg)

    def error(self, msg, *args):
        self.err += 1
        self.log.append(
            (default.PML_ERROR, self._getLineNumber(), str(msg), self._getFragment(50))
        )
        try:
            return self.context(msg % args)
        except Exception:
            return self.context(msg)

    def getFile(self, name, relative=None) -> pisaFileObject | None:
        """Returns a file name or None."""
        if name is None:
            return None
        return getFile(name, relative or self.pathDirectory, callback=self.pathCallback)

    def getFontName(self, names, default="helvetica"):
        """Name of a font."""
        # print names, self.fontList
        if not isinstance(names, list):
            names = str(names)
            names = names.strip().split(",")
        for name in names:
            name = str(name)
            font = name.strip().lower()
            if font in self.asianFontList:
                font = self.asianFontList.get(font, None)
                set_asian_fonts(font)
            else:
                font = self.fontList.get(font, None)
            if font is not None:
                return font
        return self.fontList.get(default, None)

    def registerFont(self, fontname, alias=None):
        alias = alias if alias is not None else []
        self.fontList[str(fontname).lower()] = str(fontname)
        for a in alias:
            self.fontList[str(a)] = str(fontname)

    # TODO: convert to getFile to support remotes fonts
    def loadFont(self, names, src, encoding="WinAnsiEncoding", bold=0, italic=0):
        # XXX Just works for local filenames!
        if names and src:
            file = src
            src = file.uri

            log.debug("Load font %r", src)
            if isinstance(names, str) and names.startswith("#"):
                names = names.strip("#")
            if isinstance(names, list):
                fontAlias = names
            else:
                fontAlias = (x.lower().strip() for x in names.split(",") if x)

            # XXX Problems with unicode here
            fontAlias = [str(x) for x in fontAlias]
            fontName = fontAlias[0]

            font_type = None
            if isinstance(file.instance, B64InlineURI):
                if file.getMimeType() == "font/ttf":
                    font_type = "ttf"
            else:
                parts = src.split(".")
                baseName, suffix = ".".join(parts[:-1]), parts[-1]
                suffix = suffix.lower()
                if suffix in ("ttf", "ttc"):
                    font_type = "ttf"
                elif suffix in ("afm", "pfb"):
                    font_type = suffix

            if font_type == "ttf":
                # determine full font name according to weight and style
                fullFontName = "%s_%d%d" % (fontName, bold, italic)

                # check if font has already been registered
                if fullFontName in self.fontList:
                    log.warning(
                        self.warning(
                            "Repeated font embed for %s, skip new embed ", fullFontName
                        )
                    )
                else:
                    # Register TTF font and special name
                    filename = file.getNamedFile()
                    file = TTFont(fullFontName, filename)
                    pdfmetrics.registerFont(file)

                    # Add or replace missing styles
                    for bold in (0, 1):
                        for italic in (0, 1):
                            if (
                                "%s_%d%d" % (fontName, bold, italic)
                            ) not in self.fontList:
                                addMapping(fontName, bold, italic, fullFontName)

                    # Register "normal" name and the place holder for style
                    self.registerFont(fontName, [*fontAlias, fullFontName])

            elif font_type in ("afm", ""):
                if font_type == "afm":
                    afm = file.getNamedFile()
                    tfile = pisaFileObject(baseName + ".pfb", basepath=file.basepath)
                    pfb = tfile.getNamedFile()
                else:
                    pfb = file.getNamedFile()
                    tfile = pisaFileObject(baseName + ".afm", basepath=file.basepath)
                    afm = tfile.getNamedFile()

                # determine full font name according to weight and style
                fullFontName = "%s_%d%d" % (fontName, bold, italic)

                # check if font has already been registered
                if fullFontName in self.fontList:
                    log.warning(
                        self.warning(
                            "Repeated font embed for %s, skip new embed", fontName
                        )
                    )
                else:
                    # Include font
                    face = pdfmetrics.EmbeddedType1Face(afm, pfb)
                    fontNameOriginal = face.name
                    pdfmetrics.registerTypeFace(face)
                    # print fontName, fontNameOriginal, fullFontName
                    justFont = pdfmetrics.Font(fullFontName, fontNameOriginal, encoding)
                    pdfmetrics.registerFont(justFont)

                    # Add or replace missing styles
                    for bold in (0, 1):
                        for italic in (0, 1):
                            if (
                                "%s_%d%d" % (fontName, bold, italic)
                            ) not in self.fontList:
                                addMapping(fontName, bold, italic, fontNameOriginal)

                    # Register "normal" name and the place holder for style
                    self.registerFont(
                        fontName, [*fontAlias, fullFontName, fontNameOriginal]
                    )
            else:
                log.warning(self.warning("wrong attributes for <pdf:font>"))
