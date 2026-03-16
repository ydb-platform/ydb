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

# ruff: noqa: N802, N803
from __future__ import annotations

import contextlib
import copy
import logging
import sys
from hashlib import md5
from html import escape as html_escape
from io import BytesIO, StringIO
from typing import TYPE_CHECKING, ClassVar, Iterator
from uuid import uuid4

from PIL import Image as PILImage
from PIL import UnidentifiedImageError
from PIL.Image import Image
from reportlab.lib.enums import TA_RIGHT
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.utils import LazyImageReader, flatten, haveImages, open_for_read
from reportlab.pdfbase import pdfform
from reportlab.platypus.doctemplate import (
    BaseDocTemplate,
    IndexingFlowable,
    PageTemplate,
)
from reportlab.platypus.flowables import (
    CondPageBreak,
    Flowable,
    KeepInFrame,
    ParagraphAndImage,
)
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.platypus.tables import Table, TableStyle
from reportlab.rl_config import register_reset

from xhtml2pdf.builders.watermarks import WaterMarks
from xhtml2pdf.files import pisaFileObject, pisaTempFile
from xhtml2pdf.reportlab_paragraph import Paragraph
from xhtml2pdf.util import ImageWarning, getBorderStyle

if TYPE_CHECKING:
    from reportlab.graphics.shapes import Drawing
    from reportlab.pdfgen.canvas import Canvas


try:
    from reportlab.graphics import renderPM
    from svglib.svglib import svg2rlg
except ImportError:
    svg2rlg = None
    renderPM = None

log = logging.getLogger(__name__)

MAX_IMAGE_RATIO: float = 0.95
PRODUCER: str = "xhtml2pdf <https://github.com/xhtml2pdf/xhtml2pdf/>"


class PTCycle(list):
    def __init__(self) -> None:
        self._restart: int = 0
        self._idx: int = 0
        super().__init__()

    def cyclicIterator(self) -> Iterator:
        while 1:
            yield self[self._idx]
            self._idx += 1
            if self._idx >= len(self):
                self._idx = self._restart


class PmlMaxHeightMixIn:
    def setMaxHeight(self, availHeight: int) -> int:
        self.availHeightValue: int = availHeight
        if availHeight < 70000 and hasattr(self, "canv"):
            if not hasattr(self.canv, "maxAvailHeightValue"):
                self.canv.maxAvailHeightValue = 0
            self.availHeightValue = self.canv.maxAvailHeightValue = max(
                availHeight, self.canv.maxAvailHeightValue
            )
        return self.availHeightValue

    def getMaxHeight(self) -> int:
        return self.availHeightValue if hasattr(self, "availHeightValue") else 0


class PmlBaseDoc(BaseDocTemplate):
    """We use our own document template to get access to the canvas and set some information once."""

    def beforePage(self) -> None:
        self.canv._doc.info.producer = PRODUCER

        """
        # Convert to ASCII because there is a Bug in Reportlab not
        # supporting other than ASCII. Send to list on 23.1.2007
        author = toString(self.pml_data.get("author", "")).encode("ascii","ignore")
        subject = toString(self.pml_data.get("subject", "")).encode("ascii","ignore")
        title = toString(self.pml_data.get("title", "")).encode("ascii","ignore")
        # print repr((author,title,subject))
        self.canv.setAuthor(author)
        self.canv.setSubject(subject)
        self.canv.setTitle(title)
        if self.pml_data.get("fullscreen", 0):
            self.canv.showFullScreen0()
        if self.pml_data.get("showoutline", 0):
            self.canv.showOutline()
        if self.pml_data.get("duration", None) is not None:
            self.canv.setPageDuration(self.pml_data["duration"])
        """

    def afterFlowable(self, flowable: Flowable) -> None:
        # Does the flowable contain fragments?
        if getattr(flowable, "outline", False):
            self.notify(
                "TOCEntry",
                (
                    flowable.outlineLevel,
                    html_escape(copy.deepcopy(flowable.text), quote=True),
                    self.page,
                ),
            )

    def handle_nextPageTemplate(self, pt: str | int | list | tuple) -> None:
        """If pt has also templates for even and odd page convert it to list."""
        has_left_template: bool = self._has_template_for_name(f"{pt}_left")
        has_right_template: bool = self._has_template_for_name(f"{pt}_right")

        if has_left_template and has_right_template:
            pt = [f"{pt}_left", f"{pt}_right"]

        """On endPage change to the page template with name or index pt"""
        if isinstance(pt, str):
            if hasattr(self, "_nextPageTemplateCycle"):
                del self._nextPageTemplateCycle
            for t in self.pageTemplates:
                if t.id == pt:
                    self._nextPageTemplateIndex: int = self.pageTemplates.index(t)
                    return
            msg = f"can't find template('{pt}')"
            raise ValueError(msg)
        if isinstance(pt, int):
            if hasattr(self, "_nextPageTemplateCycle"):
                del self._nextPageTemplateCycle
            self._nextPageTemplateIndex = pt
        elif isinstance(pt, (list, tuple)):
            # used for alternating left/right pages
            # collect the refs to the template objects, complain if any are bad
            c: PTCycle = PTCycle()
            for ptn in pt:
                # special case name used to short circuit the iteration
                if ptn == "*":
                    c._restart = len(c)
                    continue
                for t in self.pageTemplates:
                    sys.exit()
                    if t.id == ptn.strip():
                        c.append(t)
                        break
            if not c:
                msg = "No valid page templates in cycle"
                raise ValueError(msg)
            if c._restart > len(c):
                msg = "Invalid cycle restart position"
                raise ValueError(msg)

            # ensure we start on the first one$
            self._nextPageTemplateCycle: PageTemplate = c.cyclicIterator()
        else:
            msg = "Argument pt should be string or integer or list"
            raise TypeError(msg)

    def _has_template_for_name(self, name: str) -> bool:
        return any(template.id == name.strip() for template in self.pageTemplates)


class PmlPageTemplate(PageTemplate):
    PORTRAIT: str = "portrait"
    LANDSCAPE: str = "landscape"
    # by default portrait
    pageorientation: str = PORTRAIT

    def __init__(self, **kw) -> None:
        self.pisaStaticList: list = []
        self.pisaBackgroundList: list[tuple] = []
        self.pisaBackground = None
        super().__init__(**kw)
        self._page_count: int = 0
        self._first_flow: bool = True

        # Background Image
        self.img = None
        self.ph: int = 0
        self.h: int = 0
        self.w: int = 0

        self.backgroundids: list[int] = []

    def isFirstFlow(self, canvas: Canvas) -> bool:
        if self._first_flow:
            if canvas.getPageNumber() <= self._page_count:
                self._first_flow = False
            else:
                self._page_count = canvas.getPageNumber()
                canvas._doctemplate._page_count = canvas.getPageNumber()
        return self._first_flow

    def isPortrait(self) -> bool:
        return self.pageorientation == self.PORTRAIT

    def isLandscape(self) -> bool:
        return self.pageorientation == self.LANDSCAPE

    def beforeDrawPage(self, canvas: Canvas, doc):
        canvas.saveState()
        try:
            if doc.pageTemplate.id not in self.backgroundids:
                pisaBackground = None
                if (
                    hasattr(self, "pisaBackground")
                    and self.pisaBackground
                    and (not self.pisaBackground.notFound())
                ):
                    if self.pisaBackground.getMimeType().startswith("image/"):
                        pisaBackground = WaterMarks.generate_pdf_background(
                            self.pisaBackground,
                            self.pagesize,
                            is_portrait=self.isPortrait(),
                            context=self.backgroundContext,
                        )
                    else:
                        pisaBackground = self.pisaBackground
                    self.backgroundids.append(doc.pageTemplate.id)
                if pisaBackground:
                    self.pisaBackgroundList.append(
                        (canvas.getPageNumber(), pisaBackground, self.backgroundContext)
                    )

            def pageNumbering(objList):
                for obj in flatten(objList):
                    if isinstance(obj, PmlParagraph):
                        for frag in obj.frags:
                            if frag.pageNumber:
                                frag.text = str(pagenumber)
                            elif frag.pageCount:
                                frag.text = str(canvas._doctemplate._page_count)

                    elif isinstance(obj, PmlTable):
                        # Flatten the cells ([[1,2], [3,4]] becomes [1,2,3,4])
                        flat_cells = [
                            item for sublist in obj._cellvalues for item in sublist
                        ]
                        pageNumbering(flat_cells)

            try:
                # Paint static frames
                pagenumber = canvas.getPageNumber()
                if pagenumber > self._page_count:
                    self._page_count = canvas.getPageNumber()
                    canvas._doctemplate._page_count = canvas.getPageNumber()

                for frame in self.pisaStaticList:
                    frame_copy = copy.deepcopy(frame)
                    story = frame_copy.pisaStaticStory
                    pageNumbering(story)

                    frame_copy.addFromList(story, canvas)

            except Exception:  # TODO: Kill this!
                log.debug("PmlPageTemplate", exc_info=True)
        finally:
            canvas.restoreState()


_ctr: int = 1


class PmlImageReader:  # TODO We need a factory here, returning either a class for java or a class for PIL
    """Wraps up either PIL or Java to get data from bitmaps."""

    _cache: ClassVar[dict] = {}
    # Experimental features, disabled by default
    use_cache: bool = False
    use_lazy_loader: bool = False
    process_internal_files: bool = False

    def __init__(self, fileName: PmlImage | Image | str) -> None:
        if isinstance(fileName, PmlImage):
            self.__dict__ = fileName.__dict__  # borgize
            return
            # start with lots of null private fields, to be populated by
        # the relevant engine.
        self.fileName: PmlImage | Image | str = fileName or f"PILIMAGE_{id(self)}"
        self._image: Image = None
        self._width: int | None = None
        self._height: int | None = None
        self._transparent = None
        self._data: bytes | str | None = None
        self._dataA: PmlImageReader | None = None
        self.fp: BytesIO | StringIO | None = None
        if Image and isinstance(fileName, Image):
            self._image = fileName
            self.fp = getattr(fileName, "fp", None)
        else:
            try:
                self.fp = open_for_read(fileName, "b")
                if self.process_internal_files and isinstance(self.fp, StringIO):
                    data: str = self.fp.read()
                    with contextlib.suppress(Exception):
                        self.fp.close()
                    if self.use_cache:
                        if not self._cache:
                            register_reset(self._cache.clear)
                        cache_key = md5(data.encode("utf8")).digest()
                        data = self._cache.setdefault(cache_key, data)
                    self.fp = StringIO(data)
                elif self.use_lazy_loader and isinstance(fileName, str):
                    # try Ralf Schmitt's re-opening technique of avoiding too many open files
                    self.fp.close()
                    del self.fp  # will become a property in the next statement
                    self.__class__ = LazyImageReader
                if haveImages:
                    # detect which library we are using and open the image
                    if not self._image:
                        self._image = self._read_image(self.fp)
                    if getattr(self._image, "format", None) == "JPEG":
                        self.jpeg_fh = self._jpeg_fh
                else:
                    from reportlab.pdfbase.pdfutils import readJPEGInfo

                    try:
                        self._width, self._height, c = readJPEGInfo(self.fp)
                    except Exception as e:
                        msg = (
                            "Imaging Library not available, unable to import bitmaps"
                            " only jpegs"
                        )
                        raise ImageWarning(msg) from e
                    self.jpeg_fh = self._jpeg_fh
                    self._data = self.fp.read()
                    self.fp.seek(0)
            # Catch all errors that are known and don't need the stack trace
            except UnidentifiedImageError as e:
                msg = "Cannot identify image file"
                raise ImageWarning(msg) from e

    @staticmethod
    def _read_image(fp) -> Image:
        if sys.platform[:4] == "java":
            from java.io import ByteArrayInputStream
            from javax.imageio import ImageIO

            input_stream = ByteArrayInputStream(fp.read())
            return ImageIO.read(input_stream)
        return PILImage.open(fp)

    def _jpeg_fh(self) -> BytesIO | StringIO | None:
        fp = self.fp
        if isinstance(fp, (BytesIO, StringIO)):
            fp.seek(0)
        return fp

    def jpeg_fh(self) -> BytesIO | StringIO | None:  # noqa: PLR6301
        """Might be replaced with _jpeg_fh in some cases"""
        return None

    def getSize(self) -> tuple[int, int]:
        if self._width is None or self._height is None:
            if sys.platform[:4] == "java":
                self._width = self._image.getWidth()
                self._height = self._image.getHeight()
            else:
                self._width, self._height = self._image.size
            if TYPE_CHECKING:
                assert self._width is not None and self._height is not None
        return self._width, self._height

    def getRGBData(self) -> bytes | str:
        """Return byte array of RGB data as string."""
        if self._data is None:
            self._dataA = None
            if sys.platform[:4] == "java":
                import jarray  # TODO: Move to top.
                from java.awt.image import PixelGrabber

                width, height = self.getSize()
                buffer = jarray.zeros(width * height, "i")
                pg: PixelGrabber = PixelGrabber(
                    self._image, 0, 0, width, height, buffer, 0, width
                )
                pg.grabPixels()
                # there must be a way to do this with a cast not a byte-level loop,
                # I just haven't found it yet...
                pixels: list[str] = []
                a = pixels.append
                for rgb in buffer:
                    a(chr((rgb >> 16) & 0xFF))
                    a(chr((rgb >> 8) & 0xFF))
                    a(chr(rgb & 0xFF))
                self._data = "".join(pixels)
                self.mode = "RGB"
            else:
                im = self._image
                mode = self.mode = im.mode
                if mode == "RGBA":
                    im.load()
                    self._dataA = PmlImageReader(im.split()[3])
                    im = im.convert("RGB")
                    self.mode = "RGB"
                elif mode not in {"L", "RGB", "CMYK"}:
                    im = im.convert("RGB")
                    self.mode = "RGB"
                self._data = im.tobytes() if hasattr(im, "tobytes") else im.tostring()
        return self._data

    def getImageData(self):
        width, height = self.getSize()
        return width, height, self.getRGBData()

    def getTransparent(self):
        if sys.platform[:4] == "java":
            return None
        if "transparency" in self._image.info:
            transparency = self._image.info["transparency"] * 3
            palette = self._image.palette
            if hasattr(palette, "palette"):
                palette = palette.palette
            elif hasattr(palette, "data"):
                palette = palette.data
            else:
                return None

            # 8-bit PNGs could give an empty string as transparency value, so
            # we have to be careful here.
            try:
                return list(palette[transparency : transparency + 3])
            except Exception as e:
                log.debug(str(e), exc_info=e)
                return None
        else:
            return None

    def __str__(self) -> str:
        if isinstance(self.fileName, (PmlImage, Image, BytesIO)):
            fn = self.fileName.read() or id(self)
            return f"PmlImageObject_{hash(fn)}"
        return str(self.fileName or id(self))


class PmlImage(Flowable, PmlMaxHeightMixIn):
    def __init__(
        self,
        data: pisaFileObject | pisaTempFile | bytes,
        src: str | None = None,
        width: int | None = None,
        height: int | None = None,
        mask: str = "auto",
        mimetype: str | None = None,
        **kw: dict,
    ) -> None:
        self.kw: dict = kw
        self.hAlign: str = "CENTER"
        self._mask: str = mask
        self._imgdata: bytes = b""
        if isinstance(data, bytes):
            self._imgdata = data
        elif isinstance(data, pisaTempFile):
            self._imgdata = data.getvalue()
        elif isinstance(data, pisaFileObject):
            self._imgdata = data.getData() or b""
        self.src: str | None = src
        # print "###", repr(data)
        self.mimetype: str | None = mimetype

        # Resolve size
        drawing = self.getDrawing()
        self.imageWidth: float = 0.0
        self.imageHeight: float = 0.0
        if drawing:
            _, _, self.imageWidth, self.imageHeight = drawing.getBounds() or (
                0,
                0,
                0,
                0,
            )
        else:
            img = self.getImage()
            if img:
                self.imageWidth, self.imageHeight = img.getSize()

        self.drawWidth: float = width or self.imageWidth
        self.drawHeight: float = height or self.imageHeight

    def wrap(self, availWidth, availHeight):
        """
        Resize the image if necessary.

        This can be called more than once! Do not overwrite important data like drawWidth.
        """
        availHeight = self.setMaxHeight(availHeight)
        # print "image wrap", id(self), availWidth, availHeight, self.drawWidth, self.drawHeight
        width = min(self.drawWidth, availWidth)
        wfactor = float(width) / self.drawWidth
        height = min(self.drawHeight, availHeight * MAX_IMAGE_RATIO)
        hfactor = float(height) / self.drawHeight
        factor = min(wfactor, hfactor)
        self.dWidth = self.drawWidth * factor
        self.dHeight = self.drawHeight * factor
        # print "image result", factor, self.dWidth, self.dHeight
        return self.dWidth, self.dHeight

    def getDrawing(
        self, width: float | None = None, height: float | None = None
    ) -> Drawing | None:
        """If this image is a vector image and the library is available, returns a ReportLab Drawing."""
        if svg2rlg:
            try:
                drawing = svg2rlg(BytesIO(self._imgdata))
            except Exception:
                return None
            if drawing:
                # Apply size
                scale_x = 1
                scale_y = 1
                try:
                    if getattr(self, "drawWidth", None) is not None:
                        if width is None:
                            width = self.drawWidth
                        scale_x = width / drawing.width
                    if getattr(self, "drawHeight", None) is not None:
                        if height is None:
                            height = self.drawHeight
                        scale_y = height / drawing.height
                    if scale_x != 1 or scale_y != 1:
                        drawing.scale(scale_x, scale_y)
                except ZeroDivisionError:
                    log.warning(
                        "SVG drawing could not be resized: %r",
                        self.src or self._imgdata[:50],
                    )
                return drawing
        return None

    def getDrawingRaster(self) -> BytesIO | None:
        """If this image is a vector image and the libraries are available, returns a PNG raster."""
        if svg2rlg and renderPM:
            svg: Drawing = self.getDrawing()
            if svg:
                imgdata = BytesIO()
                renderPM.drawToFile(svg, imgdata, fmt="PNG")
                return imgdata
        return None

    def getImage(self) -> PmlImageReader:
        """Return a raster image."""
        vectorRaster = self.getDrawingRaster()
        imgdata = vectorRaster or BytesIO(self._imgdata)
        return PmlImageReader(imgdata)

    def draw(self) -> None:
        # TODO this code should work, but untested
        # drawing = self.getDrawing(self.dWidth, self.dHeight)
        # if drawing and renderPDF:
        #     renderPDF.draw(drawing, self.canv, 0, 0)
        # else:
        img = self.getImage()
        self.canv.drawImage(img, 0, 0, self.dWidth, self.dHeight, mask=self._mask)

    def identity(self, maxLen=None):
        return Flowable.identity(self, maxLen)


class PmlParagraphAndImage(ParagraphAndImage, PmlMaxHeightMixIn):
    def wrap(self, availWidth, availHeight):
        self.I.canv = self.canv
        result = ParagraphAndImage.wrap(self, availWidth, availHeight)
        del self.I.canv
        return result

    def split(self, availWidth, availHeight):
        # print "# split", id(self)
        if not hasattr(self, "wI"):
            self.wI, self.hI = self.I.wrap(
                availWidth, availHeight
            )  # drawWidth, self.I.drawHeight
        return ParagraphAndImage.split(self, availWidth, availHeight)


class PmlParagraph(Paragraph, PmlMaxHeightMixIn):
    def _calcImageMaxSizes(self, availWidth, availHeight):
        self.hasImages = False
        for frag in self.frags:
            if hasattr(frag, "cbDefn") and frag.cbDefn.kind == "img":
                img = frag.cbDefn
                if img.width > 0 and img.height > 0:
                    self.hasImages = True
                    width = min(img.width, availWidth)
                    wfactor = float(width) / img.width
                    height = min(
                        img.height, availHeight * MAX_IMAGE_RATIO
                    )  # XXX 99% because 100% do not work...
                    hfactor = float(height) / img.height
                    factor = min(wfactor, hfactor)
                    img.height *= factor
                    img.width *= factor

    def wrap(self, availWidth, availHeight):
        availHeight = self.setMaxHeight(availHeight)

        style = self.style

        self.deltaWidth = (
            style.paddingLeft
            + style.paddingRight
            + style.borderLeftWidth
            + style.borderRightWidth
        )
        self.deltaHeight = (
            style.paddingTop
            + style.paddingBottom
            + style.borderTopWidth
            + style.borderBottomWidth
        )

        # reduce the available width & height by the padding so the wrapping
        # will use the correct size
        availWidth -= self.deltaWidth
        availHeight -= self.deltaHeight

        # Modify maximum image sizes
        self._calcImageMaxSizes(availWidth, availHeight)

        # call the base class to do wrapping and calculate the size
        Paragraph.wrap(self, availWidth, availHeight)

        # self.height = max(1, self.height)
        # self.width = max(1, self.width)

        # increase the calculated size by the padding
        self.width += self.deltaWidth
        self.height += self.deltaHeight

        return self.width, self.height

    def split(self, availWidth, availHeight):
        if len(self.frags) <= 0:
            return []

        # the split information is all inside self.blPara
        if not hasattr(self, "deltaWidth"):
            self.wrap(availWidth, availHeight)

        availWidth -= self.deltaWidth
        availHeight -= self.deltaHeight

        return Paragraph.split(self, availWidth, availHeight)

    def draw(self):
        # Create outline
        if getattr(self, "outline", False):
            # Check level and add all levels
            last = getattr(self.canv, "outlineLast", -1) + 1
            while last < self.outlineLevel:
                # print "(OUTLINE",  last, self.text
                key = uuid4().hex
                self.canv.bookmarkPage(key)
                self.canv.addOutlineEntry(self.text, key, last, not self.outlineOpen)
                last += 1
            self.canv.outlineLast = self.outlineLevel

            key = uuid4().hex

            self.canv.bookmarkPage(key)
            self.canv.addOutlineEntry(
                self.text, key, self.outlineLevel, not self.outlineOpen
            )
            last += 1

        # Draw the background and borders here before passing control on to
        # ReportLab. This is because ReportLab can't handle the individual
        # components of the border independently. This will also let us
        # support more border styles eventually.
        canvas = self.canv
        style = self.style
        bg = style.backColor
        leftIndent = style.leftIndent
        bp = 0  # style.borderPadding

        x = leftIndent - bp
        y = -bp
        w = self.width - (leftIndent + style.rightIndent) + 2 * bp
        h = self.height + 2 * bp

        if bg:
            # draw a filled rectangle (with no stroke) using bg color
            canvas.saveState()
            canvas.setFillColor(bg)
            canvas.rect(x, y, w, h, fill=1, stroke=0)
            canvas.restoreState()

        # we need to hide the bg color (if any) so Paragraph won't try to draw it again
        style.backColor = None

        # offset the origin to compensate for the padding
        canvas.saveState()
        canvas.translate(
            (style.paddingLeft + style.borderLeftWidth),
            -1 * (style.paddingTop + style.borderTopWidth),
        )  # + (style.leading / 4)))

        # Call the base class draw method to finish up
        Paragraph.draw(self)
        canvas.restoreState()

        # Reset color because we need it again if we run 2-PASS like we
        # do when using TOC
        style.backColor = bg

        canvas.saveState()

        def _drawBorderLine(bstyle, width, color, x1, y1, x2, y2):
            # We need width and border style to be able to draw a border
            if width and getBorderStyle(bstyle):
                # If no color for border is given, the text color is used (like defined by W3C)
                if color is None:
                    color = style.textColor
                    # print "Border", bstyle, width, color
                if color is not None:
                    canvas.setStrokeColor(color)
                    canvas.setLineWidth(width)
                    canvas.line(x1, y1, x2, y2)

        _drawBorderLine(
            style.borderLeftStyle,
            style.borderLeftWidth,
            style.borderLeftColor,
            x,
            y,
            x,
            y + h,
        )
        _drawBorderLine(
            style.borderRightStyle,
            style.borderRightWidth,
            style.borderRightColor,
            x + w,
            y,
            x + w,
            y + h,
        )
        _drawBorderLine(
            style.borderTopStyle,
            style.borderTopWidth,
            style.borderTopColor,
            x,
            y + h,
            x + w,
            y + h,
        )
        _drawBorderLine(
            style.borderBottomStyle,
            style.borderBottomWidth,
            style.borderBottomColor,
            x,
            y,
            x + w,
            y,
        )

        canvas.restoreState()


class PmlKeepInFrame(KeepInFrame, PmlMaxHeightMixIn):
    def wrap(self, availWidth, availHeight):
        availWidth = max(availWidth, 1.0)
        availHeight = max(availHeight, 1.0)
        self.maxWidth = availWidth
        self.maxHeight = self.setMaxHeight(availHeight)
        return KeepInFrame.wrap(self, availWidth, availHeight)


class PmlTable(Table, PmlMaxHeightMixIn):
    @staticmethod
    def _normWidth(w, maxw):
        """Normalize width when using percentages."""
        if isinstance(w, str):
            w = (maxw / 100.0) * float(w[:-1])
        elif (w is None) or (w == "*"):
            w = maxw
        return min(w, maxw)

    def _listCellGeom(self, V, w, s, W=None, H=None, aH=72000):
        # print "#", self.availHeightValue
        if aH == 72000:
            aH = self.getMaxHeight() or aH
        return Table._listCellGeom(self, V, w, s, W=W, H=H, aH=aH)

    def wrap(self, availWidth, availHeight):
        self.setMaxHeight(availHeight)

        # Strange bug, sometime the totalWidth is not set !?
        if not hasattr(self, "totalWidth"):
            self.totalWidth = availWidth

        # Prepare values
        totalWidth = self._normWidth(self.totalWidth, availWidth)
        remainingWidth = totalWidth
        remainingCols = 0
        newColWidths = self._colWidths

        # Calculate widths that are fix
        # IMPORTANT!!! We can not substitute the private value
        # self._colWidths therefore we have to modify list in place
        for i, colWidth in enumerate(newColWidths):
            if colWidth is not None:
                newColWidth = self._normWidth(colWidth, totalWidth)
                remainingWidth -= newColWidth
            else:
                remainingCols += 1
                newColWidth = None
            newColWidths[i] = newColWidth

        # Distribute remaining space
        minCellWidth = totalWidth * 0.01
        if remainingCols > 0:
            for i, colWidth in enumerate(newColWidths):
                if colWidth is None:
                    newColWidths[i] = max(
                        minCellWidth, remainingWidth / remainingCols
                    )  # - 0.1

        # Bigger than totalWidth? Lets reduce the fix entries propotionally

        if sum(newColWidths) > totalWidth:
            quotient = totalWidth / sum(newColWidths)
            for i in range(len(newColWidths)):
                newColWidths[i] *= quotient

        # To avoid rounding errors adjust one col with the difference
        diff = sum(newColWidths) - totalWidth
        if diff > 0:
            newColWidths[0] -= diff

        return Table.wrap(self, availWidth, availHeight)


class PmlPageCount(IndexingFlowable):
    def __init__(self) -> None:
        super().__init__()
        self.second_round = False

    def isSatisfied(self):
        s = self.second_round
        self.second_round = True
        return s

    def drawOn(self, canvas, x, y, _sW=0):
        pass


class PmlTableOfContents(TableOfContents):
    def wrap(self, availWidth, availHeight):
        """All table properties should be known by now."""
        widths = (availWidth - self.rightColumnWidth, self.rightColumnWidth)

        # makes an internal table which does all the work.
        # we draw the LAST RUN's entries!  If there are
        # none, we make some dummy data to keep the table
        # from complaining
        if len(self._lastEntries) == 0:
            _tempEntries = [(0, "Placeholder for table of contents", 0)]
        else:
            _tempEntries = self._lastEntries

        lastMargin = 0
        tableData = []
        tableStyle = [
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]
        for i, entry in enumerate(_tempEntries):
            level, text, pageNum = entry[:3]
            leftColStyle = self.levelStyles[level]
            if i:  # Not for first element
                tableStyle.append(
                    (
                        "TOPPADDING",
                        (0, i),
                        (-1, i),
                        max(lastMargin, leftColStyle.spaceBefore),
                    )
                )
                # print leftColStyle.leftIndent
            lastMargin = leftColStyle.spaceAfter
            # right col style is right aligned
            rightColStyle = ParagraphStyle(
                name="leftColLevel%d" % level,
                parent=leftColStyle,
                leftIndent=0,
                alignment=TA_RIGHT,
            )
            leftPara = Paragraph(text, leftColStyle)
            rightPara = Paragraph(str(pageNum), rightColStyle)
            tableData.append([leftPara, rightPara])

        self._table = Table(tableData, colWidths=widths, style=TableStyle(tableStyle))

        self.width, self.height = self._table.wrapOn(self.canv, availWidth, availHeight)
        return self.width, self.height


class PmlRightPageBreak(CondPageBreak):
    def __init__(self) -> None:
        pass

    def wrap(self, availWidth, availHeight):
        if not self.canv.getPageNumber() % 2:
            self.width = availWidth
            self.height = availHeight
            return availWidth, availHeight
        self.width = self.height = 0
        return 0, 0


class PmlLeftPageBreak(CondPageBreak):
    def __init__(self) -> None:
        pass

    def wrap(self, availWidth, availHeight):
        if self.canv.getPageNumber() % 2:
            self.width = availWidth
            self.height = availHeight
            return availWidth, availHeight
        self.width = self.height = 0
        return 0, 0


# --- Pdf Form


class PmlInput(Flowable):
    def __init__(
        self,
        name,
        input_type="text",
        width=10,
        height=10,
        default="",
        options=None,
        multiline=0,
    ) -> None:
        self.width = width
        self.height = height
        self.type = input_type
        self.name = name
        self.default = default
        self.options = options if options is not None else []
        self.multiline = multiline

    def wrap(self, *args):
        return self.width, self.height

    def draw(self):
        c = self.canv

        c.saveState()
        c.setFont("Helvetica", 10)
        if self.type == "text":
            pdfform.textFieldRelative(
                c, self.name, 0, 0, self.width, self.height, multiline=self.multiline
            )
            c.rect(0, 0, self.width, self.height)
        elif self.type == "radio":
            c.rect(0, 0, self.width, self.height)
        elif self.type == "checkbox":
            if self.default:
                pdfform.buttonFieldRelative(c, self.name, "Yes", 0, 0)
            else:
                pdfform.buttonFieldRelative(c, self.name, "Off", 0, 0)
            c.rect(0, 0, self.width, self.height)
        elif self.type == "select":
            pdfform.selectFieldRelative(
                c, self.name, self.default, self.options, 0, 0, self.width, self.height
            )
            c.rect(0, 0, self.width, self.height)

        c.restoreState()
