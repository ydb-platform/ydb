from __future__ import annotations

from typing import TYPE_CHECKING, Iterator, cast

import pypdf
from PIL import Image
from reportlab.pdfgen.canvas import Canvas

from xhtml2pdf.files import getFile, pisaFileObject

if TYPE_CHECKING:
    from io import BytesIO

    from xhtml2pdf.context import pisaContext


class WaterMarks:
    @staticmethod
    def get_size_location(
        img, context: dict, pagesize: tuple[int, int], *, is_portrait: bool
    ) -> tuple[int, int, int, int]:
        object_position: tuple[int, int] | None = context.get("object_position")
        cssheight: int | None = cast(int, context.get("height"))
        csswidth: int = cast(int, context.get("width"))
        iw, ih = img.getSize()
        pw, ph = pagesize
        width: int = pw  # min(iw, pw) # max
        wfactor: float = float(width) / iw
        height: int = ph  # min(ih, ph) # max
        hfactor: float = float(height) / ih
        factor_min: float = min(wfactor, hfactor)
        factor_max: float = max(wfactor, hfactor)
        if is_portrait:
            height = ih * factor_min
            width = iw * factor_min
        else:
            height = ih * factor_max
            width = iw * factor_min

        if object_position:
            # x, y, width=None, height=None
            x, y = object_position
        elif is_portrait:
            x, y = 0, ph - height
        else:
            x, y = 0, 0
        if csswidth:
            width = csswidth
        if cssheight:
            height = cssheight

        return x, y, width, height

    @staticmethod
    def get_img_with_opacity(pisafile: pisaFileObject, context: dict) -> BytesIO:
        opacity: float = context.get("opacity", None)
        if opacity:
            name: str | None = pisafile.getNamedFile()
            img: Image.Image = Image.open(name)
            img = img.convert("RGBA")
            img.putalpha(int(255 * opacity))
            img.save(name, "PNG")
            return getFile(name).getBytesIO()
        return pisafile.getBytesIO()

    @staticmethod
    def generate_pdf_background(
        pisafile: pisaFileObject,
        pagesize: tuple[int, int],
        *,
        is_portrait: bool,
        context: dict | None = None,
    ) -> pisaFileObject:
        """
        Pypdf requires pdf as background so convert image to pdf in temporary file with same page dimensions
        :param pisafile:  Image File
        :param pagesize:  Page size for the new pdf
        """
        # don't move up, we are preventing circular import
        from xhtml2pdf.xhtml2pdf_reportlab import PmlImageReader

        if context is None:
            context = {}

        output: pisaFileObject = pisaFileObject(
            None, "application/pdf"
        )  # build temporary file
        img: PmlImageReader = PmlImageReader(
            WaterMarks.get_img_with_opacity(pisafile, context)
        )
        x, y, width, height = WaterMarks.get_size_location(
            img, context, pagesize, is_portrait=is_portrait
        )

        canvas = Canvas(output.getNamedFile(), pagesize=pagesize)
        canvas.drawImage(img, x, y, width, height, mask="auto")

        canvas.save()

        return output

    @staticmethod
    def get_watermark(context: pisaContext, max_numpage: int) -> Iterator:
        if context.pisaBackgroundList:
            pages = [x[0] for x in context.pisaBackgroundList] + [max_numpage + 1]
            pages.pop(0)
            for counter, (page, bgfile, pgcontext) in enumerate(
                context.pisaBackgroundList
            ):
                if not bgfile.notFound():
                    yield range(page, pages[counter]), bgfile, int(pgcontext["step"])

    @staticmethod
    def process_doc(
        context: pisaContext, istream: bytes, output: bytes
    ) -> tuple[bytes, bool]:
        pdfoutput: pypdf.PdfWriter = pypdf.PdfWriter()
        input1: pypdf.PdfReader = pypdf.PdfReader(istream)
        has_bg: bool = False
        for pages, bgouter, step in WaterMarks.get_watermark(
            context, len(input1.pages)
        ):
            bginput: pypdf.PdfReader = pypdf.PdfReader(bgouter.getBytesIO())
            pagebg: pypdf.PageObject = bginput.pages[0]
            for index, ctr in enumerate(pages):
                page: pypdf.PageObject = input1.pages[ctr - 1]
                if index % step == 0:
                    page.merge_page(pagebg, over=False)
                pdfoutput.add_page(page)
                has_bg = True
        if has_bg:
            pdfoutput.write(output)

        return output, has_bg
