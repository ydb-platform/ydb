# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

__all__ = ("PdfObject", "PdfImage", "PdfTextObj", "PdfFont")

import ctypes
from ctypes import c_uint, c_float
import logging
from pathlib import Path
from collections import namedtuple
import pypdfium2.raw as pdfium_c
import pypdfium2.internal as pdfium_i
from pypdfium2._helpers.misc import PdfiumError
from pypdfium2._helpers.matrix import PdfMatrix
from pypdfium2._helpers.bitmap import PdfBitmap
from pypdfium2._lazy import Lazy

logger = logging.getLogger(__name__)


class PdfObject (pdfium_i.AutoCloseable):
    """
    Pageobject helper class.
    
    When constructing a :class:`.PdfObject`, an instance of a more specific subclass may be returned instead, depending on the object's :attr:`.type` (e.g. :class:`.PdfImage`, :class:`.PdfTextObj`).
    
    Note:
        :meth:`.PdfObject.close` only takes effect on loose pageobjects.
        It is a no-op otherwise, because pageobjects that are part of a page are owned by pdfium, not the caller.
    
    Attributes:
        raw (FPDF_PAGEOBJECT):
            The underlying PDFium pageobject handle.
        type (int):
            The object's type (:data:`FPDF_PAGEOBJ_*`).
        page (PdfPage):
            Reference to the page this pageobject belongs to. May be None if not part of a page (e.g. new or detached object).
        pdf (PdfDocument):
            Reference to the document this pageobject belongs to. May be None if the object does not belong to a document yet.
            This attribute is always set if :attr:`.page` is set.
        container (PdfObject | None):
            PdfObject handle to parent Form XObject, if the pageobject is nested in a Form XObject, None otherwise.
        level (int):
            Nesting level signifying the number of parent Form XObjects, at the time of construction.
            Zero if the object is not nested in a Form XObject.
    """
    
    def __new__(cls, raw, *args, **kwargs):
        
        type = pdfium_c.FPDFPageObj_GetType(raw)
        if type == pdfium_c.FPDF_PAGEOBJ_IMAGE:
            instance = super().__new__(PdfImage)
        elif type == pdfium_c.FPDF_PAGEOBJ_TEXT:
            instance = super().__new__(PdfTextObj)
        else:
            instance = super().__new__(PdfObject)
        
        instance.type = type
        return instance
    
    
    def __init__(self, raw, page=None, pdf=None, container=None, level=0):
        
        self.raw = raw
        self.page = page
        self.pdf = pdf
        self.container = container
        self.level = level
        
        if page is not None:
            if self.pdf is None:
                self.pdf = page.pdf
            elif self.pdf is not page.pdf:
                raise ValueError("*page* must belong to *pdf* when constructing a pageobject.")
        
        # TODO if page is not None, hold it in the finalizer, unless the pageobject is detached from the page
        super().__init__(pdfium_c.FPDFPageObj_Destroy, needs_free=(page is None))
    
    
    @property
    def parent(self):  # AutoCloseable hook
        # May be None (loose pageobject)
        return self.pdf if self.page is None else self.page
    
    
    def get_bounds(self):
        """
        Get the bounds of the object on the page.
        
        Returns:
            tuple[float * 4]: Left, bottom, right and top, in PDF page coordinates.
        """
        if self.page is None:
            raise RuntimeError("Must not call get_bounds() on a loose pageobject.")
        
        l, b, r, t = c_float(), c_float(), c_float(), c_float()
        ok = pdfium_c.FPDFPageObj_GetBounds(self, l, b, r, t)
        if not ok:
            raise PdfiumError("Failed to locate pageobject.")
        
        return (l.value, b.value, r.value, t.value)
    
    
    def get_quad_points(self):
        """
        Get the object's quadriliteral points (i.e. the positions of its corners).
        For transformed objects, this may provide tighter bounds than a rectangle (e.g. rotation by a non-multiple of 90°, shear).
        
        Note:
            This function only supports image and text objects.
        
        Returns:
            tuple[tuple[float*2] * 4]: Corner positions as (x, y) tuples, counter-clockwise from origin, i.e. bottom-left, bottom-right, top-right, top-left, in PDF page coordinates.
        """
        
        if self.type not in (pdfium_c.FPDF_PAGEOBJ_IMAGE, pdfium_c.FPDF_PAGEOBJ_TEXT):
            # as of pdfium 5921
            raise RuntimeError("Quad points only supported for image and text objects.")
        
        q = pdfium_c.FS_QUADPOINTSF()
        ok = pdfium_c.FPDFPageObj_GetRotatedBounds(self, q)
        if not ok:
            raise PdfiumError("Failed to get quad points.")
        
        return (q.x1, q.y1), (q.x2, q.y2), (q.x3, q.y3), (q.x4, q.y4)
    
    
    def get_matrix(self):
        """
        Returns:
            PdfMatrix: The pageobject's current transform matrix.
        """
        fs_matrix = pdfium_c.FS_MATRIX()
        ok = pdfium_c.FPDFPageObj_GetMatrix(self, fs_matrix)
        if not ok:
            raise PdfiumError("Failed to get matrix of pageobject.")
        return PdfMatrix.from_raw(fs_matrix)
    
    
    def set_matrix(self, matrix):
        """
        Parameters:
            matrix (PdfMatrix): Set this matrix as the pageobject's transform matrix.
        """
        ok = pdfium_c.FPDFPageObj_SetMatrix(self, matrix)
        if not ok:
            raise PdfiumError("Failed to set matrix of pageobject.")
    
    
    def transform(self, matrix):
        """
        Parameters:
            matrix (PdfMatrix): Multiply the pageobject's current transform matrix by this matrix.
        """
        ok = pdfium_c.FPDFPageObj_TransformF(self, matrix)
        if not ok:
            raise PdfiumError("Failed to transform pageobject with matrix.")


class PdfTextObj (PdfObject):
    """
    Textobject helper class.
    
    You may want to call :meth:`.PdfPage.get_objects` or :meth:`.PdfTextPage.get_textobj` to obtain an instance of this class.
    """
    
    # TODO hold parent object in finalizer
    def __init__(self, *args, textpage=None, **kwargs):
        if textpage is not None:
            kwargs.update(page=textpage.page, pdf=textpage.page.pdf)
        super().__init__(*args, **kwargs)
        self.textpage = textpage
    
    def extract(self):
        """
        Returns:
            str: The objects's text content.
        """
        bufsize = pdfium_c.FPDFTextObj_GetText(self, self.textpage, None, 0)
        if bufsize == 0:
            raise PdfiumError("Failed to get text from textobject.")
        
        buffer = ctypes.create_string_buffer(bufsize)
        buffer_ptr = ctypes.cast(buffer, ctypes.POINTER(pdfium_c.FPDF_WCHAR))
        pdfium_c.FPDFTextObj_GetText(self, self.textpage, buffer_ptr, bufsize)
        
        return buffer.raw[:bufsize-2].decode("utf-16-le")
    
    def get_font(self):
        """
        Returns:
            PdfFont: Handle to the object's font. Provides name and weight info.
        """
        # The font object is _not_ owned by the caller, and the PdfTextObj must remain alive while the font object lives.
        raw_font = pdfium_c.FPDFTextObj_GetFont(self)
        return PdfFont(raw_font, self)
    
    def get_font_size(self):
        """
        Returns:
            float: Font size used by the object's text, in PDF canvas units (typically 1/72in).
        """
        r_size = ctypes.c_float()
        ok = pdfium_c.FPDFTextObj_GetFontSize(self, r_size)
        if not ok:
            raise PdfiumError("Failed to get font size.")
        return r_size.value


class PdfFont (pdfium_i.AutoCastable):
    """
    Font helper class.
    """
    
    # TODO hold parent in finalizer
    def __init__(self, raw, parent=None):
        self.raw = raw
        self.parent = parent
    
    def _get_name_impl(self, api, which):
        
        bufsize = api(self, None, 0)
        if bufsize == 0:
            raise PdfiumError(f"Failed to get font {which} name.")
        
        buffer = ctypes.create_string_buffer(bufsize)
        api(self, buffer, bufsize)
        
        return buffer.value.decode("utf-8")
    
    def get_base_name(self):
        """
        Returns:
            str: The base font name.
        """
        return self._get_name_impl(pdfium_c.FPDFFont_GetBaseFontName, "base")
    
    def get_family_name(self):
        """
        Returns:
            str: The font family name.
        """
        return self._get_name_impl(pdfium_c.FPDFFont_GetFamilyName, "family")
    
    def get_weight(self):
        """
        Returns:
            int: The font's weight. Typical values are 400 (normal) and 700 (bold).
        """
        weight = pdfium_c.FPDFFont_GetWeight(self)
        if weight == -1:
            raise PdfiumError("Failed to get font weight.")
        return weight


class PdfImage (PdfObject):
    """
    Image object helper class (specific kind of pageobject).
    """
    
    # cf. https://crbug.com/pdfium/1203
    #: Filters applied by :func:`FPDFImageObj_GetImageDataDecoded`, referred to as "simple filters". Other filters are considered "complex filters".
    SIMPLE_FILTERS = ("ASCIIHexDecode", "ASCII85Decode", "RunLengthDecode", "FlateDecode", "LZWDecode")
    
    
    @classmethod
    def new(cls, pdf):
        """
        Parameters:
            pdf (PdfDocument): The document to which the new image object shall be added.
        Returns:
            PdfImage: Handle to a new, empty image.
            Note that position and size of the image are defined by its matrix, which defaults to the identity matrix.
            This means that new images will appear as a tiny square of 1x1 canvas units on the bottom left corner of the page.
            Use :class:`.PdfMatrix` and :meth:`.set_matrix` to adjust size and position.
        """
        raw_img = pdfium_c.FPDFPageObj_NewImageObj(pdf)
        return cls(raw_img, page=None, pdf=pdf)
    
    
    def get_metadata(self):
        """
        Retrieve image metadata including DPI, bits per pixel, color space, and size.
        If the image does not belong to a page yet, bits per pixel and color space will be unset (0).
        
        Note:
            * The DPI values signify the resolution of the image on the PDF page, not the DPI metadata embedded in the image file.
            * Due to issues in pdfium, this function might be slow on some kinds of images. If you only need size, prefer :meth:`.get_px_size` instead.
        
        Returns:
            FPDF_IMAGEOBJ_METADATA: Image metadata structure
        """
        # https://crbug.com/pdfium/1928
        metadata = pdfium_c.FPDF_IMAGEOBJ_METADATA()
        ok = pdfium_c.FPDFImageObj_GetImageMetadata(self, self.page, metadata)
        if not ok:
            raise PdfiumError("Failed to get image metadata.")
        return metadata
    
    
    def get_px_size(self):
        """
        Returns:
            (int, int): Image dimensions as a tuple of (width, height).
        """
        # https://pdfium-review.googlesource.com/c/pdfium/+/106290
        w, h = c_uint(), c_uint()
        ok = pdfium_c.FPDFImageObj_GetImagePixelSize(self, w, h)
        if not ok:
            raise PdfiumError("Failed to get image size.")
        return w.value, h.value
    
    
    def load_jpeg(self, source, pages=None, inline=False, autoclose=True):
        """
        Set a JPEG as the image object's content.
        
        Parameters:
            source (str | pathlib.Path | typing.BinaryIO):
                Input JPEG, given as file path or readable byte stream.
            pages (list[PdfPage] | None):
                If replacing an image, pass in a list of loaded pages that might contain it, to update their cache.
                (The same image may be shown multiple times in different transforms across a PDF.)
                May be None or an empty sequence if the image is not shared.
            inline (bool):
                Whether to load the image content into memory. If True, the buffer may be closed after this function call.
                Otherwise, the buffer needs to remain open until the PDF is closed.
            autoclose (bool):
                If the input is a buffer, whether it should be automatically closed once not needed by the PDF anymore.
        """
        
        if isinstance(source, (str, Path)):
            buffer = open(source, "rb")
            autoclose = True
        elif pdfium_i.is_stream(source, "r"):
            buffer = source
        else:
            raise ValueError(f"Cannot load JPEG from {source} - not a file path or byte stream.")
        
        bufaccess, to_hold = pdfium_i.get_bufreader(buffer)
        loader = pdfium_c.FPDFImageObj_LoadJpegFileInline if inline else \
                 pdfium_c.FPDFImageObj_LoadJpegFile
        
        c_pages, page_count = pdfium_i.pages_c_array(pages)
        ok = loader(c_pages, page_count, self, bufaccess)
        if not ok:
            raise PdfiumError("Failed to load JPEG into image object.")
        
        if inline:
            for data in to_hold:
                id(data)
            if autoclose:
                buffer.close()
        else:
            self.pdf._data_holder += to_hold
            if autoclose:
                self.pdf._data_closer.append(buffer)
    
    
    def set_bitmap(self, bitmap, pages=None):
        """
        Set a bitmap as the image object's content.
        The pixel data will be flate compressed (as of PDFium 5418).
        
        Parameters:
            bitmap (PdfBitmap):
                The bitmap to inject into the image object.
            pages (list[PdfPage] | None):
                A list of loaded pages that might contain the image object. See :meth:`.load_jpeg`.
        """
        c_pages, page_count = pdfium_i.pages_c_array(pages)
        ok = pdfium_c.FPDFImageObj_SetBitmap(c_pages, page_count, self, bitmap)
        if not ok:
            raise PdfiumError("Failed to set image to bitmap.")
    
    
    def _get_rendered_bitmap(self, scale_to_original):
        """ This is a private implementation function. Do not use externally. """
        
        if self.pdf is None:
            raise RuntimeError("Cannot get rendered bitmap of loose pageobject.")
            
        if scale_to_original:
            # Suggested by pdfium dev Lei Zhang in https://groups.google.com/g/pdfium/c/2czGFBcWHHQ/m/g0wzOJR-BAAJ
            
            px_w, px_h = self.get_px_size()
            l, b, r, t = self.get_bounds()
            content_w, content_h = abs(r-l), abs(t-b)
            
            # align pixel and content width/height relation if swapped due to rotation (e.g. 90°, 270°)
            swap = (px_w < px_h) != (content_w < content_h)
            if swap:
                px_w, px_h = px_h, px_w
            
            # if the image is squashed/stretched, prefer partial upscaling over partial downscaling (not using separate x/y scaling, so the image will look as in the PDF)
            scale_factor = max(px_w/content_w, px_h/content_h)
            orig_mat = self.get_matrix()
            scaled_mat = orig_mat.scale(scale_factor, scale_factor)
            self.set_matrix(scaled_mat)
            # logger.debug(
            #     f"Pixel size: {px_w}, {px_h} (did swap? {swap})\n"
            #     f"Size in page coords: {content_w}, {content_h}\n"
            #     f"Scale: {scale_factor}\n"
            #     f"Current matrix: {orig_mat}\n"
            #     f"Scaled matrix: {scaled_mat}"
            # )
        
        try:
            raw_bitmap = pdfium_c.FPDFImageObj_GetRenderedBitmap(self.pdf, self.page, self)
        finally:
            if scale_to_original:
                self.set_matrix(orig_mat)
        
        return raw_bitmap
    
    
    def get_bitmap(self, render=False, scale_to_original=True):
        """
        Get a bitmap rasterization of the image.
        
        Parameters:
            render (bool):
                Whether the image should be rendered, thereby applying possible transform matrices and alpha masks.
            scale_to_original (bool):
                If *render* is True, whether to temporarily scale the image to its native resolution, or close to that (defaults to True). This should improve output quality. Ignored if *render* is False.
        Returns:
            PdfBitmap: Image bitmap (with a buffer allocated by PDFium).
        """
        
        if render:
            raw_bitmap = self._get_rendered_bitmap(scale_to_original)
        else:
            raw_bitmap = pdfium_c.FPDFImageObj_GetBitmap(self)
        
        if not raw_bitmap:
            raise PdfiumError(f"Failed to get bitmap of image {self}.")
        
        bitmap = PdfBitmap.from_raw(raw_bitmap)
        if render and scale_to_original:
            logger.debug(f"Extracted size: {bitmap.width}, {bitmap.height}")
        
        return bitmap
    
    
    def get_data(self, decode_simple=False):
        """
        Parameters:
            decode_simple (bool):
                If True, decode simple filters (see :attr:`.SIMPLE_FILTERS`), so only complex filters will remain, if any. If there are no complex filters, this provides the decoded pixel data.
                If False, the raw stream data will be returned instead.
        Returns:
            ctypes.Array: The data of the image stream (as :class:`~ctypes.c_ubyte` array).
        """
        func = pdfium_c.FPDFImageObj_GetImageDataDecoded if decode_simple else \
               pdfium_c.FPDFImageObj_GetImageDataRaw
        n_bytes = func(self, None, 0)
        buffer = (ctypes.c_ubyte * n_bytes)()
        func(self, buffer, n_bytes)
        return buffer
    
    
    def get_filters(self, skip_simple=False):
        """
        Parameters:
            skip_simple (bool):
                If True, exclude simple filters.
        Returns:
            list[str]: A list of image filters, to be applied in order (from lowest to highest index).
        """
        
        filters = []
        count = pdfium_c.FPDFImageObj_GetImageFilterCount(self)
        
        for i in range(count):
            length = pdfium_c.FPDFImageObj_GetImageFilter(self, i, None, 0)
            buffer = ctypes.create_string_buffer(length)
            pdfium_c.FPDFImageObj_GetImageFilter(self, i, buffer, length)
            f = buffer.value.decode("utf-8")
            filters.append(f)
        
        if skip_simple:
            filters = [f for f in filters if f not in self.SIMPLE_FILTERS]
        
        return filters
    
    
    def extract(self, dest, *args, **kwargs):
        """
        Extract the image into an independently usable file or byte stream, attempting to avoid re-encoding or quality loss, as far as pdfium's limited API permits.
        
        This method can only extract DCTDecode (JPEG) and JPXDecode (JPEG 2000) images directly.
        Otherwise, the pixel data is decoded and re-encoded using :mod:`PIL`, which is slower and loses the original encoding.
        For images with simple filters only, ``get_data(decode_simple=True)`` is used to preserve higher bit depth or special color formats not supported by ``FPDF_BITMAP``.
        For images with complex filters other than those extracted directly, we have to resort to :meth:`.get_bitmap`.
        
        Note, this method is not able to account for alpha masks, and potentially other data stored separately of the main image stream, which might lead to incorrect representation of the image.
        
        Tip:
            The ``pikepdf`` library is capable of preserving the original encoding in many cases where this method is not.
        
        Parameters:
            dest (str | pathlib.Path | io.BytesIO):
                File path prefix or byte stream to which the image shall be written.
            fb_format (str):
                The image format to use in case it is necessary to (re-)encode the data.
        """
        
        # https://crbug.com/pdfium/1930
        
        extraction_gen = _extract_smart(self, *args, **kwargs)
        format = next(extraction_gen)
        
        if isinstance(dest, (str, Path)):
            with open(f"{dest}.{format}", "wb") as buf:
                extraction_gen.send(buf)
        elif pdfium_i.is_stream(dest, "w"):
            extraction_gen.send(dest)
        else:
            raise ValueError(f"Cannot extract to '{dest}'")


_ImageInfo = namedtuple("_ImageInfo", "format mode metadata all_filters complex_filters")


class _ImageExtractionError (Exception):
    pass


def _get_pil_mode(cs, bpp):
    # As of Jan 2025, pdfium does not provide access to the palette, so we cannot handle indexed (palettized) color space.
    # TODO handle ICC-based color spaces (pdfium now provides access to the ICC profile via FPDFImageObj_GetIccProfileDataDecoded(), see commit edd7c5cf)
    if cs == pdfium_c.FPDF_COLORSPACE_DEVICEGRAY:
        return "1" if bpp == 1 else "L"
    elif cs == pdfium_c.FPDF_COLORSPACE_DEVICERGB:
        return "RGB"
    elif cs == pdfium_c.FPDF_COLORSPACE_DEVICECMYK:
        return "CMYK"
    else:
        return None


def _extract_smart(image_obj, fb_format=None):
    
    try:
        # TODO can we change PdfImage.get_data() to take an mmap, so the data could be written directly into a file rather than an in-memory array?
        data, info = _extract_direct(image_obj)
    except _ImageExtractionError as e:
        logger.debug(str(e))
        pil_image = image_obj.get_bitmap(render=False).to_pil()
    else:
        pil_image = None
        format = info.format
        if format == "raw":
            metadata = info.metadata
            pil_image = Lazy.PIL_Image.frombuffer(
                info.mode,
                (metadata.width, metadata.height),
                image_obj.get_data(decode_simple=True),
                "raw", info.mode, 0, 1,
            )
    
    if pil_image:
        format = fb_format
        if not format:
            format = "tiff" if pil_image.mode == "CMYK" else "png"
    
    buffer = yield format
    if pil_image:
        pil_image.save(buffer, format=format)
    else:
        buffer.write(data)
    
    yield  # breakpoint preventing StopIteration on .send()


def _extract_direct(image_obj):
    
    all_filters = image_obj.get_filters()
    complex_filters = [f for f in all_filters if f not in PdfImage.SIMPLE_FILTERS]
    metadata = image_obj.get_metadata()
    mode = _get_pil_mode(metadata.colorspace, metadata.bits_per_pixel)
    
    if len(complex_filters) == 0:
        if mode:
            out_data = image_obj.get_data(decode_simple=True)
            out_format = "raw"
        else:
            raise _ImageExtractionError(f"Unhandled color space {pdfium_i.ColorspaceToStr.get(metadata.colorspace)} - don't know how to treat data.")
    elif len(complex_filters) == 1:
        f = complex_filters[0]
        if f == "DCTDecode":
            out_data = image_obj.get_data(decode_simple=True)
            out_format = "jpg"
        elif f == "JPXDecode":
            out_data = image_obj.get_data(decode_simple=True)
            out_format = "jp2"
        else:
            raise _ImageExtractionError(f"Unhandled complex filter {f}.")
    else:
        raise _ImageExtractionError(f"Cannot handle multiple complex filters {complex_filters}.")
    
    info = _ImageInfo(out_format, mode, metadata, all_filters, complex_filters)
    return out_data, info
