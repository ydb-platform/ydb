# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

__all__ = ("PdfBitmap", "PdfPosConv")

import ctypes
import logging
import pypdfium2.raw as pdfium_c
import pypdfium2.internal as pdfium_i
from pypdfium2._helpers.misc import PdfiumError
from pypdfium2._lazy import Lazy
from pypdfium2.version import PDFIUM_INFO

logger = logging.getLogger(__name__)


class PdfBitmap (pdfium_i.AutoCloseable):
    """
    Bitmap helper class.
    
    .. _PIL Modes: https://pillow.readthedocs.io/en/stable/handbook/concepts.html#concept-modes
    
    Warning:
        ``bitmap.close()``, which frees the buffer of foreign bitmaps, is not validated for safety.
        A bitmap must not be closed while other objects still depend on its buffer!
    
    Attributes:
        raw (FPDF_BITMAP):
            The underlying PDFium bitmap handle.
        buffer (~ctypes.c_ubyte):
            A ctypes array representation of the pixel data (each item is an unsigned byte, i. e. a number ranging from 0 to 255).
        width (int):
            Width of the bitmap (horizontal size).
        height (int):
            Height of the bitmap (vertical size).
        stride (int):
            Number of bytes per line in the bitmap buffer.
            Depending on how the bitmap was created, there may be a padding of unused bytes at the end of each line, so this value can be greater than ``width * n_channels``.
        format (int):
            PDFium bitmap format constant (:attr:`FPDFBitmap_*`)
        rev_byteorder (bool):
            Whether the bitmap is using reverse byte order.
        n_channels (int):
            Number of channels per pixel.
        mode (str):
            The bitmap format as string (see `PIL Modes`_).
    """
    
    def __init__(self, raw, buffer, width, height, stride, format, rev_byteorder, needs_free):
        
        self.raw = raw
        self.buffer = buffer
        self.width = width
        self.height = height
        self.stride = stride
        self.format = format
        self.rev_byteorder = rev_byteorder
        self.n_channels = pdfium_i.BitmapTypeToNChannels[self.format]
        self.mode = (
            pdfium_i.BitmapTypeToStrReverse if self.rev_byteorder else \
            pdfium_i.BitmapTypeToStr
        )[self.format]
        
        # slot to store arguments for PdfPosConv, set on page rendering
        self._pos_args = None
        
        super().__init__(pdfium_c.FPDFBitmap_Destroy, needs_free=needs_free, obj=self.buffer)
    
    
    @property
    def parent(self):  # AutoCloseable hook
        return None
    
    
    # NOTE To test all bitmap creation strategies through the CLI:
    # MAKERS=(native foreign foreign_packed foreign_simple)
    # DOCPATH="..."  # ideally use a short one or pass e.g. --pages "1-3"
    # for MAKER in ${MAKERS[@]}; do echo "$MAKER"; mkdir -p out/$MAKER; pypdfium2 render "$DOCPATH" -o out/$MAKER --bitmap-maker $MAKER; done
    
    # To test .from_raw():
    # pypdfium2 extract-images "$DOCPATH" -o out/ --use-bitmap
    
    
    @staticmethod
    def _get_buffer(raw, stride, height):
        buffer_ptr = pdfium_c.FPDFBitmap_GetBuffer(raw)
        if not buffer_ptr:
            raise PdfiumError("Failed to get bitmap buffer (null pointer returned)")
        buffer_ptr = ctypes.cast(buffer_ptr, ctypes.POINTER(ctypes.c_ubyte))
        return pdfium_i.get_buffer(buffer_ptr, stride*height)
    
    
    @classmethod
    def from_raw(cls, raw, rev_byteorder=False, ex_buffer=None):
        """
        Construct a :class:`.PdfBitmap` wrapper around a raw PDFium bitmap handle.
        
        Note:
            This method is primarily meant for bitmaps provided by pdfium (as in :meth:`.PdfImage.get_bitmap`). For bitmaps created by the caller, where the parameters are already known, it may be preferable to call the :class:`.PdfBitmap` constructor directly.
        
        Parameters:
            raw (FPDF_BITMAP):
                PDFium bitmap handle.
            rev_byteorder (bool):
                Whether the bitmap uses reverse byte order.
            ex_buffer (~ctypes.c_ubyte | None):
                If the bitmap was created from a buffer allocated by Python/ctypes, pass in the ctypes array to keep it referenced.
        """
        
        width = pdfium_c.FPDFBitmap_GetWidth(raw)
        height = pdfium_c.FPDFBitmap_GetHeight(raw)
        stride = pdfium_c.FPDFBitmap_GetStride(raw)
        format = pdfium_c.FPDFBitmap_GetFormat(raw)
        
        if ex_buffer is None:
            needs_free, buffer = True, cls._get_buffer(raw, stride, height)
        else:
            needs_free, buffer = False, ex_buffer
        
        return cls(raw, buffer, width, height, stride, format, rev_byteorder, needs_free)
    
    
    @classmethod
    def new_native(cls, width, height, format, rev_byteorder=False, buffer=None, stride=None):
        """
        Create a new bitmap using :func:`FPDFBitmap_CreateEx`, with a buffer allocated by Python/ctypes, or provided by the caller.
        
        * If buffer and stride are None, a packed buffer is created.
        * If a custom buffer is given but no stride, the buffer is assumed to be packed.
        * If a custom stride is given but no buffer, a stride-agnostic buffer is created.
        * If both custom buffer and stride are given, they are used as-is.
        
        Caller-provided buffer/stride are subject to a logical validation.
        """
        
        bpc = pdfium_i.BitmapTypeToNChannels[format]
        if stride is None:
            stride = width * bpc
        else:
            assert stride >= width * bpc
        
        if buffer is None:
            buffer = (ctypes.c_ubyte * (stride * height))()
        else:
            assert len(buffer) >= stride * height
        
        raw = pdfium_c.FPDFBitmap_CreateEx(width, height, format, buffer, stride)
        return cls(raw, buffer, width, height, stride, format, rev_byteorder, needs_free=False)
        
        # Alternatively, we could do:
        # return cls.from_raw(raw, rev_byteorder, buffer)
        # This implies some (technically unnecessary) API calls. Note, for a short time, there was a bug in pdfium where retrieving the params of a caller-created bitmap through the FPDFBitmap_Get*() APIs didn't work correctly, so better avoid doing this if we can help it.
    
    
    @classmethod
    def new_foreign(cls, width, height, format, rev_byteorder=False, force_packed=False):
        """
        Create a new bitmap using :func:`FPDFBitmap_CreateEx`, with a buffer allocated by PDFium.
        There may be a padding of unused bytes at line end, unless *force_packed=True* is given.
        
        Note, the recommended default bitmap creation strategy is :meth:`.new_native`.
        """
        stride = width * pdfium_i.BitmapTypeToNChannels[format] if force_packed else 0
        raw = pdfium_c.FPDFBitmap_CreateEx(width, height, format, None, stride)
        # Retrieve stride set by pdfium, if we passed in 0. Otherwise, trust in pdfium to use the requested stride.
        if not force_packed:  # stride == 0
            stride = pdfium_c.FPDFBitmap_GetStride(raw)
        buffer = cls._get_buffer(raw, stride, height)
        return cls(raw, buffer, width, height, stride, format, rev_byteorder, needs_free=True)
    
    
    @classmethod
    def new_foreign_simple(cls, width, height, use_alpha, rev_byteorder=False):
        """
        Create a new bitmap using :func:`FPDFBitmap_Create`. The buffer is allocated by PDFium. 
        
        PDFium docs specify that each line uses width * 4 bytes, with no gap between adjacent lines, i.e. the resulting buffer should be packed.
        
        Contrary to the other ``PdfBitmap.new_*()`` methods, this method does not take a format constant, but a *use_alpha* boolean. If True, the format will be :attr:`FPDFBitmap_BGRA`, :attr:`FPFBitmap_BGRx` otherwise. Other bitmap formats cannot be used with this method.
        
        Note, the recommended default bitmap creation strategy is :meth:`.new_native`.
        """
        raw = pdfium_c.FPDFBitmap_Create(width, height, use_alpha)
        stride = width * 4  # see above
        buffer = cls._get_buffer(raw, stride, height)
        format = pdfium_c.FPDFBitmap_BGRA if use_alpha else pdfium_c.FPDFBitmap_BGRx
        return cls(raw, buffer, width, height, stride, format, rev_byteorder, needs_free=True)
    
    
    def fill_rect(self, color, left, top, width, height):
        """
        Fill a rectangle on the bitmap with the given color.
        The coordinate system's origin is the top left corner of the image.
        
        Note:
            This function replaces the color values in the given rectangle. It does not perform alpha compositing.
        
        Parameters:
            color (tuple[int, int, int, int]):
                RGBA fill color (a tuple of 4 integers ranging from 0 to 255).
        """
        c_color = pdfium_i.color_tohex(color, self.rev_byteorder)
        ok = pdfium_c.FPDFBitmap_FillRect(self, left, top, width, height, c_color)
        if not ok and PDFIUM_INFO.build >= 6635:
            raise PdfiumError("Failed to fill bitmap rectangle.")
    
    
    # Requirement: If the result is a view of the buffer (not a copy), it keeps the referenced memory valid.
    # 
    # Note that memory management differs between native and foreign bitmap buffers:
    # - With native bitmaps, the memory is allocated by python on creation of the buffer object (transparent).
    # - With foreign bitmaps, the buffer object is merely a view of memory allocated by pdfium and will be freed by finalizer (opaque).
    # 
    # It is necessary that receivers correctly handle both cases, e.g. by keeping the buffer object itself alive.
    # As of May 2023, this seems to hold true for NumPy and PIL. New converters should be carefully tested.
    # 
    # We could consider attaching a buffer keep-alive finalizer to any converted objects referencing the buffer,
    # but then we'd have to rely on third parties to actually create a reference at all times, otherwise we would unnecessarily delay releasing memory.
    
    
    def to_numpy(self):
        """
        Get a :mod:`numpy` array view of the bitmap.
        
        The array contains as many rows as the bitmap is high.
        Each row contains as many pixels as the bitmap is wide.
        Each pixel will be an array holding the channel values, or just a value if there is only one channel (see :attr:`.n_channels` and :attr:`.format`).
        
        The resulting array is supposed to share memory with the original bitmap buffer,
        so changes to the buffer should be reflected in the array, and vice versa.
        
        Returns:
            numpy.ndarray: NumPy array (representation of the bitmap buffer).
        """
        
        # https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray
        
        array = Lazy.numpy.ndarray(
            # layout: row major
            shape = (self.height, self.width, self.n_channels) if self.n_channels > 1 else (self.height, self.width),
            dtype = ctypes.c_ubyte,
            buffer = self.buffer,
            # number of bytes per item for each nesting level (outer->inner: row, pixel, value - or row, value for a single-channel bitmap)
            strides = (self.stride, self.n_channels, 1) if self.n_channels > 1 else (self.stride, 1),
        )
        
        return array
    
    
    def to_pil(self):
        """
        Get a :mod:`PIL` image of the bitmap, using :func:`PIL.Image.frombuffer`.
        
        For ``RGBA``, ``RGBX`` and ``L`` bitmaps, PIL is supposed to share memory with
        the original buffer, so changes to the buffer should be reflected in the image, and vice versa.
        Otherwise, PIL will make a copy of the data.
        
        Returns:
            PIL.Image.Image: PIL image (representation or copy of the bitmap buffer).
        """
        
        # https://pillow.readthedocs.io/en/stable/reference/Image.html#PIL.Image.frombuffer
        # https://pillow.readthedocs.io/en/stable/handbook/writing-your-own-image-plugin.html#the-raw-decoder
        
        dest_mode = pdfium_i.BitmapTypeToStrReverse[self.format]
        image = Lazy.PIL_Image.frombuffer(
            dest_mode,                  # target color format
            (self.width, self.height),  # size
            self.buffer,                # buffer
            "raw",                      # decoder
            self.mode,                  # input color format
            self.stride,                # bytes per line
            1,                          # orientation (top->bottom)
        )
        # set `readonly = False` so changes to the image are reflected in the buffer, if the original buffer is used
        image.readonly = False
        
        return image
    
    
    @classmethod
    def from_pil(cls, pil_image):
        """
        Convert a :mod:`PIL` image to a PDFium bitmap.
        Due to the limited number of color formats and bit depths supported by :attr:`FPDF_BITMAP`, this may be a lossy operation.
        
        Bitmaps returned by this function should be treated as immutable.
        
        Parameters:
            pil_image (PIL.Image.Image):
                The image.
        Returns:
            PdfBitmap: PDFium bitmap (with a copy of the PIL image's data).
        """
        
        # FIXME possibility to get mutable buffer from PIL image?
        
        if pil_image.mode in pdfium_i.BitmapStrToConst:
            # PIL always seems to represent BGR(A/X) input as RGB(A/X), so this code passage would only be reached for L
            format = pdfium_i.BitmapStrToConst[pil_image.mode]
        else:
            pil_image = _pil_convert_for_pdfium(pil_image)
            format = pdfium_i.BitmapStrReverseToConst[pil_image.mode]
        
        w, h = pil_image.size
        return cls.new_native(w, h, format, rev_byteorder=False, buffer=pil_image.tobytes())
    
    
    def get_posconv(self, page):
        """
        Acquire a :class:`.PdfPosConv` object to translate between coordinates on the bitmap and the page it was rendered from.
        
        This method requires passing in the page explicitly, to avoid holding a strong reference, so that bitmap and page can be independently freed by finalizer.
        """
        # if the bitmap was rendered from a page, resolve the weakref and check identity
        # before that, make sure *page* isn't None because that's what the weakref may resolve to if the referenced object is not alive anymore.
        assert page, "Page must be non-null"
        if not self._pos_args or self._pos_args[0]() is not page:
            raise RuntimeError("This bitmap does not belong to the given page.")
        return PdfPosConv(page, self._pos_args[1:])


def _pil_convert_for_pdfium(pil_image):
    
    if pil_image.mode == "1":
        pil_image = pil_image.convert("L")
    elif pil_image.mode.startswith("RGB"):
        pass
    elif "A" in pil_image.mode:
        pil_image = pil_image.convert("RGBA")
    else:
        pil_image = pil_image.convert("RGB")
    
    # convert RGB(A/X) to BGR(A) for PDFium
    if pil_image.mode == "RGB":
        r, g, b = pil_image.split()
        pil_image = Lazy.PIL_Image.merge("RGB", (b, g, r))
    elif pil_image.mode == "RGBA":
        r, g, b, a = pil_image.split()
        pil_image = Lazy.PIL_Image.merge("RGBA", (b, g, r, a))
    elif pil_image.mode == "RGBX":
        # technically the x channel may be unnecessary, but preserve what the caller passes in
        r, g, b, x = pil_image.split()
        pil_image = Lazy.PIL_Image.merge("RGBX", (b, g, r, x))
    
    return pil_image


class PdfPosConv:
    """
    Pdf coordinate translator.
    
    Hint:
        You may want to use :meth:`.PdfBitmap.get_posconv` to obtain an instance of this class.
    
    Parameters:
        page (PdfPage):
            Handle to the page.
        pos_args (tuple[int*5]):
            pdfium canvas args (start_x, start_y, size_x, size_y, rotate), as in ``FPDF_RenderPageBitmap()`` etc.
    """
    
    # FIXME do we have to do overflow checking against too large sizes?
    
    def __init__(self, page, pos_args):
        self.page = page
        self.pos_args = pos_args
    
    def __repr__(self):
        return f"{PdfPosConv.__name__}({self.page}, {self.pos_args})"
    
    def to_page(self, bitmap_x, bitmap_y):
        """
        Translate coordinates from bitmap to page.
        """
        page_x, page_y = ctypes.c_double(), ctypes.c_double()
        ok = pdfium_c.FPDF_DeviceToPage(self.page, *self.pos_args, bitmap_x, bitmap_y, page_x, page_y)
        if not ok:
            raise PdfiumError("Failed to translate to page coordinates.")
        return (page_x.value, page_y.value)
    
    def to_bitmap(self, page_x, page_y):
        """
        Translate coordinates from page to bitmap.
        """
        bitmap_x, bitmap_y = ctypes.c_int(), ctypes.c_int()
        ok = pdfium_c.FPDF_PageToDevice(self.page, *self.pos_args, page_x, page_y, bitmap_x, bitmap_y)
        if not ok:
            raise PdfiumError("Failed to translate to bitmap coordinates.")
        return (bitmap_x.value, bitmap_y.value)
