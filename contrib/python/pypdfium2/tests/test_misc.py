# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import sys
import pytest
import platform
import warnings
import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pypdfium2.internal as pdfium_i
from pypdfium2.version import PDFIUM_INFO
# from .conftest import OutputDir

@pytest.mark.parametrize(
    ["color_in", "rev_byteorder", "exp_color"],
    [
        ((170, 187, 204, 221), False, 0xDDAABBCC),
        ((170, 187, 204, 221), True, 0xDDCCBBAA),
    ]
)
def test_color_tohex(color_in, rev_byteorder, exp_color):
    
    assert pdfium_i.color_tohex(color_in, rev_byteorder) == exp_color
    
    # PDFium's utility macros just encode/decode the given color positionally, regardless of byte order
    r, g, b, a = color_in
    channels = (a, b, g, r) if rev_byteorder else (a, r, g, b)
    assert pdfium_c.FPDF_ARGB(*channels) == exp_color
    assert pdfium_c.FPDF_GetAValue(exp_color) == channels[0]
    assert pdfium_c.FPDF_GetRValue(exp_color) == channels[1]
    assert pdfium_c.FPDF_GetGValue(exp_color) == channels[2]
    assert pdfium_c.FPDF_GetBValue(exp_color) == channels[3]


def _filter(prefix, skips=(), type=int):
    items = []
    for attr in dir(pdfium_c):
        value = getattr(pdfium_c, attr)
        if not attr.startswith(prefix) or not isinstance(value, type) or value in skips:
            continue
        items.append(value)
    return items


BitmapNsp = _filter("FPDFBitmap_", [pdfium_c.FPDFBitmap_Unknown])
PageObjNsp = _filter("FPDF_PAGEOBJ_")
ErrorMapping = pdfium_i.ErrorToStr
# FIXME this will cause an erroneous test failure when using the reference bindings with a non-XFA build
if "XFA" in PDFIUM_INFO.flags:
    ErrorMapping.update(pdfium_i.XFAErrorToStr)


@pytest.mark.parametrize(
    ["mapping", "use_keys", "items"],
    [
        (pdfium_i.BitmapTypeToNChannels,   True,  BitmapNsp),
        (pdfium_i.BitmapTypeToStr,         True,  BitmapNsp),
        (pdfium_i.BitmapTypeToStrReverse,  True,  BitmapNsp),
        (pdfium_i.BitmapStrToConst,        False, BitmapNsp),
        (pdfium_i.BitmapStrReverseToConst, False, BitmapNsp),
        (pdfium_i.FormTypeToStr,           True,  _filter("FORMTYPE_", [pdfium_c.FORMTYPE_COUNT])),
        (pdfium_i.ColorspaceToStr,         True,  _filter("FPDF_COLORSPACE_")),
        (pdfium_i.ViewmodeToStr,           True,  _filter("PDFDEST_VIEW_")),
        (pdfium_i.ObjectTypeToStr,         True,  PageObjNsp),
        (pdfium_i.ObjectTypeToConst,       False, PageObjNsp),
        (pdfium_i.PageModeToStr,           True,  _filter("PAGEMODE_")),
        (ErrorMapping,                     True,  _filter("FPDF_ERR_")),
        (pdfium_i.UnsupportedInfoToStr,    True,  _filter("FPDF_UNSP_")),
    ]
)
def test_const_converters(mapping, use_keys, items):
    
    assert len(mapping) == len(items)
    
    container = mapping.keys() if use_keys else mapping.values()
    for item in items:
        assert item in container


@pytest.mark.parametrize(
    ["degrees", "const"],
    [
        (0,   0),
        (90,  1),
        (180, 2),
        (270, 3),
    ]
)
def test_const_converters_rotation(degrees, const):
    assert pdfium_i.RotationToConst[degrees] == const
    assert pdfium_i.RotationToDegrees[const] == degrees


@pytest.mark.parametrize(
    "mode_str, rev_byteorder",
    [
        ("BGR", False),
        ("BGR", True),
        ("BGRA", False),
        ("BGRA", True),
        ("BGRX", False),
        ("BGRX", True),
        ("L", False),
    ]
)
def test_bitmap_makers_to_images(mode_str, rev_byteorder):
    
    # Exercise the various bitmap maker strategies, and confirm we can add them all as images to a PDF.
    # Admittedly, this is a bit off-practice, as the bitmap maker are mainly for rendering. When embedding an existing image, you probably have a native buffer on the caller side, and don't want to create a new foreign buffer.
    
    w, h = 10, 10
    rect = 0, 0, w, h
    mode_constant = pdfium_i.BitmapStrToConst[mode_str]
    
    pdf = pdfium.PdfDocument.new()
    page = pdf.new_page(w*2, h*2)
    common_mat = pdfium.PdfMatrix().scale(w, h)
    
    def _add_bitmap_at_pos(bitmap, x, y):
        img = pdfium.PdfImage.new(pdf)
        img.set_bitmap(bitmap)
        img.set_matrix(common_mat.translate(x, y))
        page.insert_obj(img)
    
    # with rev_byteorder, red and blue swap place, while green and black are unaffected
    
    native = pdfium.PdfBitmap.new_native(w, h, mode_constant, rev_byteorder=rev_byteorder)
    native.fill_rect((255, 0, 0, 255), *rect)  # red/blue
    _add_bitmap_at_pos(native, 0, 10)  # top left
    
    foreign = pdfium.PdfBitmap.new_foreign(w, h, mode_constant, rev_byteorder=rev_byteorder)
    foreign.fill_rect((0, 255, 0, 255), *rect)  # green
    _add_bitmap_at_pos(foreign, 10, 10)  # top right
    
    foreign_packed = pdfium.PdfBitmap.new_foreign(w, h, mode_constant, force_packed=True, rev_byteorder=rev_byteorder)
    foreign_packed.fill_rect((0, 0, 255, 255), *rect)  # blue/red
    _add_bitmap_at_pos(foreign_packed, 0, 0)  # bottom left
    
    map_to_use_alpha = {pdfium_c.FPDFBitmap_BGRx: False, pdfium_c.FPDFBitmap_BGRA: True}
    if mode_constant in map_to_use_alpha:
        use_alpha = map_to_use_alpha[mode_constant]
        foreign_simple = pdfium.PdfBitmap.new_foreign_simple(w, h, use_alpha=use_alpha, rev_byteorder=rev_byteorder)
        foreign_simple.fill_rect((0, 0, 0, 255), *rect)  # black
        _add_bitmap_at_pos(foreign_simple, 10, 0)  # bottom right
    
    page.gen_content()
    # pdf.save(OutputDir / f"bitmap_makers_imgs_{mode_str}_{rev_byteorder}.pdf")


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="libc_ver/musl test only makes sense on linux")
def test_musllinux_api_available():
    
    # Test availability of the non-public API we use to detect musllinux on setup.
    # `packaging` is a pretty fundamental package so expect it to be installed
    
    import packaging._musllinux
    assert hasattr(packaging._musllinux, "_get_musl_version")
    
    libc_name, libc_ver = platform.libc_ver()
    musl_ver = packaging._musllinux._get_musl_version(sys.executable)
    
    if libc_name in ("glibc", "libc"):
        assert not musl_ver
    else:
        assert musl_ver, "Not glibc or android libc, expected musl"
        if libc_name:
            warnings.warn(f"platform.libc_ver() now returns {(libc_name, libc_ver)} for musl")
