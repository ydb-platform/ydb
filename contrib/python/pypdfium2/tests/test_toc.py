# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import pytest
import logging
import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
from .conftest import TestFiles


def _compare_bookmark(bm, **kwargs):
    assert isinstance(bm, pdfium.PdfBookmark)
    assert kwargs["title"] == bm.get_title()
    assert kwargs["count"] == bm.get_count()
    dest = bm.get_dest()
    if dest is None:
        assert kwargs["dest"] is None
    else:
        assert isinstance(dest, pdfium.PdfDest)
        assert kwargs["page_index"] == dest.get_index()
        view_mode, view_pos = dest.get_view()
        assert kwargs["view_mode"] == view_mode
        assert kwargs["view_pos"] == pytest.approx(view_pos, abs=1)


def test_gettoc():
    
    pdf = pdfium.PdfDocument(TestFiles.toc)
    toc = pdf.get_toc()
    
    # check first bookmark
    _compare_bookmark(
        next(toc),
        title = "One",
        page_index = 0,
        view_mode = pdfium_c.PDFDEST_VIEW_XYZ,
        view_pos = (89, 758, 0),
        count = -2,
    )
    
    # check common values
    for bm in toc:
        dest = bm.get_dest()
        view_mode, view_pos = dest.get_view()
        assert view_mode == pdfium_c.PDFDEST_VIEW_XYZ
        assert round(view_pos[0]) == 89
    
    # check last bookmark
    _compare_bookmark(
        bm,
        title = "Three-B",
        page_index = 1,
        view_mode = pdfium_c.PDFDEST_VIEW_XYZ,
        view_pos = (89, 657, 0),
        count = 0,
    )


def test_gettoc_circular(caplog):
    
    pdf = pdfium.PdfDocument(TestFiles.toc_circular)
    toc = pdf.get_toc()
    
    _compare_bookmark(
        next(toc),
        title = "A Good Beginning",
        dest = None,
        count = 0,
    )
    _compare_bookmark(
        next(toc),
        title = "A Good Ending",
        dest = None,
        count = 0,
    )
    with caplog.at_level(logging.WARNING):
        for other in toc: pass
    assert "circular bookmark reference" in caplog.text
