# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import io
import pytest
import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
from .conftest import TestFiles, OutputDir


def _new_pdf_saving_handler(version):
    
    pdf = pdfium.PdfDocument.new()
    size = (612, 792)
    pdf.new_page(*size)
    
    kwargs = {}
    if version:
        kwargs["version"] = version
    
    saved_pdf = yield pdf, kwargs
    if version:
        saved_pdf.get_version() == version
    assert len(saved_pdf) == 1
    assert saved_pdf.get_page_size(0) == size
    
    yield


def _save_to_file(version, tmp_path, use_str):
    
    handler = _new_pdf_saving_handler(version)
    pdf, kwargs = next(handler)
    
    path = tmp_path / "test_save_to_file.pdf"
    dest = str(path) if use_str else path
    
    pdf.save(dest, **kwargs)
    assert path.is_file()
    
    saved_pdf = pdfium.PdfDocument(path)
    handler.send(saved_pdf)
    # path.unlink()  # FIXME fails on Windows


parametrize_saving_version = pytest.mark.parametrize("version", [None, 14, 17])

def test_save_new_to_strpath(tmp_path):
    _save_to_file(15, tmp_path, use_str=True)

@parametrize_saving_version
def test_save_new_to_path(version, tmp_path):
    _save_to_file(version, tmp_path, use_str=False)

@parametrize_saving_version
def test_save_new_to_buffer(version):
    
    handler = _new_pdf_saving_handler(version)
    pdf, kwargs = next(handler)
    
    out_buffer = io.BytesIO()
    pdf.save(out_buffer, **kwargs)
    assert out_buffer.tell() > 100
    out_buffer.seek(0)
    
    saved_pdf = pdfium.PdfDocument(out_buffer, autoclose=True)
    handler.send(saved_pdf)


def test_save_tiled():

    src_pdf = pdfium.PdfDocument(TestFiles.multipage)
    new_pdf_raw = pdfium_c.FPDF_ImportNPagesToOne(
        src_pdf.raw,
        595, 842,
        2, 2,
    )

    new_pdf = pdfium.PdfDocument(new_pdf_raw)
    assert len(new_pdf) == 1
    page = new_pdf[0]
    assert page.get_size() == (595, 842)

    output_file = OutputDir / "tiling.pdf"
    new_pdf.save(output_file)
    assert output_file.exists()


def test_save_with_deletion():

    # Regression test for BUG(96):
    # Make sure page deletions take effect when saving a document

    pdf = pdfium.PdfDocument(TestFiles.multipage)
    assert len(pdf) == 3
    pdf.del_page(0)
    assert len(pdf) == 2

    buffer = io.BytesIO()
    pdf.save(buffer)
    buffer.seek(0)

    saved_pdf = pdfium.PdfDocument(buffer, autoclose=True)
    assert len(saved_pdf) == 2

    page = saved_pdf[0]
    textpage = page.get_textpage()
    assert textpage.get_text_bounded() == "Page\r\n2"


def test_save_and_check_id():  # includes deletion, versioned save, and raw data start/end check

    pdf = pdfium.PdfDocument(TestFiles.multipage)
    pre_id_p = pdf.get_identifier(pdfium_c.FILEIDTYPE_PERMANENT)
    pre_id_c = pdf.get_identifier(pdfium_c.FILEIDTYPE_CHANGING)
    assert isinstance(pre_id_p, bytes)
    pdf.del_page(1)

    buffer = io.BytesIO()
    pdf.save(buffer, version=17)

    buffer.seek(0)
    data = buffer.read()
    buffer.seek(0)

    exp_start = b"%PDF-1.7"
    exp_end = b"%EOF\r\n"
    assert data[:len(exp_start)] == exp_start
    assert data[-len(exp_end):] == exp_end

    reopened_pdf = pdfium.PdfDocument(buffer, autoclose=True)
    assert len(reopened_pdf) == 2
    assert reopened_pdf.get_version() == 17

    post_id_p = reopened_pdf.get_identifier(pdfium_c.FILEIDTYPE_PERMANENT)
    post_id_c = reopened_pdf.get_identifier(pdfium_c.FILEIDTYPE_CHANGING)
    assert pre_id_p == post_id_p
    assert pre_id_c != post_id_c
