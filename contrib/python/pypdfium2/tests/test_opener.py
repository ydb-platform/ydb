# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

# FIXME merge with test_document and deduplicate

import re
import shutil
import tempfile
import traceback
from pathlib import Path
import PIL.Image
import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest
from .conftest import TestFiles, ExpRenderPixels


def _check_general(pdf, n_pages=1):
    assert isinstance(pdf, pdfium.PdfDocument)
    assert len(pdf) == n_pages
    version = pdf.get_version()
    assert isinstance(version, int)
    assert 10 < version < 30


def _check_render(pdf):
    
    page = pdf[0]
    pil_image = page.render().to_pil()
    
    assert pil_image.mode == "RGB"
    assert pil_image.size == (595, 842)
    for pos, exp_value in ExpRenderPixels:
        assert pil_image.getpixel(pos) == exp_value
    
    return pil_image


@pytest.fixture
def open_filepath_native():
    pdf = pdfium.PdfDocument(TestFiles.render)
    assert pdf._data_holder == []
    assert pdf._data_closer == []
    _check_general(pdf)
    yield _check_render(pdf)


@pytest.fixture
def open_bytes():
    
    bytedata = TestFiles.render.read_bytes()
    assert isinstance(bytedata, bytes)
    pdf = pdfium.PdfDocument(bytedata)
    assert pdf._data_holder == [bytedata]
    assert pdf._data_closer == []
    
    _check_general(pdf)
    yield _check_render(pdf)


@pytest.fixture
def open_buffer():
    
    buffer = open(TestFiles.render, "rb")
    pdf = pdfium.PdfDocument(buffer)
    assert len(pdf._data_holder) == 1
    assert pdf._data_closer == []
    
    _check_general(pdf)
    yield _check_render(pdf)
    
    assert buffer.closed is False
    buffer.close()


def test_opener_inputtypes(open_filepath_native, open_bytes, open_buffer):
    first_image = open_filepath_native
    other_images = (
        open_bytes,
        open_buffer,
    )
    assert isinstance(first_image, PIL.Image.Image)
    assert all(first_image == other for other in other_images)


def test_open_buffer_autoclose():
    
    buffer = open(TestFiles.render, "rb")
    pdf = pdfium.PdfDocument(buffer, autoclose=True)
    assert len(pdf._data_holder) == 1
    assert pdf._data_closer == [buffer]
    _check_general(pdf)
    
    pdf._finalizer()
    assert buffer.closed is True


def test_open_encrypted():
    
    buffer = open(TestFiles.encrypted, "rb")
    bytedata = buffer.read()
    buffer.seek(0)
    
    test_cases = [
        (TestFiles.encrypted, "test_owner"),
        (TestFiles.encrypted, "test_user"),
        (bytedata, "test_owner"),
        (bytedata, "test_user"),
        (buffer, "test_owner"),
        (buffer, "test_user"),
    ]
    
    for input_data, password in test_cases:
        pdf = pdfium.PdfDocument(input_data, password)
        _check_general(pdf)
        if input_data is buffer:
            buffer.seek(0)
    
    buffer.close()
    
    with pytest.raises(pdfium.PdfiumError, match=re.escape("Failed to load document (PDFium: Incorrect password error).")):
        pdf = pdfium.PdfDocument(TestFiles.encrypted, "wrong_password")


def test_open_nonascii():
    
    tempdir = tempfile.TemporaryDirectory(prefix="pypdfium2_")
    nonascii_file = Path(tempdir.name) / "tên file chứakýtự éèáàçß 发短信.pdf"
    shutil.copy(TestFiles.render, nonascii_file)
    
    pdf = pdfium.PdfDocument(nonascii_file)
    _check_general(pdf)
    
    # FIXME cleanup permission error on Windows
    try:
        nonascii_file.unlink()
        tempdir.cleanup()
    except Exception:
        traceback.print_exc()


def test_open_new():
    
    dest_pdf = pdfium.PdfDocument.new()
    
    assert isinstance(dest_pdf, pdfium.PdfDocument)
    assert isinstance(dest_pdf.raw, pdfium_c.FPDF_DOCUMENT)
    assert dest_pdf._data_holder == []
    assert dest_pdf._data_closer == []
    
    assert dest_pdf.get_version() is None
    
    src_pdf = pdfium.PdfDocument(TestFiles.multipage)
    dest_pdf.import_pages(src_pdf, [0, 2])
    assert len(dest_pdf) == 2


def test_open_invalid():
    with pytest.raises(TypeError):
        pdf = pdfium.PdfDocument(123)
    with pytest.raises(FileNotFoundError):
        pdf = pdfium.PdfDocument("invalid/path")


def test_object_hierarchy():
    
    pdf = pdfium.PdfDocument(TestFiles.images)
    assert isinstance(pdf, pdfium.PdfDocument)
    assert isinstance(pdf.raw, pdfium_c.FPDF_DOCUMENT)
    
    page = pdf[0]
    assert isinstance(page, pdfium.PdfPage)
    assert isinstance(page.raw, pdfium_c.FPDF_PAGE)
    assert page.pdf is pdf
    
    pageobj = next(page.get_objects())
    assert isinstance(pageobj, pdfium.PdfObject)
    assert isinstance(pageobj.raw, pdfium_c.FPDF_PAGEOBJECT)
    assert isinstance(pageobj.type, int)
    assert pageobj.page is page
    
    textpage = page.get_textpage()
    assert isinstance(textpage, pdfium.PdfTextPage)
    assert isinstance(textpage.raw, pdfium_c.FPDF_TEXTPAGE)
    assert textpage.page is page
    
    searcher = textpage.search("abcd")
    assert isinstance(searcher, pdfium.PdfTextSearcher)
    assert isinstance(searcher.raw, pdfium_c.FPDF_SCHHANDLE)
    assert searcher.textpage is textpage
    
    for obj in (searcher, textpage, page, pdf):
        assert obj._finalizer and obj._finalizer.alive
        obj.close()
        assert not obj._finalizer


def _compare_metadata(pdf, metadata, exp_metadata):
    all_keys = ("Title", "Author", "Subject", "Keywords", "Creator", "Producer", "CreationDate", "ModDate")
    assert len(metadata) == len(all_keys)
    assert all(k in metadata for k in all_keys)
    for k in all_keys:
        assert metadata[k] == pdf.get_metadata_value(k)
        if k in exp_metadata:
            assert metadata[k] == exp_metadata[k]
        else:
            assert metadata[k] == ""


def test_metadata():
    pdf = pdfium.PdfDocument(TestFiles.empty)
    metadata = pdf.get_metadata_dict()
    exp_metadata = {
        "Producer": "LibreOffice 6.4",
        "Creator": "Writer",
        "CreationDate": "D:20220520145414+02'00'",
    }
    _compare_metadata(pdf, metadata, exp_metadata)


def test_doc_extras():
        
    pdf = pdfium.PdfDocument(TestFiles.empty)
    assert len(pdf) == 1
    page = pdf[0]
    
    pdf = pdfium.PdfDocument.new()
    assert len(pdf) == 0
    
    sizes = [(50, 100), (100, 150), (150, 200), (200, 250)]
    for size in sizes:
        page = pdf.new_page(*size)
    for i, (size, page) in enumerate(zip(sizes, pdf)):
        assert size == page.get_size() == pdf.get_page_size(i)
    
    del pdf[0]
    page = pdf[0]
    assert page.get_size() == pdf.get_page_size(0) == (100, 150)
