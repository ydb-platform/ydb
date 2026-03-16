# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import pytest
import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
from .conftest import TestFiles, OutputDir


def test_xobject_placement():
    
    # basic test to at least run through the code
    
    src_pdf = pdfium.PdfDocument(TestFiles.multipage)
    dest_pdf = pdfium.PdfDocument.new()
    xobject = src_pdf.page_as_xobject(0, dest_pdf)
    
    src_width, src_height = src_pdf.get_page_size(0)
    assert (round(src_width), round(src_height)) == (595, 842)
    w, h = src_width/2, src_height/2  # object size
    base_matrix = pdfium.PdfMatrix().scale(0.5, 0.5)
    
    dest_page_1 = dest_pdf.new_page(src_width, src_height)
    
    po = xobject.as_pageobject()
    assert po.get_matrix() == pdfium.PdfMatrix()
    assert po.type == pdfium_c.FPDF_PAGEOBJ_FORM
    matrix = base_matrix.translate(0, h)
    assert matrix == pdfium.PdfMatrix(0.5, 0, 0, 0.5, 0, h)
    po.set_matrix(matrix)
    assert po.get_matrix() == matrix
    dest_page_1.insert_obj(po)
    assert po.pdf is dest_pdf
    assert po.page is dest_page_1
    pos_a = po.get_bounds()
    # xfail with pdfium < 5370, https://crbug.com/pdfium/1905
    assert pytest.approx(pos_a, abs=0.5) == (19, 440, 279, 823)
    
    po = xobject.as_pageobject()
    matrix = base_matrix.mirror(invert_x=True, invert_y=False).translate(w, 0).translate(w, h)
    assert matrix == pdfium.PdfMatrix(-0.5, 0, 0, 0.5, 2*w, h)
    po.transform(matrix)
    dest_page_1.insert_obj(po)
    
    po = xobject.as_pageobject()
    assert po.get_matrix() == pdfium.PdfMatrix()
    matrix = base_matrix.mirror(invert_x=False, invert_y=True).translate(0, h).translate(w, 0)
    assert matrix == pdfium.PdfMatrix(0.5, 0, 0, -0.5, w, h)
    po.set_matrix(matrix)
    assert po.get_matrix() == matrix
    dest_page_1.insert_obj(po)
    
    po = xobject.as_pageobject()
    matrix = base_matrix.mirror(invert_x=True, invert_y=True).translate(w, h)
    assert matrix == pdfium.PdfMatrix(-0.5, 0, 0, -0.5, w, h)
    po.set_matrix(matrix)
    dest_page_1.insert_obj(po)
    
    dest_page_1.gen_content()
    square_len = w + h
    dest_page_2 = dest_pdf.new_page(square_len, square_len)
    
    po = xobject.as_pageobject()
    matrix = base_matrix.rotate(360).translate(0, w)
    assert pytest.approx(matrix.get()) == (0.5, 0, 0, 0.5, 0, w)
    po.set_matrix(matrix)
    dest_page_2.insert_obj(po)
    
    po = xobject.as_pageobject()
    matrix = base_matrix.rotate(90).translate(0, w).translate(w, h)
    assert pytest.approx(matrix.get()) == (0, -0.5, 0.5, 0, w, w+h)
    po.set_matrix(matrix)
    dest_page_2.insert_obj(po)
    
    po = xobject.as_pageobject()
    matrix = base_matrix.rotate(180).translate(w, h).translate(h, 0)
    assert pytest.approx(matrix.get()) == (-0.5, 0, 0, -0.5, w+h, h)
    po.set_matrix(matrix)
    dest_page_2.insert_obj(po)
    
    po = xobject.as_pageobject()
    matrix = base_matrix.rotate(270).translate(h, 0)
    assert pytest.approx(matrix.get()) == (0, 0.5, -0.5, 0, h, 0)
    po.set_matrix(matrix)
    dest_page_2.insert_obj(po)
    
    dest_page_2.gen_content()
    dest_page_3 = dest_pdf.new_page(src_width, src_height)
    
    po = xobject.as_pageobject()
    matrix = base_matrix.translate(-w/2, -h/2).rotate(90).translate(h/2, w/2)
    po.set_matrix(matrix)
    dest_page_3.insert_obj(po)
    
    po = xobject.as_pageobject()
    matrix = base_matrix.skew(10, 20).translate(0, w)
    po.set_matrix(matrix)
    dest_page_3.insert_obj(po)
    
    dest_page_3.gen_content()
    
    dest_pdf.save(OutputDir / "xobject_placement.pdf")


# TODO
# * test matrix copy and repr
# * test PdfObject.transform() properly
# * assert that we operate from the origin of the coordinate system
