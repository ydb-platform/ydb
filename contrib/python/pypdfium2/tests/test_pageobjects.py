# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import io
import re
import pytest
import PIL.Image
import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
from .conftest import TestFiles, OutputDir, compare_n2


def test_image_objects():
    pdf = pdfium.PdfDocument(TestFiles.images)
    page = pdf[0]
    assert page.pdf is pdf
    
    images = list( page.get_objects(filter=[pdfium_c.FPDF_PAGEOBJ_IMAGE]) )
    assert len(images) == 3
    
    img_0 = images[0]
    assert isinstance(img_0, pdfium.PdfObject)
    assert type(img_0) is pdfium.PdfImage
    assert img_0.type == pdfium_c.FPDF_PAGEOBJ_IMAGE
    assert isinstance(img_0.raw, pdfium_c.FPDF_PAGEOBJECT)
    assert img_0.level == 0
    assert img_0.page is page
    assert img_0.pdf is pdf
    
    positions = [img.get_bounds() for img in images]
    exp_positions = [
        (133, 459, 350, 550),
        (48, 652, 163, 700),
        (204, 204, 577, 360),
    ]
    compare_n2(positions, exp_positions)
    
    compare_n2(
        img_0.get_quad_points(),
        ((132.7, 459.2), (349.5, 459.2), (349.5, 549.7), (132.7, 549.7))
    )


def test_misc_objects():
    
    pdf = pdfium.PdfDocument(TestFiles.render)
    page = pdf[0]
    assert page.pdf is pdf
    
    for obj in page.get_objects():
        if obj.type == pdfium_c.FPDF_PAGEOBJ_TEXT:
            assert type(obj) is pdfium.PdfTextObj
        else:
            assert type(obj) is pdfium.PdfObject
        assert isinstance(obj.raw, pdfium_c.FPDF_PAGEOBJECT)
        assert obj.type in (pdfium_c.FPDF_PAGEOBJ_TEXT, pdfium_c.FPDF_PAGEOBJ_PATH)
        assert obj.level == 0
        assert obj.page is page
        assert obj.pdf is pdf
        pos = obj.get_bounds()
        assert len(pos) == 4
    
    text_obj = next(obj for obj in page.get_objects() if obj.type == pdfium_c.FPDF_PAGEOBJ_TEXT)
    path_obj = next(obj for obj in page.get_objects() if obj.type == pdfium_c.FPDF_PAGEOBJ_PATH)
    
    compare_n2(
        text_obj.get_quad_points(),
        ((57.3, 767.4), (124.2, 767.4), (124.2, 780.9), (57.3, 780.9))
    )
    
    with pytest.raises(RuntimeError, match=re.escape("Quad points only supported for image and text objects.")):
        path_obj.get_quad_points()


def test_new_image_from_jpeg():
    
    pdf = pdfium.PdfDocument.new()
    page = pdf.new_page(240, 120)
    
    image_a = pdfium.PdfImage.new(pdf)
    buffer = open(TestFiles.mona_lisa, "rb")
    image_a.load_jpeg(buffer, autoclose=True)
    width, height = image_a.get_px_size()
    page.insert_obj(image_a)
    
    assert len(pdf._data_holder) == 1
    assert pdf._data_closer == [buffer]
    
    assert image_a.get_matrix() == pdfium.PdfMatrix()
    image_a.set_matrix( pdfium.PdfMatrix().scale(width, height) )
    assert image_a.get_matrix() == pdfium.PdfMatrix(width, 0, 0, height, 0, 0)
    
    pil_image_1 = PIL.Image.open(TestFiles.mona_lisa)
    bitmap = image_a.get_bitmap()
    pil_image_2 = bitmap.to_pil()
    assert (120, 120) == pil_image_1.size == pil_image_2.size == (bitmap.width, bitmap.height)
    assert "RGB" == pil_image_1.mode == pil_image_2.mode
    
    in_data = TestFiles.mona_lisa.read_bytes()
    out_buffer = io.BytesIO()
    image_a.extract(out_buffer)
    out_buffer.seek(0)
    out_data = out_buffer.read()
    assert in_data == out_data
    
    metadata = image_a.get_metadata()
    assert isinstance(metadata, pdfium_c.FPDF_IMAGEOBJ_METADATA)
    assert metadata.bits_per_pixel == 24  # 3 channels, 8 bits each
    assert metadata.colorspace == pdfium_c.FPDF_COLORSPACE_DEVICERGB
    assert metadata.height == height == 120
    assert metadata.width == width == 120
    assert metadata.horizontal_dpi == 72
    assert metadata.vertical_dpi == 72
    
    image_b = pdfium.PdfImage.new(pdf)
    with open(TestFiles.mona_lisa, "rb") as buffer:
        image_b.load_jpeg(buffer, inline=True, autoclose=False)
    
    assert image_b.get_matrix() == pdfium.PdfMatrix()
    image_b.set_matrix( pdfium.PdfMatrix().scale(width, height).translate(width, 0) )
    image_b.get_matrix() == pdfium.PdfMatrix(width, 0, 0, height, width, 0)
    page.insert_obj(image_b)
    
    page.gen_content()
    out_path = OutputDir / "image_jpeg.pdf"
    pdf.save(out_path)
    assert out_path.exists()
    
    page._finalizer()
    pdf._finalizer()
    assert buffer.closed is True


def test_new_image_from_bitmap():
    
    src_pdf = pdfium.PdfDocument(TestFiles.render)
    src_page = src_pdf[0]
    dst_pdf = pdfium.PdfDocument.new()
    image_a = pdfium.PdfImage.new(dst_pdf)
    
    bitmap = src_page.render()
    w, h = bitmap.width, bitmap.height
    image_a.set_bitmap(bitmap)
    image_a.set_matrix( pdfium.PdfMatrix().scale(w, h) )
    
    pil_image = PIL.Image.open(TestFiles.mona_lisa)
    bitmap = pdfium.PdfBitmap.from_pil(pil_image)
    image_b = pdfium.PdfImage.new(dst_pdf)
    image_b.set_matrix( pdfium.PdfMatrix().scale(bitmap.width, bitmap.height) )
    image_b.set_bitmap(bitmap)
    
    dst_page = dst_pdf.new_page(w, h)
    dst_page.insert_obj(image_a)
    dst_page.insert_obj(image_b)
    dst_page.gen_content()
    
    out_path = OutputDir / "image_bitmap.pdf"
    dst_pdf.save(out_path)
    
    reopened_pdf = pdfium.PdfDocument(out_path)
    reopened_page = reopened_pdf[0]
    reopened_image = next( reopened_page.get_objects(filter=[pdfium_c.FPDF_PAGEOBJ_IMAGE]) )
    assert reopened_image.get_filters() == ["FlateDecode"]


def test_replace_image_with_jpeg():
    
    pdf = pdfium.PdfDocument(TestFiles.images)
    page = pdf[0]
    
    images = list( page.get_objects(filter=[pdfium_c.FPDF_PAGEOBJ_IMAGE]) )
    matrices = [img.get_matrix() for img in images]
    assert len(images) == 3
    image_1 = images[0]
    
    image_1.load_jpeg(TestFiles.mona_lisa, pages=[page])
    width, height = image_1.get_px_size()
    assert matrices == [img.get_matrix() for img in images]
    
    # preserve the aspect ratio
    # this strategy only works if the matrix was just used for size/position
    for image, matrix in zip(images, matrices):
        w_scale = matrix.a / width
        h_scale = matrix.d / height
        scale = min(w_scale, h_scale)
        new_matrix = pdfium.PdfMatrix(width*scale, 0, 0, height*scale, matrix.e, matrix.f)
        image.set_matrix(new_matrix)
        assert image.get_matrix() == new_matrix
    
    page.gen_content()
    output_path = OutputDir / "replace_images.pdf"
    pdf.save(output_path)
    assert output_path.exists()


@pytest.mark.parametrize(
    "render, scale_to_original",
    [(False, None), (True, False), (True, True)]
)
def test_image_get_bitmap(render, scale_to_original):
    
    pdf = pdfium.PdfDocument(TestFiles.images)
    page = pdf[0]
    image = next( page.get_objects(filter=[pdfium_c.FPDF_PAGEOBJ_IMAGE]) )
    
    metadata = image.get_metadata()
    assert metadata.width == 115
    assert metadata.height == 48
    assert round(metadata.horizontal_dpi) == 38
    assert round(metadata.vertical_dpi) == 38
    assert metadata.colorspace == pdfium_c.FPDF_COLORSPACE_DEVICEGRAY
    assert metadata.marked_content_id == 1
    assert metadata.bits_per_pixel == 1
    
    kwargs = dict(render=render)
    if scale_to_original is not None:
        kwargs["scale_to_original"] = scale_to_original
    bitmap = image.get_bitmap(**kwargs)
    assert isinstance(bitmap, pdfium.PdfBitmap)
    
    if render:
        assert bitmap.format == pdfium_c.FPDFBitmap_BGRA
        assert bitmap.n_channels == 4
        # Somewhere between pdfium 6462 and 6899, size/stride expectation changed here
        if scale_to_original:
            assert (bitmap.width, bitmap.height, bitmap.stride) == (115, 49, 460)
        else:
            assert (bitmap.width, bitmap.height, bitmap.stride) == (217, 91, 868)
        assert bitmap.rev_byteorder is False
        output_path = OutputDir / "extract_rendered.png"
    else:
        # NOTE fails with pdfium >= 1e1e173 (6015), < b5bc2e9 (6029), which returns RGB
        assert bitmap.format == pdfium_c.FPDFBitmap_Gray
        assert bitmap.n_channels == 1
        assert (bitmap.width, bitmap.height, bitmap.stride) == (115, 48, 116)
        assert bitmap.rev_byteorder is False
        output_path = OutputDir / "extract.png"
    
    pil_image = bitmap.to_pil()
    assert isinstance(pil_image, PIL.Image.Image)
    pil_image.save(output_path)
    assert output_path.exists()


def test_remove_image():
    
    pdf = pdfium.PdfDocument(TestFiles.images)
    page_1 = pdf[0]
    
    # TODO order images by position
    images = list( page_1.get_objects(filter=[pdfium_c.FPDF_PAGEOBJ_IMAGE]) )
    assert len(images) == 3
    
    # drop an image
    page_1.remove_obj(images[0])
    
    # delete and re-insert an image in place
    page_1.remove_obj(images[1])
    page_1.insert_obj(images[1])
    
    page_1.gen_content()
    
    output_path = OutputDir / "test_remove_objects.pdf"
    pdf.save(output_path)
    assert output_path.exists()


@pytest.mark.skip(reason="FPDFFormObj_RemoveObject not available in pdfium 145.0.7616.0")
def test_remove_image_in_xobject():
    pdf = pdfium.PdfDocument(TestFiles.form_object_with_image)
    page = pdf.get_page(0)
    image, = list(page.get_objects(filter=[pdfium_c.FPDF_PAGEOBJ_IMAGE]))
    
    assert isinstance(image.container, pdfium.PdfObject)
    assert image.container.type == pdfium_c.FPDF_PAGEOBJ_FORM
    
    # FIXME does not actually disappear yet when saving the page. This is probably a bug in the experimental API.
    page.remove_obj(image)
    # page.remove_obj(image.container)
    page.gen_content()
    
    output_path = OutputDir / "remove_image_in_xobject.pdf"
    pdf.save(output_path)
    assert output_path.exists()


def test_loose_pageobject():
    src_pdf = pdfium.PdfDocument(TestFiles.multipage)
    dest_pdf = pdfium.PdfDocument.new()
    xobject = src_pdf.page_as_xobject(0, dest_pdf)
    po = xobject.as_pageobject()
    assert po._finalizer.alive
    po.close()
