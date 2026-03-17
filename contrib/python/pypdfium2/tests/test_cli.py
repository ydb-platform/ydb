# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import io
import sys
import logging
import filecmp
import contextlib
from pathlib import Path
import pytest
import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pypdfium2.__main__ as pdfium_cli
from .conftest import TestFiles, TestExpectations

lib_logger = logging.getLogger("pypdfium2")

@contextlib.contextmanager
def logging_capture_handler(buf):
    orig_handlers = lib_logger.handlers
    lib_logger.handlers = []
    handler = logging.StreamHandler(buf)
    lib_logger.addHandler(handler)
    yield
    lib_logger.removeHandler(handler)
    lib_logger.handlers = orig_handlers


@contextlib.contextmanager
def joined_ctx(ctxes):
    with contextlib.ExitStack() as stack:
        for ctx in ctxes: stack.enter_context(ctx)
        yield


class StringIOWithFileno (io.StringIO):
    
    def __init__(self, orig_stderr):
        super().__init__()
        self._orig_stderr = orig_stderr
    
    def fileno(self):
        return self._orig_stderr.fileno()


def run_cli(argv, exp_output=None, capture=("out", "err", "log"), normalize_lfs=False):
    
    argv = [str(a) for a in argv]
    
    if exp_output is None:
        pdfium_cli.api_main(argv)
        
    else:
        
        output = StringIOWithFileno(sys.stderr)
        ctxes = []
        assert isinstance(capture, (tuple, list))
        if "out" in capture:
            ctxes += [contextlib.redirect_stdout(output)]
        if "err" in capture:
            ctxes += [contextlib.redirect_stderr(output)]
        # for some reason, logging doesn't seem to go the usual stdout/stderr path, so explicitly install a stream handler to capture
        if "log" in capture:
            ctxes += [logging_capture_handler(output)]
        assert len(ctxes) >= 1
        with joined_ctx(ctxes):
            pdfium_cli.api_main(argv)
        
        if isinstance(exp_output, Path):
            exp_output = exp_output.read_text()
        
        output = output.getvalue()
        if normalize_lfs:
            output = output.replace("\r\n", "\n")
        
        assert output == exp_output


def _get_files(dir):
    return sorted([p.name for p in dir.iterdir() if p.is_file()])


def _get_text(pdf, index):
    return pdf[index].get_textpage().get_text_bounded()


@pytest.mark.parametrize("resource", ["toc", "toc_viewmodes", "toc_circular", "toc_maxdepth"])
def test_toc(resource):
    run_cli(["toc", getattr(TestFiles, resource)], getattr(TestExpectations, resource))


def test_attachments(tmp_path):
    
    run_cli(["attachments", TestFiles.attachments, "list"], TestExpectations.attachments_list)
    
    run_cli(["attachments", TestFiles.attachments, "extract", "-o", tmp_path])
    assert _get_files(tmp_path) == ["1_1.txt", "2_attached.pdf"]
    
    edited_pdf = tmp_path / "edited.pdf"
    run_cli(["attachments", TestFiles.attachments, "edit", "--del-numbers", "1,2", "--add-files", TestFiles.mona_lisa, "-o", edited_pdf])
    run_cli(["attachments", edited_pdf, "list"], "[1] mona_lisa.jpg\n", capture=["out"])


def test_images(tmp_path):
    
    img_pdf = tmp_path / "img_pdf.pdf"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    
    run_cli(["imgtopdf", TestFiles.mona_lisa, "-o", img_pdf])
    run_cli(["extract-images", img_pdf, "-o", output_dir])
    
    output_name = "img_pdf_1_1.jpg"
    assert _get_files(output_dir) == [output_name]
    assert filecmp.cmp(TestFiles.mona_lisa, output_dir/output_name)


@pytest.mark.parametrize("strategy", ["range", "bounded"])
def test_extract_text(strategy):
    run_cli(["extract-text", TestFiles.text, "--strategy", strategy], TestExpectations.text_extract, normalize_lfs=True)


@pytest.mark.parametrize("resource", ["multipage", "attachments", "forms"])
def test_pdfinfo(resource):
    run_cli(["pdfinfo", getattr(TestFiles, resource)], getattr(TestExpectations, "pdfinfo_%s" % resource))


@pytest.mark.parametrize("resource", ["images"])
def test_pageobjects(resource):
    run_cli(["pageobjects", getattr(TestFiles, resource)], getattr(TestExpectations, "pageobjects_%s" % resource))


def test_arrange(tmp_path):
    
    out = tmp_path / "out.pdf"
    run_cli(["arrange", TestFiles.multipage, TestFiles.encrypted, TestFiles.empty, "--pages", "1,3", "--passwords", "_", "test_user", "-o", out])
    
    pdf = pdfium.PdfDocument(out)
    assert len(pdf) == 4
    
    exp_texts = ["Page\r\n1", "Page\r\n3", "Encrypted PDF", ""]
    assert [_get_text(pdf, i) for i in range(len(pdf))] == exp_texts


def test_tile(tmp_path):
    
    out = tmp_path / "out.pdf"
    run_cli(["tile", TestFiles.multipage, "-r", 2, "-c", 2, "--width", 21.0, "--height", 29.7, "-u", "cm", "-o", out])
    
    pdf = pdfium.PdfDocument(out)
    assert len(pdf) == 1
    page = pdf[0]
    pageobjs = list( page.get_objects(max_depth=1) )
    assert len(pageobjs) == 3
    assert all(o.type == pdfium_c.FPDF_PAGEOBJ_FORM for o in pageobjs)


def test_render_multipage(tmp_path):
    
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    
    run_cli(["render", TestFiles.multipage, "-o", out_dir, "--scale", 0.2, "-f", "jpg"])
    
    out_files = list(out_dir.iterdir())
    assert sorted([f.name for f in out_files]) == ["multipage_1.jpg", "multipage_2.jpg", "multipage_3.jpg"]
