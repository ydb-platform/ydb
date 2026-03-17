#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import contextlib
from io import BytesIO

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfdocument import PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser


def get_text_lines(location, max_pages=5):
    """
    Return a list of unicode text lines extracted from a pdf file at
    `location`. May raise exceptions. Extract up to `max_pages` pages.
    """
    extracted_text = BytesIO()
    laparams = LAParams()
    with open(location, 'rb') as pdf_file:
        with contextlib.closing(PDFParser(pdf_file)) as parser:
            document = PDFDocument(parser)
            if not document.is_extractable:
                raise PDFTextExtractionNotAllowed(
                    'Encrypted PDF document: text extraction is not allowed')

            manager = PDFResourceManager()
            with contextlib.closing(
                TextConverter(manager, extracted_text, laparams=laparams)) as extractor:
                interpreter = PDFPageInterpreter(manager, extractor)
                pages = PDFPage.create_pages(document)
                for page_num, page in enumerate(pages, 1):
                    interpreter.process_page(page)
                    if max_pages and page_num == max_pages:
                        break
                extracted_text.seek(0)
                return extracted_text.readlines()
