# Copyright 2010 Dirk Holtwick, holtwick.it
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import logging
from io import BytesIO

from pypdf import PdfReader, PdfWriter

from xhtml2pdf.files import getFile, pisaTempFile

log = logging.getLogger(__name__)


class pisaPDF:
    def __init__(self, capacity=-1) -> None:
        self.capacity = capacity
        self.files: list[BytesIO] = []

    def addFromURI(self, url, basepath=None):
        obj = getFile(url, basepath)
        data = obj.getFileContent()
        if data:
            self.files.append(BytesIO(data))

    addFromFileName = addFromURI

    def addFromFile(self, f):
        if hasattr(f, "read"):
            self.files.append(f)
        else:
            self.addFromURI(f)

    def addFromString(self, data):
        f = getFile(data.encode(), capacity=self.capacity).getFileContent()
        if f:
            self.files.append(f)

    def addDocument(self, doc):
        if hasattr(doc.dest, "read"):
            self.files.append(doc.dest)

    def join(self, file=None):
        output = PdfWriter()
        for pdffile in self.files:
            pdf = PdfReader(pdffile)
            for pageNumber in range(len(pdf.pages)):
                output.add_page(pdf.pages[pageNumber])

        if file is not None:
            output.write(file)
            return file
        out = pisaTempFile(capacity=self.capacity)
        output.write(out)
        return out.getvalue()

    getvalue = join
    __str__ = join
