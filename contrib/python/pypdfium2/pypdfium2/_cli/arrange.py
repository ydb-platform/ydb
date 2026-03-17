# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import pypdfium2._helpers as pdfium
from pypdfium2._cli._parsers import parse_numtext


def attach(parser):
    parser.add_argument(
        "inputs",
        nargs = "+",
        help = "Sequence of PDF files.",
    )
    parser.add_argument(
        "--pages",
        nargs = "+",
        default = [],
        help = "Sequence of page texts, definig the pages to include from each PDF. Use '_' as placeholder for all pages."
    )
    parser.add_argument(
        "--passwords",
        nargs = "+",
        default = [],
        help = "Passwords to unlock encrypted PDFs. Any placeholder may be used for non-encrypted documents.",
    )
    parser.add_argument(
        "--output", "-o",
        required = True,
        help = "Target path for the output document",
    )


def main(args):
    
    args.pages = [None if p == "_" else parse_numtext(p) for p in args.pages]
    
    for _ in range(len(args.inputs) - len(args.pages)):
        args.pages.append(None)
    for _ in range(len(args.inputs) - len(args.passwords)):
        args.passwords.append(None)
    
    dest_pdf = pdfium.PdfDocument.new()
    
    for in_path, pages, password in zip(args.inputs, args.pages, args.passwords):
        with pdfium.PdfDocument(in_path, password=password) as src_pdf:
            dest_pdf.import_pages(src_pdf, pages=pages)
    
    dest_pdf.save(args.output)
