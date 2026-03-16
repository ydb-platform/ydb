# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

# TODO test-cover converting non-jpeg format

from pathlib import Path
import pypdfium2._helpers as pdfium
from pypdfium2._lazy import Lazy

def attach(parser):
    parser.add_argument(
        "images",
        nargs = "+",
        help = "Input images",
        type = Path,
    )
    parser.add_argument(
        "--output", "-o",
        required = True,
        type = Path,
        help = "Target path for the new PDF"
    )
    parser.add_argument(
        "--inline",
        action = "store_true",
        help = "If JPEG, whether to use PDFium's inline loading function."
    )


def main(args):
    
    # Rudimentary image to PDF conversion (testing / proof of concept)
    # Due to limitations in PDFium's public API, this function may be inefficient/lossy for non-JPEG input.
    # The technically best available open-source tool for image to PDF conversion is probably img2pdf (although its code style can be regarded as displeasing).
    
    pdf = pdfium.PdfDocument.new()
    
    for fp in args.images:
        
        image_obj = pdfium.PdfImage.new(pdf)
        
        # Simple check whether the file is a JPEG image - a better implementation could use mimetypes, python-magic, or PIL
        if fp.suffix.lower() in (".jpg", ".jpeg"):
            image_obj.load_jpeg(fp, inline=args.inline)
        else:
            pil_image = Lazy.PIL_Image.open(fp)
            bitmap = pdfium.PdfBitmap.from_pil(pil_image)
            pil_image.close()
            image_obj.set_bitmap(bitmap)
            bitmap.close()
        
        w, h = image_obj.get_px_size()
        image_obj.set_matrix( pdfium.PdfMatrix().scale(w, h) )
        page = pdf.new_page(w, h)
        page.insert_obj(image_obj)
        page.gen_content()
        
        image_obj.close()  # no-op
        page.close()
    
    pdf.save(args.output)
    pdf.close()
