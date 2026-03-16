# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

# TODO test-cover use_bitmap, format and render

import logging
import traceback
from pathlib import Path
import pypdfium2.raw as pdfium_c
import pypdfium2._helpers as pdfium
from pypdfium2._cli._parsers import (
    add_input, get_input,
    BooleanOptionalAction,
)

logger = logging.getLogger(__name__)


def attach(parser):
    add_input(parser, pages=True)
    parser.add_argument(
        "--output-dir", "-o",
        required = True,
        type = Path,
        help = "Output directory to take the extracted images",
    )
    parser.add_argument(
        "--max-depth",
        type = int,
        default = 15,
        help = "Maximum recursion depth to consider when looking for pageobjects.",
    )
    parser.add_argument(
        "--use-bitmap",
        action = "store_true",
        help = "Enforce the use of bitmaps rather than attempting a smart extraction of the image.",
    )
    parser.add_argument(
        "--format",
        help = "Image format to use when saving bitmaps. (Fallback if doing smart extraction.)",
    )
    parser.add_argument(
        "--render",
        action = "store_true",
        help = "When --use-bitmap is given, whether to get rendered bitmaps, taking masks and transform matrices into account.",
    )
    parser.add_argument(
        "--scale-to-original",
        action = BooleanOptionalAction,
        default = True,
        help = "When --use-bitmap --render is given, whether to scale the image so it is rendered at its native resolution, or close to that. This should improve output quality. The default is True, but you may opt out.",
    )


def main(args):
    
    if not args.output_dir.is_dir():
        raise NotADirectoryError(args.output_dir)
    if args.use_bitmap and not args.format:
        args.format = "png"
    
    pdf = get_input(args)
    n_pdigits = len(str( max(args.pages)+1 ))
    
    for i in args.pages:
        
        page = pdf[i]
        images = page.get_objects(
            filter = (pdfium_c.FPDF_PAGEOBJ_IMAGE, ),
            max_depth = args.max_depth,
        )
        
        # not perfectly memory efficient, but we need image count for digit formatting
        images = list(images)
        n_idigits = len(str( len(images) ))
        
        for j, image in enumerate(images):
            tag = "%0*d_%0*d" % (n_pdigits, i+1, n_idigits, j+1)
            prefix = args.output_dir / f"{args.input.stem}_{tag}"
            # logger.debug("\n"+tag)
            try:
                if args.use_bitmap:
                    pil_image = image.get_bitmap(render=args.render, scale_to_original=args.scale_to_original).to_pil()
                    pil_image.save(f"{prefix}.{args.format}")
                else:
                    image.extract(prefix, fb_format=args.format)
            except pdfium.PdfiumError:
                traceback.print_exc()
            image.close()
