# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import pypdfium2.raw as pdfium_c
import pypdfium2.internal as pdfium_i
from pypdfium2._cli._parsers import (
    add_input,
    add_n_digits,
    get_input,
    round_list,
)


def attach(parser):
    add_input(parser)
    add_n_digits(parser)


def main(args):
    
    pdf = get_input(args)
    print(f"Page Count: {len(pdf)}")
    print(f"PDF Version: {pdf.get_version() / 10}")
    
    id_permanent = pdf.get_identifier(pdfium_c.FILEIDTYPE_PERMANENT)
    id_changing  = pdf.get_identifier(pdfium_c.FILEIDTYPE_CHANGING)
    print(f"ID (permanent): {id_permanent}")
    print(f"ID (changing):  {id_changing}")
    print(f"ID match? - {id_permanent == id_changing}")
    print(f"Tagged? - {pdf.is_tagged()}")
    
    pagemode = pdf.get_pagemode()
    if pagemode != pdfium_c.PAGEMODE_USENONE:
        print(f"Page Mode: {pdfium_i.PageModeToStr.get(pagemode)}")
    
    formtype = pdf.get_formtype()
    if formtype != pdfium_c.FORMTYPE_NONE:
        print(f"Form Type: {pdfium_i.FormTypeToStr.get(formtype)}")
    
    metadata = pdf.get_metadata_dict(skip_empty=True)
    if len(metadata) > 0:
        print("Metadata:")
        for key, value in metadata.items():
            print(f"    {key}: {value}")
    
    for i in args.pages:
        
        print(f"\n# Page {i+1}")
        
        page = pdf[i]
        print(f"Size: {round_list(page.get_size(), args.n_digits)}")
        print(f"Rotation: {page.get_rotation()}")
        print(f"Bounding Box: {round_list(page.get_bbox(), args.n_digits)}")
        
        for box_name in ("media", "crop", "bleed", "trim", "art"):
            box = getattr(page, f"get_{box_name.lower()}box")(fallback_ok=False)
            if box:
                print(f"{box_name.capitalize()}Box: {round_list(box, args.n_digits)}")
