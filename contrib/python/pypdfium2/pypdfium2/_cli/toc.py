# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import pypdfium2.internal as pdfium_i
from pypdfium2._cli._parsers import (
    add_input,
    add_n_digits,
    get_input,
    round_list,
)


def attach(parser):
    add_input(parser, pages=False)
    add_n_digits(parser)
    parser.add_argument(
        "--max-depth",
        type = int,
        default = 15,
        help = "Maximum recursion depth to consider when parsing the table of contents",
    )


def main(args):
    
    pdf = get_input(args)
    toc = pdf.get_toc(max_depth=args.max_depth)
    
    for bm in toc:
        count, dest = bm.get_count(), bm.get_dest()
        out = "    " * bm.level
        out += "[%s] %s -> " % (
            f"{count:+}" if count != 0 else "*",
            bm.get_title(),
        )
        # distinguish between "dest == None" and "dest with unknown mode" while keeping the output machine readable
        if dest:
            index, (view_mode, view_pos) = dest.get_index(), dest.get_view()
            out += "%s  # %s %s" % (
                index+1 if index != None else "?",
                pdfium_i.ViewmodeToStr.get(view_mode),
                round_list(view_pos, args.n_digits),
            )
        else:
            out += "_"
        print(out)
