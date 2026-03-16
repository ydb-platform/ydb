# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

from pypdfium2._cli._parsers import add_input, get_input

EXTRACT_RANGE   = "range"
EXTRACT_BOUNDED = "bounded"

# __main__.py hook
PARSER_DESC = """\
Note that PDFium outputs CRLF (\\r\\n) style line breaks.
This may be undesirable or confusing in some situations, e.g. when processing the output with an (unaware) parser on the command line.
If this is an issue, run e.g. `dos2unix` on the output, or use the Python API.\
"""

def attach(parser):
    add_input(parser, pages=True)
    parser.add_argument(
        "--strategy",
        default = EXTRACT_RANGE,
        choices = (EXTRACT_RANGE, EXTRACT_BOUNDED),
        help = "PDFium text extraction strategy (range, bounded).",
    )


def main(args):
    
    pdf = get_input(args)
    
    sep = ""
    for i in args.pages:
        
        page = pdf[i]
        textpage = page.get_textpage()
        
        # TODO let caller pass in possible range/boundary parameters
        if args.strategy == EXTRACT_RANGE:
            text = textpage.get_text_range()
        elif args.strategy == EXTRACT_BOUNDED:
            text = textpage.get_text_bounded()
        else:
            assert False
        
        print(sep + f"# Page {i+1}\n" + text)
        sep = "\n"
