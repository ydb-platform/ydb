from __future__ import annotations

import os
from base64 import encodebytes

from nbformat.v2.nbbase import (
    new_author,
    new_code_cell,
    new_metadata,
    new_notebook,
    new_output,
    new_text_cell,
    new_worksheet,
)

# some random base64-encoded *bytes*
png = encodebytes(os.urandom(5))
jpeg = encodebytes(os.urandom(6))

ws = new_worksheet(name="worksheet1")

ws.cells.append(new_text_cell("html", source="Some NumPy Examples", rendered="Some NumPy Examples"))


ws.cells.append(new_code_cell(input="import numpy", prompt_number=1, collapsed=False))

ws.cells.append(new_text_cell("markdown", source="A random array", rendered="A random array"))

ws.cells.append(new_code_cell(input="a = numpy.random.rand(100)", prompt_number=2, collapsed=True))

ws.cells.append(
    new_code_cell(
        input="print a",
        prompt_number=3,
        collapsed=False,
        outputs=[
            new_output(
                output_type="pyout",
                output_text="<array a>",
                output_html="The HTML rep",
                output_latex="$a$",
                output_png=png,
                output_jpeg=jpeg,
                output_svg="<svg>",
                output_json="json data",
                output_javascript="var i=0;",
                prompt_number=3,
            ),
            new_output(
                output_type="display_data",
                output_text="<array a>",
                output_html="The HTML rep",
                output_latex="$a$",
                output_png=png,
                output_jpeg=jpeg,
                output_svg="<svg>",
                output_json="json data",
                output_javascript="var i=0;",
            ),
            new_output(
                output_type="pyerr",
                etype="NameError",
                evalue="NameError was here",
                traceback=["frame 0", "frame 1", "frame 2"],
            ),
        ],
    )
)

authors = [
    new_author(
        name="Bart Simpson",
        email="bsimpson@fox.com",
        affiliation="Fox",
        url="http://www.fox.com",
    )
]
md = new_metadata(
    name="My Notebook",
    license="BSD",
    created="8601_goes_here",
    modified="8601_goes_here",
    gistid="21341231",
    authors=authors,
)

nb0 = new_notebook(worksheets=[ws, new_worksheet(name="worksheet2")], metadata=md)

nb0_py = """# -*- coding: utf-8 -*-
# <nbformat>2</nbformat>

# <htmlcell>

# Some NumPy Examples

# <codecell>

import numpy

# <markdowncell>

# A random array

# <codecell>

a = numpy.random.rand(100)

# <codecell>

print a

"""
