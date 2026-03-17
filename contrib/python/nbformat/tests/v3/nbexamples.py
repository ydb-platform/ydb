from __future__ import annotations

import os
from base64 import encodebytes

from nbformat.v3.nbbase import (
    nbformat,
    nbformat_minor,
    new_author,
    new_code_cell,
    new_heading_cell,
    new_metadata,
    new_notebook,
    new_output,
    new_text_cell,
    new_worksheet,
)

# some random base64-encoded *text*
png = encodebytes(os.urandom(5)).decode("ascii")
jpeg = encodebytes(os.urandom(6)).decode("ascii")

ws = new_worksheet()

ws.cells.append(
    new_text_cell(
        "html",
        source="Some NumPy Examples",
    )
)


ws.cells.append(new_code_cell(input="import numpy", prompt_number=1, collapsed=False))

ws.cells.append(
    new_text_cell(
        "markdown",
        source="A random array",
    )
)

ws.cells.append(
    new_text_cell(
        "raw",
        source="A random array",
    )
)

ws.cells.append(new_heading_cell("My Heading", level=2))

ws.cells.append(new_code_cell(input="a = numpy.random.rand(100)", prompt_number=2, collapsed=True))
ws.cells.append(
    new_code_cell(
        input="a = 10\nb = 5\n",
        prompt_number=3,
    )
)
ws.cells.append(
    new_code_cell(
        input="a = 10\nb = 5",
        prompt_number=4,
    )
)

ws.cells.append(
    new_code_cell(
        input='print "ünîcødé"',
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
                output_json='{"json": "data"}',
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
                output_json='{"json": "data"}',
                output_javascript="var i=0;",
            ),
            new_output(
                output_type="pyerr",
                ename="NameError",
                evalue="NameError was here",
                traceback=["frame 0", "frame 1", "frame 2"],
            ),
            new_output(output_type="stream", output_text="foo\rbar\r\n"),
            new_output(output_type="stream", stream="stderr", output_text="\rfoo\rbar\n"),
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

nb0 = new_notebook(worksheets=[ws, new_worksheet()], metadata=md)

nb0_py = """# -*- coding: utf-8 -*-
# <nbformat>%i.%i</nbformat>

# <htmlcell>

# Some NumPy Examples

# <codecell>

import numpy

# <markdowncell>

# A random array

# <rawcell>

# A random array

# <headingcell level=2>

# My Heading

# <codecell>

a = numpy.random.rand(100)

# <codecell>

a = 10
b = 5

# <codecell>

a = 10
b = 5

# <codecell>

print "ünîcødé"

""" % (
    nbformat,
    nbformat_minor,
)
