from __future__ import annotations

import os
from base64 import encodebytes

from nbformat.v4.nbbase import (
    new_code_cell,
    new_markdown_cell,
    new_notebook,
    new_output,
    new_raw_cell,
)

# some random base64-encoded *text*
png = encodebytes(os.urandom(5)).decode("ascii")
jpeg = encodebytes(os.urandom(6)).decode("ascii")

cells = []
cells.append(
    new_markdown_cell(
        source="Some NumPy Examples",
    )
)


cells.append(
    new_code_cell(
        source="import numpy",
        execution_count=1,
    )
)

cells.append(
    new_markdown_cell(
        source="Cell with attachments",
        attachments={
            "attachment1": {
                "text/plain": "\n".join(["a", "b", "c"]),
                "application/vnd.stuff+json": ["a", 1, "x"],
            }
        },
    )
)

cells.append(
    new_raw_cell(
        source="A random array",
    )
)

cells.append(
    new_markdown_cell(
        source="## My Heading",
    )
)

cells.append(
    new_code_cell(
        source="a = numpy.random.rand(100)",
        execution_count=2,
    )
)
cells.append(
    new_code_cell(
        source="a = 10\nb = 5\n",
        execution_count=3,
    )
)
cells.append(
    new_code_cell(
        source="a = 10\nb = 5",
        execution_count=4,
    )
)

cells.append(
    new_code_cell(
        source="json_outputs()",
        execution_count=12,
        outputs=[
            new_output(
                output_type="display_data",
                data={
                    "text/plain": "<json outputs>",
                    "application/json": {"key": "value", "x": 5, "lis": [1, 2, "x"]},
                    "application/vnd.listofstr+json": ["a", "b", "c"],
                    "application/vnd.numbers+json": [1, 2, 3],
                    "application/vnd.number+json": 42,
                    "application/vnd.object+json": {
                        "number": 5,
                        "array": [1, 2],
                        "str": "x",
                    },
                    "application/vnd.string+json": "ok",
                },
            )
        ],
    )
)

cells.append(
    new_code_cell(
        source='print "ünîcødé"',
        execution_count=3,
        outputs=[
            new_output(
                output_type="execute_result",
                data={
                    "text/plain": "<array a>",
                    "text/html": "The HTML rep",
                    "text/latex": "$a$",
                    "image/png": png,
                    "image/jpeg": jpeg,
                    "image/svg+xml": "<svg>",
                    "application/json": {"key": "value"},
                    "application/javascript": "var i=0;",
                },
                execution_count=3,
            ),
            new_output(
                output_type="display_data",
                data={
                    "text/plain": "<array a>",
                    "text/html": "The HTML rep",
                    "text/latex": "$a$",
                    "image/png": png,
                    "image/jpeg": jpeg,
                    "image/svg+xml": "<svg>",
                    "application/json": {"key": "value"},
                    "application/javascript": "var i=0;",
                },
            ),
            new_output(
                output_type="error",
                ename="NameError",
                evalue="NameError was here",
                traceback=["frame 0", "frame 1", "frame 2"],
            ),
            new_output(output_type="stream", text="foo\rbar\r\n"),
            new_output(output_type="stream", name="stderr", text="\rfoo\rbar\n"),
        ],
    )
)

nb0 = new_notebook(
    cells=cells,
    metadata={
        "language": "python",
    },
)
