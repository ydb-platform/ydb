from __future__ import annotations

from unittest import TestCase

from nbformat.v2.nbbase import (
    NotebookNode,
    new_author,
    new_code_cell,
    new_metadata,
    new_notebook,
    new_output,
    new_text_cell,
    new_worksheet,
)


class TestCell(TestCase):
    def test_empty_code_cell(self):
        cc = new_code_cell()
        self.assertEqual(cc.cell_type, "code")
        self.assertEqual("input" not in cc, True)
        self.assertEqual("prompt_number" not in cc, True)
        self.assertEqual(cc.outputs, [])
        self.assertEqual(cc.collapsed, False)

    def test_code_cell(self):
        cc = new_code_cell(input="a=10", prompt_number=0, collapsed=True)
        cc.outputs = [
            new_output(output_type="pyout", output_svg="foo", output_text="10", prompt_number=0)
        ]
        self.assertEqual(cc.input, "a=10")
        self.assertEqual(cc.prompt_number, 0)
        self.assertEqual(cc.language, "python")
        self.assertEqual(cc.outputs[0].svg, "foo")
        self.assertEqual(cc.outputs[0].text, "10")
        self.assertEqual(cc.outputs[0].prompt_number, 0)
        self.assertEqual(cc.collapsed, True)

    def test_pyerr(self):
        o = new_output(
            output_type="pyerr",
            etype="NameError",
            evalue="Name not found",
            traceback=["frame 0", "frame 1", "frame 2"],
        )
        self.assertEqual(o.output_type, "pyerr")
        self.assertEqual(o.etype, "NameError")
        self.assertEqual(o.evalue, "Name not found")
        self.assertEqual(o.traceback, ["frame 0", "frame 1", "frame 2"])

    def test_empty_html_cell(self):
        tc = new_text_cell("html")
        self.assertEqual(tc.cell_type, "html")
        self.assertEqual("source" not in tc, True)
        self.assertEqual("rendered" not in tc, True)

    def test_html_cell(self):
        tc = new_text_cell("html", "hi", "hi")
        self.assertEqual(tc.source, "hi")
        self.assertEqual(tc.rendered, "hi")

    def test_empty_markdown_cell(self):
        tc = new_text_cell("markdown")
        self.assertEqual(tc.cell_type, "markdown")
        self.assertEqual("source" not in tc, True)
        self.assertEqual("rendered" not in tc, True)

    def test_markdown_cell(self):
        tc = new_text_cell("markdown", "hi", "hi")
        self.assertEqual(tc.source, "hi")
        self.assertEqual(tc.rendered, "hi")


class TestWorksheet(TestCase):
    def test_empty_worksheet(self):
        ws = new_worksheet()
        self.assertEqual(ws.cells, [])
        self.assertEqual("name" not in ws, True)

    def test_worksheet(self):
        cells = [new_code_cell(), new_text_cell("html")]
        ws = new_worksheet(cells=cells, name="foo")
        self.assertEqual(ws.cells, cells)
        self.assertEqual(ws.name, "foo")


class TestNotebook(TestCase):
    def test_empty_notebook(self):
        nb = new_notebook()
        self.assertEqual(nb.worksheets, [])
        self.assertEqual(nb.metadata, NotebookNode())
        self.assertEqual(nb.nbformat, 2)

    def test_notebook(self):
        worksheets = [new_worksheet(), new_worksheet()]
        metadata = new_metadata(name="foo")
        nb = new_notebook(metadata=metadata, worksheets=worksheets)
        self.assertEqual(nb.metadata.name, "foo")
        self.assertEqual(nb.worksheets, worksheets)
        self.assertEqual(nb.nbformat, 2)


class TestMetadata(TestCase):
    def test_empty_metadata(self):
        md = new_metadata()
        self.assertEqual("name" not in md, True)
        self.assertEqual("authors" not in md, True)
        self.assertEqual("license" not in md, True)
        self.assertEqual("saved" not in md, True)
        self.assertEqual("modified" not in md, True)
        self.assertEqual("gistid" not in md, True)

    def test_metadata(self):
        authors = [new_author(name="Bart Simpson", email="bsimpson@fox.com")]
        md = new_metadata(
            name="foo",
            license="BSD",
            created="today",
            modified="now",
            gistid="21341231",
            authors=authors,
        )
        self.assertEqual(md.name, "foo")
        self.assertEqual(md.license, "BSD")
        self.assertEqual(md.created, "today")
        self.assertEqual(md.modified, "now")
        self.assertEqual(md.gistid, "21341231")
        self.assertEqual(md.authors, authors)
