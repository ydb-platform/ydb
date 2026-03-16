import pytest

from wasabi.markdown import MarkdownRenderer


def test_markdown():
    md = MarkdownRenderer()
    md.add(md.title(1, "Title"))
    md.add("Paragraph with {}".format(md.bold("bold")))
    md.add(md.list(["foo", "bar"]))
    md.add(md.table([("a", "b"), ("c", "d")], ["foo", "bar"]))
    md.add(md.code_block('import spacy\n\nnlp = spacy.blank("en")', "python"))
    md.add(md.list(["first", "second"], numbered=True))
    expected = """# Title\n\nParagraph with **bold**\n\n- foo\n- bar\n\n| foo | bar |\n| --- | --- |\n| a | b |\n| c | d |\n\n```python\nimport spacy\n\nnlp = spacy.blank("en")\n```\n\n1. first\n2. second"""
    assert md.text == expected


def test_markdown_table_aligns():
    md = MarkdownRenderer()
    md.add(md.table([("a", "b", "c")], ["foo", "bar", "baz"], aligns=("c", "r", "l")))
    expected = """| foo | bar | baz |\n| :---: | ---: | --- |\n| a | b | c |"""
    assert md.text == expected
    with pytest.raises(ValueError):
        md.table([("a", "b", "c")], ["foo", "bar", "baz"], aligns=("c", "r"))
    with pytest.raises(ValueError):
        md.table([("a", "b", "c")], ["foo", "bar", "baz"], aligns=("c", "r", "l", "l"))
