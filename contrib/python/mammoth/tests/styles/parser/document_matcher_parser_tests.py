from mammoth import documents, document_matchers
from mammoth.styles.parser.document_matcher_parser import parse_document_matcher
from mammoth.styles.parser.errors import LineParseError
from mammoth.styles.parser.tokeniser import tokenise
from mammoth.styles.parser.token_iterator import TokenIterator
from ...testing import assert_equal, assert_raises


def test_unrecognised_document_element_raises_error():
    error = assert_raises(LineParseError, lambda: read_document_matcher("x"))
    assert_equal("Unrecognised document element: x", str(error))


def test_reads_plain_paragraph():
    assert_equal(
        document_matchers.paragraph(),
        read_document_matcher("p")
    )


def test_reads_paragraph_with_style_id():
    assert_equal(
        document_matchers.paragraph(style_id="Heading1"),
        read_document_matcher("p.Heading1")
    )


def test_reads_paragraph_with_exact_style_name():
    assert_equal(
        document_matchers.paragraph(style_name=document_matchers.equal_to("Heading 1")),
        read_document_matcher("p[style-name='Heading 1']")
    )


def test_reads_paragraph_with_style_name_prefix():
    assert_equal(
        document_matchers.paragraph(style_name=document_matchers.starts_with("Heading")),
        read_document_matcher("p[style-name^='Heading']")
    )


def test_unrecognised_string_matcher_raises_error():
    error = assert_raises(LineParseError, lambda: read_document_matcher("p[style-name*='Heading']"))
    assert_equal("Unrecognised string matcher: *", str(error))


def test_reads_paragraph_ordered_list():
    assert_equal(
        document_matchers.paragraph(numbering=documents.numbering_level(1, is_ordered=True)),
        read_document_matcher("p:ordered-list(2)")
    )


def test_reads_paragraph_unordered_list():
    assert_equal(
        document_matchers.paragraph(numbering=documents.numbering_level(1, is_ordered=False)),
        read_document_matcher("p:unordered-list(2)")
    )


def test_unrecognised_list_type_raises_error():
    error = assert_raises(LineParseError, lambda: read_document_matcher("p:blah"))
    assert_equal("Unrecognised list type: blah", str(error))


def test_reads_plain_run():
    assert_equal(
        document_matchers.run(),
        read_document_matcher("r")
    )


def test_reads_run_with_style_id():
    assert_equal(
        document_matchers.run(style_id="Emphasis"),
        read_document_matcher("r.Emphasis")
    )


def test_reads_run_with_style_name():
    assert_equal(
        document_matchers.run(style_name=document_matchers.equal_to("Emphasis")),
        read_document_matcher("r[style-name='Emphasis']")
    )


def test_reads_plain_table():
    assert_equal(
        document_matchers.table(),
        read_document_matcher("table")
    )


def test_reads_table_with_style_id():
    assert_equal(
        document_matchers.table(style_id="TableNormal"),
        read_document_matcher("table.TableNormal")
    )


def test_reads_table_with_style_name():
    assert_equal(
        document_matchers.table(style_name=document_matchers.equal_to("Normal Table")),
        read_document_matcher("table[style-name='Normal Table']")
    )


def test_reads_bold():
    assert_equal(
        document_matchers.bold,
        read_document_matcher("b")
    )

def test_reads_italic():
    assert_equal(
        document_matchers.italic,
        read_document_matcher("i")
    )

def test_reads_underline():
    assert_equal(
        document_matchers.underline,
        read_document_matcher("u")
    )

def test_reads_strikethrough():
    assert_equal(
        document_matchers.strikethrough,
        read_document_matcher("strike")
    )

def test_reads_all_caps():
    assert_equal(
        document_matchers.all_caps,
        read_document_matcher("all-caps")
    )

def test_reads_small_caps():
    assert_equal(
        document_matchers.small_caps,
        read_document_matcher("small-caps")
    )

def test_reads_highlight_without_color():
    assert_equal(
        document_matchers.highlight(),
        read_document_matcher("highlight")
    )

def test_reads_highlight_with_color():
    assert_equal(
        document_matchers.highlight(color="yellow"),
        read_document_matcher("highlight[color='yellow']")
    )

def test_reads_comment_reference():
    assert_equal(
        document_matchers.comment_reference,
        read_document_matcher("comment-reference")
    )

def test_reads_line_breaks():
    assert_equal(
        document_matchers.line_break,
        read_document_matcher("br[type='line']"),
    )

def test_reads_page_breaks():
    assert_equal(
        document_matchers.page_break,
        read_document_matcher("br[type='page']"),
    )

def test_reads_column_breaks():
    assert_equal(
        document_matchers.column_break,
        read_document_matcher("br[type='column']"),
    )


def test_unrecognised_break_type_raises_error():
    error = assert_raises(LineParseError, lambda: read_document_matcher("br[type='unknownBreakType']"))
    assert_equal("Unrecognised break type: unknownBreakType", str(error))


def read_document_matcher(string):
    return parse_document_matcher(TokenIterator(tokenise(string)))
