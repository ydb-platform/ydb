# coding=utf-8

from __future__ import unicode_literals

import io

from mammoth import documents, results, html
from mammoth.conversion import convert_document_element_to_html, _comment_author_label
from mammoth.docx.xmlparser import parse_xml
from mammoth.styles.parser import read_style_mapping
from .testing import assert_equal


def test_plain_paragraph_is_converted_to_plain_paragraph():
    result = convert_document_element_to_html(
        documents.paragraph(children=[_run_with_text("Hello")])
    )
    assert_equal('<p>Hello</p>', result.value)


def test_multiple_paragraphs_are_converted_to_multiple_paragraphs():
    result = convert_document_element_to_html(
        documents.document([
            documents.paragraph(children=[_run_with_text("Hello")]),
            documents.paragraph(children=[_run_with_text("there")]),
        ])
    )
    assert_equal('<p>Hello</p><p>there</p>', result.value)


def test_empty_paragraphs_are_ignored():
    result = convert_document_element_to_html(
        documents.paragraph(children=[_run_with_text("")])
    )
    assert_equal('', result.value)


def test_style_mappings_using_style_ids_can_be_used_to_map_paragraphs():
    result = convert_document_element_to_html(
        documents.paragraph(style_id="TipsParagraph", children=[
            _run_with_text("Tip")
        ]),
        style_map=[
            _style_mapping("p.TipsParagraph => p.tip")
        ]
    )
    assert_equal('<p class="tip">Tip</p>', result.value)


def test_style_mappings_using_style_names_can_be_used_to_map_paragraphs():
    result = convert_document_element_to_html(
        documents.paragraph(style_id="TipsParagraph", style_name="Tips Paragraph", children=[
            _run_with_text("Tip")
        ]),
        style_map=[
            _style_mapping("p[style-name='Tips Paragraph'] => p.tip")
        ]
    )
    assert_equal('<p class="tip">Tip</p>', result.value)


def test_style_names_in_style_mappings_are_case_insensitive():
    result = convert_document_element_to_html(
        documents.paragraph(style_id="TipsParagraph", style_name="Tips Paragraph", children=[
            _run_with_text("Tip")
        ]),
        style_map=[
            _style_mapping("p[style-name='tips paragraph'] => p.tip")
        ]
    )
    assert_equal('<p class="tip">Tip</p>', result.value)


def test_default_paragraph_style_is_used_if_no_matching_style_is_found():
    result = convert_document_element_to_html(
        documents.paragraph(style_id="TipsParagraph", children=[
            _run_with_text("Tip")
        ]),
    )
    assert_equal('<p>Tip</p>', result.value)


def test_default_paragraph_style_is_specified_by_mapping_plain_paragraphs():
    result = convert_document_element_to_html(
        documents.paragraph(style_id="TipsParagraph", children=[
            _run_with_text("Tip")
        ]),
        style_map=[
            _style_mapping("p => p.tip")
        ]
    )
    assert_equal('<p class="tip">Tip</p>', result.value)


def test_warning_is_emitted_if_paragraph_style_is_unrecognised():
    result = convert_document_element_to_html(
        documents.paragraph(
            style_id="Heading1",
            style_name="Heading 1",
            children=[_run_with_text("Tip")]
        ),
    )
    assert_equal([results.warning("Unrecognised paragraph style: Heading 1 (Style ID: Heading1)")], result.messages)


def test_no_warning_if_there_is_no_style_for_plain_paragraphs():
    result = convert_document_element_to_html(
        documents.paragraph(children=[_run_with_text("Tip")]),
    )
    assert_equal([], result.messages)


def test_bulleted_paragraphs_are_converted_using_matching_styles():
    result = convert_document_element_to_html(
        documents.paragraph(children=[
            _run_with_text("Hello")
        ], numbering=documents.numbering_level(level_index=0, is_ordered=False)),
        style_map=[
            _style_mapping("p:unordered-list(1) => ul > li:fresh")
        ]
    )
    assert_equal('<ul><li>Hello</li></ul>', result.value)


def test_bulleted_styles_dont_match_plain_paragraph():
    result = convert_document_element_to_html(
        documents.paragraph(children=[
            _run_with_text("Hello")
        ]),
        style_map=[
            _style_mapping("p:unordered-list(1) => ul > li:fresh")
        ]
    )
    assert_equal('<p>Hello</p>', result.value)


def test_bold_runs_are_wrapped_in_strong_tags_by_default():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_bold=True),
    )
    assert_equal("<strong>Hello</strong>", result.value)


def test_bold_runs_can_be_configured_with_style_mapping():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_bold=True),
        style_map=[_style_mapping("b => em")]
    )
    assert_equal("<em>Hello</em>", result.value)


def test_italic_runs_are_wrapped_in_emphasis_tags_by_default():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_italic=True),
    )
    assert_equal("<em>Hello</em>", result.value)


def test_italic_runs_can_be_configured_with_style_mapping():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_italic=True),
        style_map=[_style_mapping("i => strong")]
    )
    assert_equal("<strong>Hello</strong>", result.value)


def test_underline_runs_are_ignored_by_default():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_underline=True),
    )
    assert_equal("Hello", result.value)


def test_underline_runs_can_be_mapped_using_style_mapping():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_underline=True),
        style_map=[
            _style_mapping("u => em")
        ]
    )
    assert_equal("<em>Hello</em>", result.value)


def test_style_mapping_for_underline_runs_does_not_close_parent_elements():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_underline=True, is_bold=True),
        style_map=[
            _style_mapping("u => em")
        ]
    )
    assert_equal("<strong><em>Hello</em></strong>", result.value)


def test_strikethrough_runs_are_wrapped_in_s_elements_by_default():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_strikethrough=True),
    )
    assert_equal("<s>Hello</s>", result.value)


def test_strikethrough_runs_can_be_configured_with_style_mapping():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_strikethrough=True),
        style_map=[
            _style_mapping("strike => del")
        ]
    )
    assert_equal("<del>Hello</del>", result.value)


def test_all_caps_runs_are_ignored_by_default():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_all_caps=True),
    )
    assert_equal("Hello", result.value)


def test_all_caps_runs_can_be_mapped_using_style_mapping():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_all_caps=True),
        style_map=[
            _style_mapping("all-caps => span")
        ]
    )
    assert_equal("<span>Hello</span>", result.value)


def test_small_caps_runs_are_ignored_by_default():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_small_caps=True),
    )
    assert_equal("Hello", result.value)


def test_small_caps_runs_can_be_mapped_using_style_mapping():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], is_small_caps=True),
        style_map=[
            _style_mapping("small-caps => span")
        ]
    )
    assert_equal("<span>Hello</span>", result.value)


def test_highlighted_runs_are_ignored_by_default():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], highlight="yellow"),
    )
    assert_equal("Hello", result.value)


def test_highlighted_runs_can_be_configured_with_style_mapping_for_all_highlights():
    result = convert_document_element_to_html(
        documents.run(children=[documents.text("Hello")], highlight="yellow"),
        style_map=[
            _style_mapping("highlight => mark"),
        ],
    )
    assert_equal("<mark>Hello</mark>", result.value)


def test_highlighted_runs_can_be_configured_with_style_mapping_for_specific_highlight_color():
    result = convert_document_element_to_html(
        documents.paragraph(children=[
            documents.run(children=[documents.text("Yellow")], highlight="yellow"),
            documents.run(children=[documents.text("Red")], highlight="red"),
        ]),
        style_map=[
            _style_mapping("highlight[color='yellow'] => mark.yellow"),
            _style_mapping("highlight => mark"),
        ]
    )
    assert_equal('<p><mark class="yellow">Yellow</mark><mark>Red</mark></p>', result.value)


def test_superscript_runs_are_wrapped_in_sup_tags():
    result = convert_document_element_to_html(
        documents.run(
            children=[documents.text("Hello")],
            vertical_alignment=documents.VerticalAlignment.superscript,
        ),
    )
    assert_equal("<sup>Hello</sup>", result.value)


def test_subscript_runs_are_wrapped_in_sub_tags():
    result = convert_document_element_to_html(
        documents.run(
            children=[documents.text("Hello")],
            vertical_alignment=documents.VerticalAlignment.subscript,
        ),
    )
    assert_equal("<sub>Hello</sub>", result.value)


def test_runs_are_converted_by_satisfying_matching_paths():
    result = convert_document_element_to_html(
        documents.run(style_id="TipsRun", children=[documents.Text("Tip")]),
        style_map=[
            _style_mapping("r.TipsRun => span.tip")
        ]
    )
    assert_equal('<span class="tip">Tip</span>', result.value)


def test_docx_hyperlink_with_href_is_converted_to_anchor_tag():
    result = convert_document_element_to_html(
        documents.hyperlink(href="http://example.com", children=[documents.Text("Hello")]),
    )
    assert_equal('<a href="http://example.com">Hello</a>', result.value)


def test_docx_hyperlinks_can_be_collapsed():
    result = convert_document_element_to_html(
        documents.document(children=[
            documents.hyperlink(href="http://example.com", children=[documents.Text("Hello ")]),
            documents.hyperlink(href="http://example.com", children=[documents.Text("world")]),
        ])
    )
    assert_equal('<a href="http://example.com">Hello world</a>', result.value)


def test_docx_hyperlink_with_internal_anchor_reference_is_converted_to_anchor_tag():
    result = convert_document_element_to_html(
        documents.hyperlink(anchor="start", children=[documents.Text("Hello")]),
        id_prefix="doc-42-",
    )
    assert_equal('<a href="#doc-42-start">Hello</a>', result.value)


def test_hyperlink_target_frame_is_used_as_anchor_target():
    result = convert_document_element_to_html(
        documents.hyperlink(
            anchor="start",
            target_frame="_blank",
            children=[documents.Text("Hello")],
        ),
    )
    assert_equal('<a href="#start" target="_blank">Hello</a>', result.value)


def test_unchecked_checkbox_is_converted_to_unchecked_checkbox_input():
    result = convert_document_element_to_html(
        documents.checkbox(checked=False),
    )
    assert_equal('<input type="checkbox" />', result.value)


def test_checked_checkbox_is_converted_to_checked_checkbox_input():
    result = convert_document_element_to_html(
        documents.checkbox(checked=True),
    )
    assert_equal('<input checked="checked" type="checkbox" />', result.value)


def test_bookmarks_are_converted_to_anchors_with_ids():
    result = convert_document_element_to_html(
        documents.bookmark(name="start"),
        id_prefix="doc-42-",
    )
    assert_equal('<a id="doc-42-start"></a>', result.value)


def test_docx_tab_is_converted_to_tab_in_html():
    result = convert_document_element_to_html(documents.tab())
    assert_equal('\t', result.value)


def test_docx_table_is_converted_to_table_in_html():
    table = documents.table([
        documents.table_row([
            documents.table_cell([_paragraph_with_text("Top left")]),
            documents.table_cell([_paragraph_with_text("Top right")]),
        ]),
        documents.table_row([
            documents.table_cell([_paragraph_with_text("Bottom left")]),
            documents.table_cell([_paragraph_with_text("Bottom right")]),
        ]),
    ])
    result = convert_document_element_to_html(table)
    expected_html = (
        "<table>" +
        "<tr><td><p>Top left</p></td><td><p>Top right</p></td></tr>" +
        "<tr><td><p>Bottom left</p></td><td><p>Bottom right</p></td></tr>" +
        "</table>")
    assert_equal(expected_html, result.value)


def test_unmerged_table_cell_is_treated_as_table_cell():
    table = documents.table([
        documents.table_row([
            documents.table_cell_unmerged(
                [_paragraph_with_text("Cell")],
                colspan=1,
                rowspan=1,
                vmerge=False,
            ),
        ]),
    ])
    result = convert_document_element_to_html(table)
    expected_html = (
        "<table>" +
        "<tr><td><p>Cell</p></td></tr>" +
        "</table>")
    assert_equal(expected_html, result.value)


def test_table_style_mappings_can_be_used_to_map_tables():
    table = documents.table([], style_name="Normal Table")
    result = convert_document_element_to_html(
        table,
        style_map=[
            _style_mapping("table[style-name='Normal Table'] => table.normal-table:fresh"),
        ],
    )
    expected_html = '<table class="normal-table"></table>'
    assert_equal(expected_html, result.value)


def test_header_rows_are_wrapped_in_thead():
    table = documents.table([
        documents.table_row([documents.table_cell([])], is_header=True),
        documents.table_row([documents.table_cell([])], is_header=True),
        documents.table_row([documents.table_cell([])], is_header=False),
    ])
    result = convert_document_element_to_html(table)
    expected_html = (
        "<table>" +
        "<thead><tr><th></th></tr><tr><th></th></tr></thead>" +
        "<tbody><tr><td></td></tr></tbody>" +
        "</table>")
    assert_equal(expected_html, result.value)


def test_tbody_is_omitted_if_all_rows_are_headers():
    table = documents.table([
        documents.table_row([documents.table_cell([])], is_header=True),
    ])
    result = convert_document_element_to_html(table)
    expected_html = (
        "<table>" +
        "<thead><tr><th></th></tr></thead>" +
        "</table>")
    assert_equal(expected_html, result.value)


def test_unexpected_table_children_do_not_cause_error():
    table = documents.table([
        documents.tab(),
    ])
    result = convert_document_element_to_html(table)
    expected_html = "<table>\t</table>"
    assert_equal(expected_html, result.value)


def test_empty_cells_are_preserved_in_table():
    table = documents.table([
        documents.table_row([
            documents.table_cell([_paragraph_with_text("")]),
            documents.table_cell([_paragraph_with_text("Top right")]),
        ]),
    ])
    result = convert_document_element_to_html(table)
    expected_html = (
        "<table>" +
        "<tr><td></td><td><p>Top right</p></td></tr>" +
        "</table>")
    assert_equal(expected_html, result.value)


def test_empty_rows_are_preserved_in_table():
    table = documents.table([
        documents.table_row([
            documents.table_cell([_paragraph_with_text("Row 1")]),
        ]),
        documents.table_row([]),
    ])
    result = convert_document_element_to_html(table)
    expected_html = (
        "<table>" +
        "<tr><td><p>Row 1</p></td></tr><tr></tr>" +
        "</table>")
    assert_equal(expected_html, result.value)


def test_table_cells_are_written_with_colspan_if_not_equal_to_one():
    table = documents.table([
        documents.table_row([
            documents.table_cell([_paragraph_with_text("Top left")], colspan=2),
            documents.table_cell([_paragraph_with_text("Top right")]),
        ]),
    ])
    result = convert_document_element_to_html(table)
    expected_html = (
        "<table>" +
        "<tr><td colspan=\"2\"><p>Top left</p></td><td><p>Top right</p></td></tr>" +
        "</table>")
    assert_equal(expected_html, result.value)


def test_table_cells_are_written_with_rowspan_if_not_equal_to_one():
    table = documents.table([
        documents.table_row([
            documents.table_cell([], rowspan=2),
        ]),
    ])
    result = convert_document_element_to_html(table)
    expected_html = (
        "<table>" +
        "<tr><td rowspan=\"2\"></td></tr>" +
        "</table>")
    assert_equal(expected_html, result.value)


def test_line_break_is_converted_to_br():
    result = convert_document_element_to_html(documents.line_break)
    assert_equal("<br />", result.value)


def test_breaks_that_are_not_line_breaks_are_ignored():
    result = convert_document_element_to_html(documents.page_break)
    assert_equal("", result.value)


def test_breaks_can_be_mapped_using_style_mappings():
    result = convert_document_element_to_html(
        documents.run(children=[
            documents.page_break,
            documents.line_break,
        ]),
        style_map=[
            _style_mapping("br[type='page'] => hr"),
            _style_mapping("br[type='line'] => br.line-break"),
        ],
    )
    assert_equal('<hr /><br class="line-break" />', result.value)


def test_images_are_converted_to_img_tags_with_data_uri():
    image = documents.image(alt_text=None, content_type="image/png", open=lambda: io.BytesIO(b"abc"))
    result = convert_document_element_to_html(image)
    assert_equal('<img src="data:image/png;base64,YWJj" />', result.value)


def test_images_have_alt_tags_if_available():
    image = documents.image(alt_text="It's a hat", content_type="image/png", open=lambda: io.BytesIO(b"abc"))
    result = convert_document_element_to_html(image)
    image_html = parse_xml(io.StringIO(result.value))
    assert_equal('It\'s a hat', image_html.attributes["alt"])


def test_can_define_custom_conversion_for_images():
    def convert_image(image):
        with image.open() as image_file:
            return [html.element("img", {"alt": image_file.read().decode("ascii")})]

    image = documents.image(alt_text=None, content_type="image/png", open=lambda: io.BytesIO(b"abc"))
    result = convert_document_element_to_html(image, convert_image=convert_image)
    assert_equal('<img alt="abc" />', result.value)


def test_footnote_reference_is_converted_to_superscript_intra_page_link():
    footnote_reference = documents.note_reference("footnote", "4")
    result = convert_document_element_to_html(
        footnote_reference,
        id_prefix="doc-42-"
    )
    assert_equal('<sup><a href="#doc-42-footnote-4" id="doc-42-footnote-ref-4">[1]</a></sup>', result.value)


def test_footnotes_are_included_after_the_main_body():
    footnote_reference = documents.note_reference("footnote", "4")
    document = documents.document(
        [documents.paragraph([
            _run_with_text("Knock knock"),
            documents.run([footnote_reference])
        ])],
        notes=documents.notes([
            documents.note("footnote", "4", [_paragraph_with_text("Who's there?")])
        ])
    )
    result = convert_document_element_to_html(
        document,
        id_prefix="doc-42-"
    )
    expected_html = ('<p>Knock knock<sup><a href="#doc-42-footnote-4" id="doc-42-footnote-ref-4">[1]</a></sup></p>' +
                '<ol><li id="doc-42-footnote-4"><p>Who\'s there? <a href="#doc-42-footnote-ref-4">↑</a></p></li></ol>')
    assert_equal(expected_html, result.value)


def test_comments_are_ignored_by_default():
    reference = documents.comment_reference("4")
    comment = documents.comment(
        comment_id="4",
        body=[_paragraph_with_text("Who's there?")],
    )
    document = documents.document(
        [documents.paragraph([
            _run_with_text("Knock knock"),
            documents.run([reference])
        ])],
        comments=[comment],
    )
    result = convert_document_element_to_html(document, id_prefix="doc-42-")
    expected_html = '<p>Knock knock</p>'
    assert_equal(expected_html, result.value)


def test_comment_references_are_linked_to_comment_after_main_body():
    reference = documents.comment_reference("4")
    comment = documents.comment(
        comment_id="4",
        body=[_paragraph_with_text("Who's there?")],
        author_name="The Piemaker",
        author_initials="TP",
    )
    document = documents.document(
        [documents.paragraph([
            _run_with_text("Knock knock"),
            documents.run([reference])
        ])],
        comments=[comment],
    )
    result = convert_document_element_to_html(
        document,
        id_prefix="doc-42-",
        style_map=[
            _style_mapping("comment-reference => sup")
        ],
    )
    expected_html = (
        '<p>Knock knock<sup><a href="#doc-42-comment-4" id="doc-42-comment-ref-4">[TP1]</a></sup></p>' +
        '<dl><dt id="doc-42-comment-4">Comment [TP1]</dt><dd><p>Who\'s there? <a href="#doc-42-comment-ref-4">↑</a></p></dd></dl>'
    )
    assert_equal(expected_html, result.value)


def test_when_initials_are_not_blank_then_comment_author_label_is_initials():
    assert_equal("TP", _comment_author_label(documents.comment(
        comment_id="0",
        body=[],
        author_initials="TP",
    )))


def test_when_initials_are_blank_then_comment_author_label_is_blank():
    assert_equal("", _comment_author_label(documents.comment(
        comment_id="0",
        body=[],
        author_initials=None,
    )))


def _paragraph_with_text(text):
    return documents.paragraph(children=[_run_with_text(text)])


def _run_with_text(text):
    return documents.run(children=[documents.text(text)])


def _style_mapping(text):
    result = read_style_mapping(text)
    assert_equal([], result.messages)
    return result.value
