PY3TEST()

PEERDIR(
    contrib/python/mammoth
    contrib/python/Funk
    contrib/python/precisely
    contrib/python/tempman
)

NO_LINT()

TEST_SRCS(
    testing.py
    conftest.py
    conversion_tests.py
    docx/__init__.py
    docx/body_xml_tests.py
    docx/comments_xml_tests.py
    docx/content_types_xml_tests.py
    docx/document_matchers.py
    docx/document_xml_tests.py
    docx/docx_tests.py
    docx/notes_xml_tests.py
    docx/numbering_xml_tests.py
    docx/office_xml_tests.py
    docx/relationships_xml_tests.py
    docx/style_map_tests.py
    docx/styles_xml_tests.py
    docx/uris_tests.py
    docx/xmlparser_tests.py
    html/__init__.py
    html/collapse_tests.py
    html/strip_empty_tests.py
    images_tests.py
    lists_tests.py
    options_tests.py
    raw_text_tests.py
    styles/__init__.py
    styles/document_matcher_tests.py
    styles/parser/__init__.py
    styles/parser/document_matcher_parser_tests.py
    styles/parser/html_path_parser_tests.py
    styles/parser/style_mapping_parser_tests.py
    styles/parser/token_parser_tests.py
    styles/parser/tokeniser_tests.py
    transforms_tests.py
    writers/__init__.py
    writers/markdown_tests.py
    zips_tests.py
)

RESOURCE_FILES(
    PREFIX contrib/python/mammoth/
    test-data/comments.docx
    test-data/embedded-style-map.docx
    test-data/empty.docx
    test-data/endnotes.docx
    test-data/external-picture.docx
    test-data/footnote-hyperlink.docx
    test-data/footnotes.docx
    test-data/simple-list.docx
    test-data/single-paragraph.docx
    test-data/strikethrough.docx
    test-data/tables.docx
    test-data/text-box.docx
    test-data/tiny-picture-target-base-relative.docx
    test-data/tiny-picture.docx
    test-data/tiny-picture.png
    test-data/underline.docx
    test-data/utf8-bom.docx
)


END()