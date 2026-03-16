PY3TEST()

PEERDIR(
    contrib/python/Markdown
    contrib/python/PyYAML
)

DATA(
    arcadia/contrib/python/Markdown/py3/tests
)

TEST_SRCS(
    __init__.py
    test_apis.py
    test_extensions.py
    test_legacy.py
    test_meta.py
    test_syntax/__init__.py
    test_syntax/blocks/__init__.py
    test_syntax/blocks/test_blockquotes.py
    test_syntax/blocks/test_code_blocks.py
    test_syntax/blocks/test_headers.py
    test_syntax/blocks/test_hr.py
    test_syntax/blocks/test_html_blocks.py
    test_syntax/blocks/test_paragraphs.py
    test_syntax/extensions/__init__.py
    test_syntax/extensions/test_abbr.py
    test_syntax/extensions/test_admonition.py
    test_syntax/extensions/test_attr_list.py
    test_syntax/extensions/test_code_hilite.py
    test_syntax/extensions/test_def_list.py
    test_syntax/extensions/test_fenced_code.py
    test_syntax/extensions/test_footnotes.py
    test_syntax/extensions/test_legacy_attrs.py
    test_syntax/extensions/test_legacy_em.py
    test_syntax/extensions/test_md_in_html.py
    test_syntax/extensions/test_tables.py
    test_syntax/extensions/test_toc.py
    test_syntax/inline/__init__.py
    test_syntax/inline/test_autolinks.py
    test_syntax/inline/test_emphasis.py
    test_syntax/inline/test_entities.py
    test_syntax/inline/test_images.py
    test_syntax/inline/test_links.py
    test_syntax/inline/test_raw_html.py
)

NO_LINT()

END()
