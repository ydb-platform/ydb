PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/jusText
    contrib/python/lxml-html-clean
)

TEST_SRCS(
    test_classify_paragraphs.py
    test_core.py
    test_dom_utils.py
    test_html_encoding.py
    test_paths.py
    test_sax.py
    test_utils.py
)

END()