PY2TEST()

PEERDIR(
    contrib/python/dominate
)

TEST_SRCS(
    test_document.py
    test_dom1core.py
    test_dominate.py
    test_html.py
    test_svg.py
    test_utils.py
)

NO_LINT()

END()
