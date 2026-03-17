PY2TEST()

PEERDIR(
    contrib/python/bleach
)

DATA(
    arcadia/contrib/python/bleach/py2/tests/data
)

TEST_SRCS(
    test_callbacks.py
    test_clean.py
    test_css.py
    test_html5lib_shim.py
    test_linkify.py
    test_unicode.py
    test_utils.py
)

NO_LINT()

END()
