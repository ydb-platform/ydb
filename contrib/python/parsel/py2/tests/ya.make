PY2TEST()

PEERDIR(
    contrib/python/parsel
)

TEST_SRCS(
    test_selector.py
    test_selector_csstranslator.py
    test_utils.py
    test_xpathfuncs.py
)

NO_LINT()

END()
