PY3TEST()

PEERDIR(
    contrib/python/parsel
    contrib/python/psutil
)

DATA(
    arcadia/contrib/python/parsel/py3/tests/xml_attacks
)

TEST_SRCS(
    test_selector.py
    test_selector_csstranslator.py
    test_utils.py
    test_xml_attacks.py
    test_xpathfuncs.py
)

NO_LINT()

END()
