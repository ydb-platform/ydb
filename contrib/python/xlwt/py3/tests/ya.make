PY3TEST()

PEERDIR (
    contrib/python/xlwt
)

DATA(
    arcadia/contrib/python/xlwt/py3/tests
)

TEST_SRCS(
    test_biff_records.py
    test_bitmaps.py
    test_by_name_functions.py
    test_compound_doc.py
    test_easyxf.py
    test_mini.py
    test_simple.py
    test_unicode1.py
    test_unicodeutils.py
    utils.py
)

NO_LINT()

END()
