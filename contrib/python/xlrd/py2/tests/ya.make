PY2TEST()

PEERDIR (
    contrib/python/xlrd
)

DATA(
    arcadia/contrib/python/xlrd/py2/tests
)

TEST_SRCS(
    base.py
    test_biffh.py
    test_cell.py
    test_formats.py
    test_formulas.py
    test_missing_records.py
    test_open_workbook.py
    test_sheet.py
    test_workbook.py
    test_xldate.py
    test_xldate_to_datetime.py
    test_xlsx_comments.py
    test_xlsx_parse.py
)

NO_LINT()

END()
