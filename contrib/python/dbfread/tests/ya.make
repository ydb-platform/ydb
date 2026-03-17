PY3TEST()

PEERDIR(
    contrib/python/dbfread
)

DATA(
    arcadia/contrib/python/dbfread/testcases
)

SRCDIR(contrib/python/dbfread)

TEST_SRCS(
    dbfread/test_field_parser.py
    dbfread/test_ifiles.py
    dbfread/test_invalid_value.py
    dbfread/test_memo.py
    dbfread/test_read_and_length.py
)

NO_LINT()

END()
