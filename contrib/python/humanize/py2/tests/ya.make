PY2TEST()

PEERDIR(
    contrib/python/humanize
    contrib/python/mock
    contrib/python/freezegun
)

TEST_SRCS(
    __init__.py
    base.py
    test_filesize.py
    test_i18n.py
    test_number.py
    test_time.py
)

NO_LINT()

END()
