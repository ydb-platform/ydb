PY2TEST()

PEERDIR(
    contrib/python/isodate
)

SRCDIR(contrib/python/isodate/py2/isodate/tests)

TEST_SRCS(
    test_date.py
    test_datetime.py
    test_duration.py
    test_pickle.py
    test_strf.py
    test_time.py
)

NO_LINT()

END()
