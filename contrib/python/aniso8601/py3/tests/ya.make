PY3TEST()

PEERDIR(
    contrib/python/aniso8601
)

SRCDIR(
    contrib/python/aniso8601/py3
)

PY_SRCS(
    TOP_LEVEL
    aniso8601/tests/compat.py
)

TEST_SRCS(
    aniso8601/builders/tests/__init__.py
    aniso8601/builders/tests/test_init.py
    aniso8601/builders/tests/test_python.py
    aniso8601/tests/__init__.py
    aniso8601/tests/test_compat.py
    aniso8601/tests/test_date.py
    aniso8601/tests/test_decimalfraction.py
    aniso8601/tests/test_duration.py
    aniso8601/tests/test_init.py
    aniso8601/tests/test_interval.py
    aniso8601/tests/test_time.py
    aniso8601/tests/test_timezone.py
    aniso8601/tests/test_utcoffset.py
)

NO_LINT()

END()
