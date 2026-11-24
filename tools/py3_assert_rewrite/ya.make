PY3_PROGRAM(py3_assert_rewrite)

PEERDIR(
    contrib/python/pytest
)

NO_CHECK_IMPORTS()

PY_SRCS(
    MAIN main.py
)

END()
