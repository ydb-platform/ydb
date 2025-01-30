PY3TEST()

PEERDIR(
    contrib/python/retry
)

IF (PYTHON2)
    PEERDIR(
        contrib/python/mock
    )
ENDIF()

SRCDIR(contrib/python/retry/py3/retry/tests)

TEST_SRCS(
    test_retry.py
)

NO_LINT()

END()
