PY23_TEST()

SRCDIR(util/digest)

PY_SRCS(
    NAMESPACE util.digest
    multi_ut.pyx
)

TEST_SRCS(
    test_digest.py
)

END()
