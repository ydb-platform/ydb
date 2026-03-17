PY3TEST()

PEERDIR(
    contrib/python/cymem
)

SRCDIR(contrib/python/cymem/cymem/tests)

TEST_SRCS(
    test_import.py
)

NO_LINT()

END()
