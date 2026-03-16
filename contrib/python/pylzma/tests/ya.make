PY3TEST()

PEERDIR(
    contrib/python/pylzma
)

DATA(
    arcadia/contrib/python/pylzma/tests
)

TEST_SRCS(
    test_7zfiles.py
    test_compatibility.py
    test_pylzma.py
)

NO_LINT()

END()
