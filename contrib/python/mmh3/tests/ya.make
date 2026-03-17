PY3TEST()

PEERDIR(
    contrib/python/mmh3
)

PY_SRCS(
    TOP_LEVEL
    helper.py
)

TEST_SRCS(
    test_doctrings.py
    test_invalid_inputs.py
    test_mmh3.py
    test_mmh3_hasher.py
)

NO_LINT()

END()
