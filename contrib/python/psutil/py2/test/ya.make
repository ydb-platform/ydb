PY2TEST()
PEERDIR(
    contrib/python/psutil
    library/python/import_test
)
TEST_SRCS(test.py)
NO_LINT()
END()
