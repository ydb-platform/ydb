PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/smmap
)

SRCDIR(contrib/python/smmap/smmap/test)

TEST_SRCS(
    lib.py
    test_buf.py
    test_mman.py
    test_tutorial.py
    test_util.py
)

END()
