PY2TEST()

PEERDIR(
    contrib/deprecated/python/nose
    contrib/python/pyfst
)

NO_LINT()

SRCDIR(contrib/python/pyfst/py2/fst/tests)

TEST_SRCS(
    test_operations.py
    test_path.py
    test_simple.py
    test_syms.py
)

END()
