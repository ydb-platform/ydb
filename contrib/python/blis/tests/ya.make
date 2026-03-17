PY3TEST()

PEERDIR(
    contrib/python/blis
    contrib/python/hypothesis
)

SRCDIR(contrib/python/blis/blis/tests)

PY_SRCS(
    NAMESPACE blis.tests
    __init__.py
    common.py
)

TEST_SRCS(
    test_dotv.py
    test_gemm.py
)

NO_LINT()

END()
