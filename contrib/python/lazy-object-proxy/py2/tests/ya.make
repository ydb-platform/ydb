PY2TEST()

PEERDIR(
    contrib/python/lazy-object-proxy
)

PY_SRCS(
    TOP_LEVEL
    compat.py
)

TEST_SRCS(
    conftest.py
    test_lazy_object_proxy.py
)

NO_LINT()

END()
