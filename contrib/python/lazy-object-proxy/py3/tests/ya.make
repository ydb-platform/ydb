PY3TEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/python/lazy-object-proxy
)

TEST_SRCS(
    conftest.py
    test_async_py3.py
    test_lazy_object_proxy.py
)

NO_LINT()

END()
