PY3TEST()

PEERDIR(contrib/python/aiounittest)

TEST_SRCS(
    dummy_math.py
    test_async_test.py
    test_asyncmockiterator.py
    test_asynctestcase.py
    test_asynctestcase_get_event_loop.py
    test_futurized.py
    test_unitest_module_invocation.py
)

NO_LINT()

END()

