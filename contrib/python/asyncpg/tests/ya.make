PY3TEST()

PEERDIR(contrib/python/asyncpg)

NO_LINT()

FORK_TESTS()

DATA(
    arcadia/contrib/python/asyncpg/tests/certs
)

TEST_SRCS(
    test_adversity.py
    test_cache_invalidation.py
    test_cancellation.py
    test_codecs.py
    test_connect.py
    test_copy.py
    test_cursor.py
    test_exceptions.py
    test_execute.py
    test_introspection.py
    test_listeners.py
    test_pool.py
    test_prepare.py
    test_record.py
    test_test.py
    test_timeout.py
    test_transaction.py
    test_types.py
    test_utils.py
)

END()
