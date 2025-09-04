PY3TEST()

SUBSCRIBER(g:python-contrib)

NO_LINT()

PEERDIR(
    contrib/python/portalocker
    contrib/python/redis
)

TEST_SRCS(
    __init__.py
    conftest.py
    temporary_file_lock.py
    # Tests intallation.
    # test_combined.py
    test_redis.py
    test_semaphore.py
    tests.py
)

END()
