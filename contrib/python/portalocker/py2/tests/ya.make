PY2TEST()

SUBSCRIBER(g:python-contrib)

NO_LINT()

PEERDIR(
    contrib/python/portalocker
)

TEST_SRCS(
    __init__.py
    conftest.py
    temporary_file_lock.py
    # Tests intallation.
    # test_combined.py
    tests.py
)

END()
