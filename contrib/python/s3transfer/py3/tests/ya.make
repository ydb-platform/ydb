PY3TEST()

SIZE(MEDIUM)

FORK_TESTS()

PEERDIR(
    contrib/python/mock
    contrib/python/s3transfer
)

TEST_SRCS(
    functional/__init__.py
    functional/test_copy.py
    functional/test_crt.py
    functional/test_delete.py
    functional/test_download.py
    functional/test_manager.py
    functional/test_processpool.py
    functional/test_upload.py
    functional/test_utils.py
    __init__.py
    unit/__init__.py
    unit/test_bandwidth.py
    unit/test_compat.py
    unit/test_copies.py
    unit/test_crt.py
    unit/test_delete.py
    unit/test_download.py
    unit/test_futures.py
    unit/test_manager.py
    unit/test_processpool.py
    unit/test_s3transfer.py
    unit/test_subscribers.py
    unit/test_tasks.py
    unit/test_upload.py
    unit/test_utils.py
)

NO_LINT()

END()
