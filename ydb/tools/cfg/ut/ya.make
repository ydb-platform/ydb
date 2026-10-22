PY3TEST()

TEST_SRCS(
    test_bootstrap.py
    test_state_storage.py
)

PEERDIR(
    contrib/python/pytest
    ydb/tools/cfg
)

END()
