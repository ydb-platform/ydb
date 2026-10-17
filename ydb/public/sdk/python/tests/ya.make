PY3TEST()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/python
)

TEST_SRCS(
    test_connection_cleanup.py
)

END()
