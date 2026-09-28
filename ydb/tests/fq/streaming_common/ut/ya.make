PY3TEST()

TEST_SRCS(
    test_message_acceptor.py
)

PEERDIR(
    ydb/tests/fq/streaming_common
    ydb/tests/tools/datastreams_helpers
)

END()
