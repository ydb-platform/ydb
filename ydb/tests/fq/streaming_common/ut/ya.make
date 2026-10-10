PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_message_acceptor.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/fq/streaming_common
    ydb/tests/tools/datastreams_helpers
)

END()
