PY3TEST()

PEERDIR(
    contrib/python/panamap-proto
)

PY_SRCS(
    NAMESPACE tests
    messages.proto
)

TEST_SRCS(
    test_proto_mapping.py
)

NO_LINT()

END()
