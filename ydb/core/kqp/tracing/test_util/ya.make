LIBRARY()

SRCS(
    kqp_trace_test_helpers.h
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/kqp/tracing
    ydb/library/actors/wilson/test_util
)

END()
