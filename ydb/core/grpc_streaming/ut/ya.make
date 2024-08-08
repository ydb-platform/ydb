UNITTEST_FOR(ydb/core/grpc_streaming)

FORK_SUBTESTS()

TIMEOUT(300)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    grpc_streaming_ut.cpp
)

PEERDIR(
    ydb/library/grpc/client
    ydb/core/grpc_streaming/ut/grpc
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
