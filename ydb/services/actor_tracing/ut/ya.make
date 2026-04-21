UNITTEST_FOR(ydb/services/actor_tracing)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    grpc_service_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/actor_tracing
    ydb/library/actors/trace_data
    ydb/public/api/grpc
    ydb/public/api/protos
)

END()
