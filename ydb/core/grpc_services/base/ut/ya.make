UNITTEST_FOR(ydb/core/grpc_services/base)

SRCS(
    path_aliasing_ut.cpp
)

PEERDIR(
    ydb/core/grpc_services/counters
    ydb/core/protos
)

END()
