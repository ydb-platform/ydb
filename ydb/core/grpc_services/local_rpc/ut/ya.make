UNITTEST_FOR(ydb/core/grpc_services/local_rpc)

SRCS(
    path_context_ut.cpp
)

PEERDIR(
    ydb/core/path_aliasing/context
    ydb/core/protos
    ydb/core/util
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
