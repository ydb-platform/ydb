UNITTEST_FOR(ydb/core/grpc_services/base)

SRCS(
    local_grpc_ut.cpp
    path_aliasing_ut.cpp
)

PEERDIR(
    ydb/core/grpc_services/local_rpc
    ydb/core/protos
    ydb/core/testlib
    yql/essentials/sql/pg_dummy
)

END()
