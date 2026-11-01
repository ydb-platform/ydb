UNITTEST_FOR(ydb/core/grpc_services/base)

SRCS(
    path_aliasing_ut.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/testlib
    yql/essentials/sql/pg_dummy
)

END()
