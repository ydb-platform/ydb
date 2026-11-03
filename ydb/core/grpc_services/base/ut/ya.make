UNITTEST_FOR(ydb/core/grpc_services/base)

SRCS(
    mvcc_version_ut.cpp
    path_aliasing_ut.cpp
)

PEERDIR(
    ydb/core/protos
)

END()
