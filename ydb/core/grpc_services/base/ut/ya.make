UNITTEST_FOR(ydb/core/grpc_services/base)

REQUIREMENTS(cpu:1)
SRCS(
    path_aliasing_ut.cpp
)

PEERDIR(
    ydb/core/protos
)

END()
