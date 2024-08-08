UNITTEST_FOR(ydb/library/grpc/server)

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/library/grpc/server
)

SRCS(
    grpc_response_ut.cpp
    stream_adaptor_ut.cpp
)

END()

