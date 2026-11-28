UNITTEST_FOR(ydb/library/grpc/server)

SIZE(MEDIUM)

PEERDIR(
    ydb/library/grpc/server
    ydb/public/api/grpc
)

SRCS(
    grpc_request_ut.cpp
    grpc_response_ut.cpp
    stream_adaptor_ut.cpp
)

END()

