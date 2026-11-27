UNITTEST_FOR(ydb/library/grpc/server)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/common
    ydb/core/grpc_streaming/ut/grpc
    ydb/library/grpc/server
)

SRCS(
    disconnect_clients_ut.cpp
    grpc_response_ut.cpp
    stream_adaptor_ut.cpp
)

END()

