UNITTEST()

FORK_SUBTESTS()

SRCS(
    bounded_response_ut.cpp
    request_control_ut.cpp
    grpc_client_low_ut.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/grpc/client
)

END()
