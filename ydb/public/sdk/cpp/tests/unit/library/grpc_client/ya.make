UNITTEST()

FORK_SUBTESTS()

SRCS(
    grpc_client_low_ut.cpp
    stream_lifetime_ut.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/library/grpc/client
)

END()
