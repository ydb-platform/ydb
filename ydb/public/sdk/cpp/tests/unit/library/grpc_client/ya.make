UNITTEST()

FORK_SUBTESTS()

SRCS(
    grpc_client_low_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
)

END()
