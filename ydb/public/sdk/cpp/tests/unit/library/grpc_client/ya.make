UNITTEST()

FORK_SUBTESTS()

SRCS(
    grpc_client_low_ut.cpp
    grpc_service_client_ut.cpp
)

PEERDIR(
    ydb/library/grpc/actor_client
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/library/grpc/client
)

END()
