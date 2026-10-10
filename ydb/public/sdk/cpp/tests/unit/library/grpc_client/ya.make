UNITTEST()

FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
SRCS(
    grpc_client_low_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
)

END()
