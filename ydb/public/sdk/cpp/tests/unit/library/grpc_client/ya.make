UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

FORK_SUBTESTS()

SRCS(
    grpc_client_low_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
)

END()
