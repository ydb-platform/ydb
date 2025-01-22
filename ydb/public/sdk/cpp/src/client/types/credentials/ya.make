LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    credentials.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/types/status
    ydb/public/sdk/cpp/src/library/issue
)

END()

RECURSE(
    oauth2_token_exchange
)
