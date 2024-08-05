LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    credentials.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp_v2/src/client/types/status
    ydb/public/sdk/cpp_v2/src/library/yql/public/issue
)

END()

RECURSE(
    oauth2_token_exchange
)
