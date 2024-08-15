LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/iam/common
    ydb/public/sdk/cpp/src/client/types/credentials
    ydb/public/sdk/cpp/src/client/types/credentials/oauth2_token_exchange
)

END()
