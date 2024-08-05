LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/iam/common
    ydb/public/sdk/cpp_v2/src/client/types/credentials
    ydb/public/sdk/cpp_v2/src/client/types/credentials/oauth2_token_exchange
)

END()
