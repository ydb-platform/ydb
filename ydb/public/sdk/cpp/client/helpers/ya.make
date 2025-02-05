LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/iam/common
    ydb/public/sdk/cpp/client/ydb_types/credentials
    ydb/public/sdk/cpp/client/ydb_types/credentials/oauth2_token_exchange
    yql/essentials/public/issue/protos
)

END()
