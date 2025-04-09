LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/sdk/cpp/src/client/types/credentials
    ydb/public/sdk/cpp/src/client/types/credentials/oauth2_token_exchange
)

END()
