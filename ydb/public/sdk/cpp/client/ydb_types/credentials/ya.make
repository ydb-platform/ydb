LIBRARY()

SRCS(
    credentials.cpp
)

PEERDIR(
    ydb/library/login
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/ydb_types/status
    ydb/public/sdk/cpp/client/ydb_types/credentials/login
    yql/essentials/public/issue
)

END()

RECURSE(
    oauth2_token_exchange
)
