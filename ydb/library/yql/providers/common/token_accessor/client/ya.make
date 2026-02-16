LIBRARY()

SRCS(
    bearer_credentials_provider.cpp
    factory.cpp
    token_accessor_client.cpp
    token_accessor_client_factory.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/library/yql/providers/common/token_accessor/grpc
    ydb/public/sdk/cpp/src/client/types/credentials
    ydb/public/sdk/cpp/src/client/types/credentials/login
    ydb/public/sdk/cpp/src/library/grpc/client
    yql/essentials/providers/common/structured_token
)

END()
