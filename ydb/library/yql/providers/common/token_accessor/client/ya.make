LIBRARY()

SRCS(
    bearer_credentials_provider.cpp
    factory.cpp
    token_accessor_client.cpp
    token_accessor_client_factory.cpp
)

PEERDIR(
    ydb/library/grpc/client
    library/cpp/threading/atomic
    library/cpp/threading/future
    ydb/library/yql/providers/common/structured_token
    ydb/library/yql/providers/common/token_accessor/grpc
    ydb/public/sdk/cpp/client/ydb_types/credentials
)

END()
