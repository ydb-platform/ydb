LIBRARY(client-iam-common-include)

SRCS(
    generic_provider.h
    types.h
)

PEERDIR(
    contrib/libs/grpc
    ydb/public/sdk/cpp/src/library/issue
    ydb/public/sdk/cpp/src/library/jwt
    ydb/public/sdk/cpp/src/library/time
    ydb/public/sdk/cpp/src/client/types/credentials
    ydb/public/sdk/cpp/src/client/types/status
)

END()
