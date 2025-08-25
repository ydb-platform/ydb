LIBRARY(client-iam-common-include)

SRCS(
    types.h
)

PEERDIR(
    contrib/libs/grpc
    ydb/public/sdk/cpp/src/library/jwt
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
