LIBRARY(client-iam-common-include)

SRCS(
    types.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/jwt
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
