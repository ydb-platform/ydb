LIBRARY(client-iam-common-include)

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    types.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/jwt
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
