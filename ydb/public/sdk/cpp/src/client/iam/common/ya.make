LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/http/simple
    library/cpp/json
    ydb/public/api/client/yc_public/iam
    ydb/public/sdk/cpp/src/library/jwt
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
