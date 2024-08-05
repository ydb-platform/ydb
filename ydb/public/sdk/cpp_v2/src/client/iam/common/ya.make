LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/grpc/client
    library/cpp/http/simple
    library/cpp/json
    ydb/public/api/client/yc_public/iam
    ydb/public/sdk/cpp_v2/src/library/jwt
    ydb/public/sdk/cpp_v2/src/client/types/credentials
)

END()
