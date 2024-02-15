LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/library/grpc/client
    library/cpp/http/simple
    library/cpp/json
    library/cpp/threading/atomic
    ydb/public/api/client/yc_public/iam
    ydb/public/lib/jwt
    ydb/public/sdk/cpp/client/ydb_types/credentials
)

END()

