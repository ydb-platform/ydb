LIBRARY()

SRCS(
    iam_impl.h
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/grpc/client
    library/cpp/http/simple
    library/cpp/json
    library/cpp/threading/atomic
    ydb/public/sdk/cpp_v2/src/library/jwt
    ydb/public/sdk/cpp/client/ydb_types/credentials
    ydb/public/sdk/cpp/client/iam/common
)

END()
