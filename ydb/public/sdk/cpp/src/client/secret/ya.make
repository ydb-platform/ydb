LIBRARY()

SRCS(
    out.cpp
    secret.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/impl/internal/make_request
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/scheme
)

END()
