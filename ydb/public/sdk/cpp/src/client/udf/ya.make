LIBRARY()

SRCS(
    udf.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/impl/internal/make_request
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/types/executor
    ydb/public/sdk/cpp/src/client/driver
)

END()
