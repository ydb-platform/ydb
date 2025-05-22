LIBRARY()

SRCS(
    accessor.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/value
)

END()
