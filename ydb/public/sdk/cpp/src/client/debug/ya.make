LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/common_client/impl
)

END()
