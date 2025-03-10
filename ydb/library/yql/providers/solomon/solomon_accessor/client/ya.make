LIBRARY()

SRCS(
    solomon_accessor_client.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/solomon/proto
    ydb/library/yql/providers/solomon/solomon_accessor/grpc
    ydb/public/sdk/cpp/src/client/types/credentials
    ydb/public/sdk/cpp/src/library/grpc/client
    yql/essentials/utils
)

END()
