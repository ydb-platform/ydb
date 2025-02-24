LIBRARY()

SRCS(
    solomon_accessor_client.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/common/solomon_accessor/grpc
    ydb/library/yql/providers/solomon/proto
    ydb/public/sdk/cpp/src/library/grpc/client
    yql/essentials/utils
)

END()
