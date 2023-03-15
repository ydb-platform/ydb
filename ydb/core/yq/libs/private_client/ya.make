LIBRARY()

SRCS(
    internal_service.cpp
    loopback_service.cpp
    private_client.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/json
    ydb/public/sdk/cpp/client/ydb_table
    ydb/core/yq/libs/control_plane_storage/proto
    ydb/core/yq/libs/grpc
    ydb/core/yq/libs/shared_resources
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
