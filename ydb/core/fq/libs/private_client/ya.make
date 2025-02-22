LIBRARY()

ADDINCL(
    ydb/public/sdk/cpp
)

SRCS(
    internal_service.cpp
    loopback_service.cpp
    private_client.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/json
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/grpc
    ydb/core/fq/libs/shared_resources
    ydb/core/protos
    ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()
