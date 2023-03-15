LIBRARY()

SRCS(
    quota_manager.cpp
    quota_proxy.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/json
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/client/ydb_table
    ydb/core/yq/libs/control_plane_storage/proto
    ydb/core/yq/libs/quota_manager/events
    ydb/core/yq/libs/shared_resources
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
    proto
    ut_helpers
)
