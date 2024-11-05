LIBRARY()

SRCS(
    quota_manager.cpp
    quota_proxy.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/json
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/quota_manager/events
    ydb/core/fq/libs/shared_resources
    ydb/core/protos
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
    ut_helpers
)
