LIBRARY()

SRCS(
    config.cpp
    control_plane_storage_counters.cpp
    in_memory_control_plane_storage.cpp
    probes.cpp
    request_validators.cpp
    util.cpp
    validators.cpp
    ydb_control_plane_storage.cpp
    ydb_control_plane_storage_bindings.cpp
    ydb_control_plane_storage_connections.cpp
    ydb_control_plane_storage_queries.cpp
    ydb_control_plane_storage_quotas.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/protobuf/interop
    ydb/core/base
    ydb/core/mon
    ydb/core/yq/libs/actors/logging
    ydb/core/yq/libs/common
    ydb/core/yq/libs/config
    ydb/core/yq/libs/config/protos
    ydb/core/yq/libs/control_plane_storage/events
    ydb/core/yq/libs/control_plane_storage/internal
    ydb/core/yq/libs/control_plane_storage/proto
    ydb/core/yq/libs/db_schema
    ydb/core/yq/libs/graph_params/proto
    ydb/core/yq/libs/quota_manager/events
    ydb/core/yq/libs/shared_resources
    ydb/core/yq/libs/ydb
    ydb/library/security
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/library/db_pool
    ydb/library/yql/providers/s3/path_generator
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
    internal
    proto
)
