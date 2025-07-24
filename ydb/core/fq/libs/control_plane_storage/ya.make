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
    ydb_control_plane_storage_compute_database.cpp
    ydb_control_plane_storage_connections.cpp
    ydb_control_plane_storage_queries.cpp
    ydb_control_plane_storage_quotas.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/protobuf/interop
    ydb/core/base
    ydb/core/external_sources
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/common
    ydb/core/fq/libs/config
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_storage/events
    ydb/core/fq/libs/control_plane_storage/internal
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/db_schema
    ydb/core/fq/libs/graph_params/proto
    ydb/core/fq/libs/quota_manager/events
    ydb/core/fq/libs/shared_resources
    ydb/core/fq/libs/ydb
    ydb/core/kqp/opt
    ydb/core/kqp/proxy_service
    ydb/core/mon
    ydb/library/db_pool
    ydb/library/security
    ydb/library/yql/providers/s3/path_generator
    ydb/public/api/protos
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
    internal
)
