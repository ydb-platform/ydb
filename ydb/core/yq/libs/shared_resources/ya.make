LIBRARY()

SRCS(
    db_exec.cpp
    shared_resources.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/monlib/dynamic_counters
    ydb/core/protos
    ydb/core/yq/libs/common
    ydb/core/yq/libs/config
    ydb/core/yq/libs/control_plane_storage/proto
    ydb/core/yq/libs/db_schema
    ydb/core/yq/libs/events
    ydb/core/yq/libs/exceptions
    ydb/core/yq/libs/quota_manager/events
    ydb/core/yq/libs/shared_resources/interface
    ydb/library/db_pool
    ydb/library/logger
    ydb/library/security
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    interface
)
