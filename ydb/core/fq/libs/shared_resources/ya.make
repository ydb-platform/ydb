LIBRARY()

SRCS(
    db_exec.cpp
    shared_resources.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
    ydb/core/fq/libs/common
    ydb/core/fq/libs/config
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/db_schema
    ydb/core/fq/libs/events
    ydb/core/fq/libs/exceptions
    ydb/core/fq/libs/quota_manager/events
    ydb/core/fq/libs/shared_resources/interface
    ydb/core/protos
    ydb/library/db_pool
    ydb/library/logger
    ydb/library/security
    ydb/public/sdk/cpp/client/extensions/solomon_stats
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_extension
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    interface
)
