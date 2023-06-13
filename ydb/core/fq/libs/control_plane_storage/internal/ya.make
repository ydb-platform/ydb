LIBRARY()

SRCS(
    nodes_health_check.cpp
    rate_limiter_resources.cpp
    response_tasks.cpp
    task_get.cpp
    task_ping.cpp
    task_result_write.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/lwtrace/mon
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/metering
    ydb/core/fq/libs/common
    ydb/core/fq/libs/config
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/exceptions
    ydb/core/fq/libs/quota_manager
    ydb/core/fq/libs/quota_manager/events
    ydb/core/fq/libs/rate_limiter/events
    ydb/core/fq/libs/ydb
    ydb/core/mon
    ydb/library/protobuf_printer
    ydb/library/security
    ydb/library/yql/public/issue
    ydb/public/lib/fq
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_value
)

YQL_LAST_ABI_VERSION()

END()
