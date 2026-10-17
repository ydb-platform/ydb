LIBRARY()

SRCS(
    nodes_health_check.cpp  # gn: into ydb/core/fq/libs/control_plane_storage
    rate_limiter_resources.cpp  # gn: into ydb/core/fq/libs/control_plane_storage
    response_tasks.cpp  # gn: into ydb/core/fq/libs/control_plane_storage
    task_get.cpp  # gn: into ydb/core/fq/libs/control_plane_storage
    task_ping.cpp  # gn: into ydb/core/fq/libs/control_plane_storage
    task_result_write.cpp  # gn: into ydb/core/fq/libs/control_plane_storage
    utils.cpp  # gn: into ydb/core/fq/libs/control_plane_storage
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/lwtrace/mon
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/fq/libs/common
    ydb/core/fq/libs/compute/common
    ydb/core/fq/libs/config
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/metrics
    ydb/core/fq/libs/quota_manager
    ydb/core/fq/libs/quota_manager/events
    ydb/core/fq/libs/rate_limiter/events
    ydb/core/fq/libs/ydb
    ydb/core/kqp/opt
    ydb/core/kqp/script_executions/common
    ydb/core/metering
    ydb/core/mon
    ydb/library/actors/core
    ydb/library/protobuf_printer
    ydb/library/security
    ydb/public/lib/fq
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/value
    yql/essentials/public/issue
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
