LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/base
    ydb/core/cms/console/events
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/mind
    ydb/core/protos
    ydb/core/resource_pools
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/services/workload_manager
    ydb/services/workload_manager/actors
    ydb/services/workload_manager/common
    ydb/services/workload_manager/metadata_subscription/resource_pool_classifier
    ydb/services/workload_manager/tables
)

YQL_LAST_ABI_VERSION()

END()

