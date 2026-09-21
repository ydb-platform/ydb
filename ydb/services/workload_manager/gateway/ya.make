LIBRARY()

SRCS(
    gateway.cpp
    resource_pools_cache_actor.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime
    ydb/core/protos
    ydb/core/resource_pools

    ydb/library/actors/core

    ydb/services/metadata
    ydb/services/workload_manager
    ydb/services/workload_manager/common
    ydb/services/workload_manager/metadata_subscription/resource_pool_classifier
)

YQL_LAST_ABI_VERSION()

END()
