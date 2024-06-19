LIBRARY()

SRCS(
    kqp_workload_service_queues.cpp
    kqp_workload_service_tables.cpp
    kqp_workload_service.cpp
)

PEERDIR(
    ydb/core/cms/console
    ydb/core/kqp/common/events
    ydb/core/protos
    ydb/core/resource_pools

    ydb/library/actors/core
    ydb/library/query_actor
    ydb/library/services
    ydb/library/table_creator
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
