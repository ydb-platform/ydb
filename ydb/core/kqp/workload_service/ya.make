LIBRARY()

SRCS(
    kqp_query_classifier.cpp
    kqp_workload_service.cpp
)

PEERDIR(
    ydb/core/cms/console

    ydb/core/fq/libs/compute/common

    ydb/core/kqp/common
    ydb/core/kqp/gateway/behaviour/resource_pool_classifier
    ydb/core/kqp/query_data
    ydb/core/kqp/workload_service/actors

    ydb/core/mind

    ydb/core/resource_pools

    ydb/library/actors/interconnect
    ydb/library/aclib

    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
    common
    tables
)

RECURSE_FOR_TESTS(
    ut
)
