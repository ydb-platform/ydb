YQL_LIBRARY()

SRCS(
    kqp_compute_scheduler_service.cpp
    service.h
)

PEERDIR(
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common/simple
    ydb/core/kqp/rm_service
    ydb/core/kqp/runtime/scheduler
    ydb/core/protos
    ydb/library/actors/core
    ydb/services/workload_manager
    ydb/services/workload_manager/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
