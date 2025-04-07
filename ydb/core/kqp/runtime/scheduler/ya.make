LIBRARY()

PEERDIR(
    ydb/core/kqp/common/simple
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
)

SRCS(
    kqp_compute_pool.cpp
    kqp_compute_scheduler.cpp
    kqp_schedulable_actor.cpp
)

YQL_LAST_ABI_VERSION()

END()
