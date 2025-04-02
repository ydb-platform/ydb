LIBRARY()

PEERDIR(
    ydb/core/kqp/common/simple
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
)

SRCS(
    kqp_compute_scheduler.cpp
)

YQL_LAST_ABI_VERSION()

END()
