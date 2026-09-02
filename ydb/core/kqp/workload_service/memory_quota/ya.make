LIBRARY()

SRCS(
    kqp_memory_quota.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/kqp/counters
    ydb/core/kqp/rm_service
    ydb/library/services
    ydb/library/yql/dq/actors/compute
)

YQL_LAST_ABI_VERSION()

END()
