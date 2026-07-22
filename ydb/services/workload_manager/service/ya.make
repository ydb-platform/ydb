LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/cms/console
    ydb/core/mind
    ydb/core/resource_pools
    ydb/library/aclib
    ydb/library/actors/interconnect
    ydb/services/workload_manager/actors
    ydb/services/workload_manager/common
    ydb/services/workload_manager/tables
)

YQL_LAST_ABI_VERSION()

END()

