LIBRARY()

SRCS(
    service.cpp
)
# gn: ids headers service_id.h

PEERDIR(
    ydb/core/base
    ydb/core/cms/console
    ydb/core/mind  # gn: :tenant_node_enumeration
    ydb/core/resource_pools
    ydb/library/aclib
    ydb/library/actors/interconnect
    ydb/services/workload_manager/actors  # gn: :cpu_load_actors
    ydb/services/workload_manager/common
    ydb/services/workload_manager/tables
)

YQL_LAST_ABI_VERSION()

END()

