YQL_LIBRARY()

SRCS(
    resource_pools.h
    resource_pools.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime/common
    ydb/core/sys_view/common
    ydb/library/actors/core
    ydb/library/table_creator
)

END()
