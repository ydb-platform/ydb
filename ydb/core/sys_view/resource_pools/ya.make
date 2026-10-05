YQL_LIBRARY()

SRCS(
    resource_pools.h
    resource_pools.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
)

END()
