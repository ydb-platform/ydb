YQL_LIBRARY()

SRCS(
    groups.h
    groups.cpp
    pdisks.h
    pdisks.cpp
    storage_pools.h
    storage_pools.cpp
    storage_stats.h
    storage_stats.cpp
    vslots.h
    vslots.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime/common
    ydb/core/sys_view/common
    ydb/library/actors/core
)

END()
