LIBRARY()

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
    ydb/library/actors/core
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
)

YQL_LAST_ABI_VERSION()

END()
