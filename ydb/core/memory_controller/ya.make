LIBRARY()

SRCS(
    memory_controller.cpp
    memtable_collection.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/cms/console
    ydb/core/mon_alloc
    ydb/core/node_whiteboard
    ydb/core/tablet
    ydb/library/actors/core
    ydb/library/services
    yql/essentials/minikql
    yql/essentials/utils/memory_profiling
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
