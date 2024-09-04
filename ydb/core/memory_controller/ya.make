LIBRARY()

SRCS(
    memory_controller.cpp
    memtable_collection.cpp
)

PEERDIR(
    ydb/core/base
    ydb/library/actors/core
    ydb/library/services
    ydb/library/yql/minikql
    ydb/core/cms/console
    ydb/core/mon_alloc
    ydb/core/node_whiteboard
    ydb/core/tablet
)

END()

RECURSE_FOR_TESTS(
    ut
)