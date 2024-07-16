LIBRARY()

SRCS(
    memory_controller.cpp
    memtable_collection.cpp
)

PEERDIR(
    ydb/core/base
    ydb/library/actors/core
    ydb/library/services
    ydb/core/cms/console
    ydb/core/mon_alloc
)

END()
