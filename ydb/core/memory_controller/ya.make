LIBRARY()

SRCS(
    memory_controller.cpp
)

PEERDIR(
    ydb/core/base
    ydb/library/actors/core
    ydb/library/services
    ydb/core/mon_alloc
    ydb/core/tablet_flat
)

END()
