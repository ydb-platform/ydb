LIBRARY()

SRCS(
    collector.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/sys_view/common
    ydb/core/util
    ydb/library/actors/core
)

END()
