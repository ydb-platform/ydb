LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/base
    ydb/library/yql/providers/solomon/proto
    ydb/library/yql/providers/solomon/solomon_accessor/client
)

END()
