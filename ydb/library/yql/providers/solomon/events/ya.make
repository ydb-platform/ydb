LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/providers/solomon/proto
    ydb/library/yql/providers/solomon/solomon_accessor/client
)

END()
