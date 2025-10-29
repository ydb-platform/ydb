LIBRARY()

SRCS(
    events.cpp
    internal.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/keyvalue
    ydb/core/protos
    ydb/core/persqueue/public/counters
    ydb/core/tablet
    ydb/public/api/protos
    ydb/library/persqueue/topic_parser
)

END()
