LIBRARY()

SRCS(
    actor.cpp
    actor.h
)

PEERDIR(
    ydb/core/persqueue/public/cloud_events/proto
    ydb/core/protos
    ydb/core/protos/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
