LIBRARY()

SRCS(
    actor.cpp
    actor.h
)

PEERDIR(
    ydb/core/persqueue/public/cloud_events/proto
)

YQL_LAST_ABI_VERSION()

END()
