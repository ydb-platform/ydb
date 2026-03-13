LIBRARY()

SRCS(
    actor.cpp
    actor.h
)

PEERDIR(
    ydb/core/persqueue/public/cloud_events/proto
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/core/base
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)