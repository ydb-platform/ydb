LIBRARY()

SRCS(
    cloud_events.h
    cloud_events.cpp
)

PEERDIR(
    ydb/core/audit
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/core/util
    ydb/core/ymq/actor/cloud_events/proto
    ydb/core/ymq/base
    ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    cloud_events_ut
)
