LIBRARY()

SRCS(
    actor.cpp
    actor.h
    cloud_events.h
    events_writer.cpp
    events_writer.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/json
    library/cpp/yt/coding
    library/cpp/unified_agent_client
    library/cpp/monlib/dynamic_counters
    ydb/core/persqueue/events
    ydb/core/persqueue/public/cloud_events/proto
    ydb/core/protos/schemeshard
    ydb/core/base
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)