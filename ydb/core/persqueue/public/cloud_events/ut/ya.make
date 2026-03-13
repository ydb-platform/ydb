UNITTEST_FOR(ydb/core/persqueue/public/cloud_events)

PEERDIR(
    ydb/core/audit
    ydb/core/persqueue/public/cloud_events
    ydb/core/persqueue/events
    ydb/core/protos
    ydb/library/actors/testlib
    library/cpp/json
    library/cpp/logger
    library/cpp/testing/unittest
    library/cpp/threading/blocking_queue
)

SRCS(
    cloud_events_ut.cpp
)

END()
