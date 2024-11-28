UNITTEST_FOR(ydb/library/yql/dq/actors/common)

SRCS(
    retry_events_queue_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/actors
    ydb/core/testlib
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
